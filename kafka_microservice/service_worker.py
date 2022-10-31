import os
import json
import time
import uwsgi
import queue
import logging
import threading
import traceback
from codetiming import Timer
import multiprocessing as mp
from .kafka_avro_wrapper import KafkaAvroWrapper

# Infrastructure parameters
ENV_KAFKA_BOOTSTRAP_SERVERS = "KAFKA_BOOTSTRAP_SERVERS"
ENV_SCHEMA_REGISTRY_URL = "SCHEMA_REGISTRY_URL"

MAX_LOG_MESSAGE = 256

class ServiceWorker:
    """
    Manages a single worker to process messages received from kafka.
    The main function of this class is to protect the worker from hanging
    by starting it in separate process and killing it when result is not
    recieved in specified timeout MAX_MESSAGE_PROCESS_TIMEOUT.

    :param name (str): Name of the service
    :param consumer_args (dict):
            'topics'     : a list of topics to get messages from
            'error_topic': the name of the error topic to send error details
            **           : any other properties will be sent to AvroConsumer
    """

    # Maximum message processing time (10 mins)
    MAX_MESSAGE_PROCESS_TIMEOUT = 1 * 60

    # Maximum idle time before resetting counters (5 mins)
    MAX_MESSAGE_IDLE_TIMEOUT = 5 * 60 * 1000

    # Throttle consumer iteration loop on error (10 secs)
    THROTTLE_ON_ERROR = 10

    class Panic(Exception):
        pass

    def __init__(self, name, consumer_args=dict()):
        self.name = name
        topics = consumer_args.pop("topics") if "topics" in consumer_args else []
        self.error_topic = (
            consumer_args.pop("error_topic") if "error_topic" in consumer_args else None
        )
        self.message_queue = mp.Queue()
        self.result_queue = mp.Queue()

        # Collect message process timing statistics
        self.process_timer = Timer("process", logger=None)

        group_id = f'{self.name.replace("-", "_")}_group_id'
        init_consumer_args = {
            "group.id": group_id,
            "auto.offset.reset": "latest",
            # The maximum delay between invocations of poll() when
            # using consumer group management.
            # We give it 10% more time to prevent kafka consumer reaching it
            "max.poll.interval.ms": 1.1 * ServiceWorker.MAX_MESSAGE_PROCESS_TIMEOUT * 1000,
            # stats callback
            'stats_cb': self.update_kafka_metrics,
            # get stats every 10 seconds
            'statistics.interval.ms': 10 * 1000
        }
        if ENV_KAFKA_BOOTSTRAP_SERVERS in os.environ:
            init_consumer_args['bootstrap.servers'] = os.environ[ENV_KAFKA_BOOTSTRAP_SERVERS]
        if ENV_SCHEMA_REGISTRY_URL in os.environ:
            init_consumer_args['schema.registry.url'] = os.environ[ENV_SCHEMA_REGISTRY_URL]
        consumer_args = dict(init_consumer_args, **consumer_args)
        if 'bootstrap.servers' not in consumer_args:
            raise ServiceWorker.Panic(f"Missing consumer_args property 'bootstrap.servers' or environment var {ENV_KAFKA_BOOTSTRAP_SERVERS}")
        if 'schema.registry.url' not in consumer_args:
            raise ServiceWorker.Panic(f"Missing consumer_args property 'schema.registry.url' or environment var {ENV_SCHEMA_REGISTRY_URL}")

        # Setup kafka wrapper
        logging.info(
            f"{self.me()} initializing: consumer_args={str(consumer_args)}"
            f", topics={topics}, error_topic={self.error_topic}"
        )
        self.kafka = KafkaAvroWrapper(consumer_args, topics)

        # Timestamp of the first processed message
        self.first_msg_ts = 0

        # Timestamp of the last processed message
        self.last_msg_ts = 0

        # counts number of processed messages
        self.count_processed = 0

        # counts number of failed to process messages
        self.count_failed = 0

        # Get cache name from uwsgi opts where to write our stats
        opt_cache2 = dict(item.split('=') for item in uwsgi.opt['cache2'].decode('utf-8').split(','))
        self.cache = opt_cache2['name']

        # Compute max speed
        self.max_speed = 0

        # Starting time
        self.started_on = time.time()

        # Cache updating lock
        self.cache_lock = threading.Lock()

        # Creates a child worker.
        # This must be the last statement in the constructor as it will fork the current process
        # and jump into __call__ where all member fields must already be set.
        self.create_worker()

    def start(self):
        """
        Starts a looper reading from kafka and sending the messages
        to a child process. It guarantees the child process will not hang by
        killing it on a result timeout.
        """
        consumer_iterator = iter(self.kafka)
        while True:
            try:
                error, topic, message, offset = next(consumer_iterator)
                if error is not None:
                    if type(error) is KafkaAvroWrapper.Timeout:
                        logging.info(f'{self.me()}: waiting for messages')
                        continue
                    elif error.fatal():
                        self.terminate_worker()
                        raise ServiceWorker.Panic(error.reason)
                    else:
                        self.on_error(
                            topic=topic,
                            offset=offset,
                            message=str(message),
                            error=error.str(),
                            details=traceback.format_exc(),
                        )
                        continue
                assert self.message_queue.qsize() == 0
                self.message_queue.put((topic, message))
                try:
                    # Wait to receive the processed message with timeout
                    result_tuple = self.result_queue.get(
                        timeout=ServiceWorker.MAX_MESSAGE_PROCESS_TIMEOUT
                    )
                    success = result_tuple[0]
                    if success:
                        # On sucess send the result to kafka
                        out_topic = result_tuple[1]
                        if out_topic is not None:
                            self.kafka.produce(*result_tuple[1:])
                    else:
                        # handle error
                        self.on_error(
                            topic=topic,
                            offset=offset,
                            message=message,
                            error=result_tuple[1],
                            detals=result_tuple[2]
                        )
                except queue.Empty:
                    # worker is unable to process the message on time
                    self.on_error(
                        topic=topic,
                        offset=offset,
                        message=message,
                        error=f"{ServiceWorker.MAX_MESSAGE_PROCESS_TIMEOUT}s timeout",
                        details=traceback.format_exc(),
                    )
                    # terminate worker
                    self.terminate_worker()
                    # create another worker
                    self.create_worker()
            except:
                logging.exception(f'{self.me()}: Error getting message from consumer, retrying in {ServiceWorker.THROTTLE_ON_ERROR} seconds...')
                time.sleep(ServiceWorker.THROTTLE_ON_ERROR)
            finally:
                self.stat_set('started_on', self.started_on, 'Service start time')
                self.stat_set('up_time', time.time() - self.started_on, 'Service total operational time')

    def me(self):
        return f"{self.name}-{uwsgi.mule_id()}"

    def create_worker(self):
        logging.info(f'{self.me()}: create_worker')
        """
        Starts a child worker as process with target the service object itself.
        The worker's run method will have all initialized objects ready.
        """
        self.worker = mp.Process(target=self)
        self.worker.start()

    def terminate_worker(self):
        logging.info(f'{self.me()}: terminate_worker')
        if self.worker is not None:
            self.worker.terminate()
            self.worker = None

    def on_error(self, **error_props):
        """Handles message processing errors"""
        error_data = json.dumps(dict(service=self.name, worker=f'mule {uwsgi.mule_id()}', **error_props))
        if len(error_data) > MAX_LOG_MESSAGE:
            log_message = f'{error_data[:MAX_LOG_MESSAGE//2]}...{error_data[-MAX_LOG_MESSAGE//2:]}'
        else:
            log_message = error_data
        logging.error(f'{self.me()}: {log_message}')
        if self.error_topic is not None:
            self.kafka.produce(self.error_topic, error_data)

    def _stat_set(self, metric_type, key, value, help):
        assert metric_type in ['inc', 'set']
        # lock is needed as the function is called by
        # parent process and a child thread of consumer via stats_cb callback
        with self.cache_lock:
            ukey = f'mule.{uwsgi.mule_id()}.{key}'
            uwsgi.cache_update(f'{ukey}.type', metric_type, 0, self.cache)
            # Using only integer values
            uwsgi.cache_update(f'{ukey}.value', str(value), 0, self.cache)
            uwsgi.cache_update(f'{ukey}.help', help, 0, self.cache)

            # this function is called by the main mule process and child worker
            # so we need to merge the list of keys between them
            mule_keys = f'mule.{uwsgi.mule_id()}.keys'
            union_keys = set([key])
            if uwsgi.cache_exists(mule_keys):
                union_keys = union_keys.union(uwsgi.cache_get(mule_keys, self.cache).decode('utf-8').split(','))
            uwsgi.cache_update(mule_keys, ','.join(union_keys), 0, self.cache)

    def stat_inc(self, key, value=1, help=''):
        ukey = f'mule.{uwsgi.mule_id()}.{key}'
        current = int(uwsgi.cache_get(ukey, self.cache).decode('utf-8')) if uwsgi.cache_exists(ukey, self.cache) else 0
        self._stat_set('inc', key, current + value, help)

    def stat_set(self, key, value, help=''):
        self._stat_set('set', key, value, help)

    def __call__(self):
        """Worker function in separate process"""
        while True:
            self.update_process_metrics()
            try:
                topic, message = self.message_queue.get(ServiceWorker.MAX_MESSAGE_IDLE_TIMEOUT)
                with self.process_timer:
                    result_tuple = self.process(topic, message)
                self.result_queue.put((True, *result_tuple))

                # Increment processed messages
                self.count_processed += 1

                # Mark last processed message time
                self.last_msg_ts = time.time()

                # Mark first processed message once
                if self.first_msg_ts == 0:
                    self.first_msg_ts = time.time()

            except queue.Empty:
                # Clear timing stats
                Timer.timers.clear("process")
            except Exception as e:
                self.result_queue.put((False, str(e), traceback.format_exc()))
                # Increment failed to process messages
                self.count_failed += 1

    def update_process_metrics(self):
        """Update timing related process metrics"""

        self.stat_set('first_message_timestamp', self.first_msg_ts, 'Timestamp of the first processed message')
        self.stat_set('last_message_timestamp', self.last_msg_ts, 'Timestamp of the last processed message')
        self.stat_inc('count_processed', self.count_processed, 'Number of processed messages')
        self.stat_inc('count_failed', self.count_failed, 'Number of failed to process messages')

        # timing metrics will be present only after first processing
        if self.first_msg_ts > 0:
            # Compute messages per second
            # Timer.timers.mean will return total time in milliseconds (Timer.timers.total)
            # divided by the number of calls (Timer.timers.count)
            speed = 1000 * Timer.timers.mean("process")
            if speed > self.max_speed:
                self.max_speed = speed
            min_ts = Timer.timers.min("process")
            max_ts = Timer.timers.max("process")
            self.stat_set('messages_per_second', speed, 'Message processing speed (Hz)')
            self.stat_set('max_messages_per_second', self.max_speed, 'Maximum processing speed (Hz)')
            self.stat_set('min_processing_time_seconds', min_ts, 'Minimum processing time duration')
            self.stat_set('max_processing_time_seconds', max_ts, 'Maximum processing time duration')

    def update_kafka_metrics(self, stats_json_str):
        """Update kafka consumer metrics"""

        stats = json.loads(stats_json_str)
        for topic_name, topic_metrics in stats['topics'].items():
            for partition, partition_metrics in topic_metrics['partitions'].items():
                if int(partition) < 0:
                    continue
                lag = int(partition_metrics['consumer_lag'])
                self.stat_set(f'topic.{topic_name}.{partition}.lag', lag, 'The lag of topic partition')
