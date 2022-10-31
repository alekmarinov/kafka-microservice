import os
import uwsgi
import time
import random
import logging
from kafka_microservice import ServiceWorker

# Log verbosity level
LOGGING_LEVEL = logging.INFO

logging.basicConfig(
    level=LOGGING_LEVEL,
    format='%(asctime)s.%(msecs)03d %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)

class DemoWorker(ServiceWorker):
    topics = [os.environ['DEMO_SERVICE_TOPIC_IN']]
    def __init__(self):
        # Initialize service

        # Init any server connections here
        # self.dbconn = mysqldb.connect(...)

        self.restarted_time = None

        # Call the super at last as it will fork the current process and the control will jump to the process method.
        super().__init__('demo', consumer_args=dict(topics=DemoWorker.topics, error_topic=os.environ['DEMO_SERVICE_TOPIC_ERR']))

    def process(self, in_topic, message):
        assert in_topic in DemoWorker.topics

        if self.restarted_time is None:
            self.restarted_time = time.time()

        if time.time() - self.restarted_time > 60:
            # Hang
            self.restarted_time = None
            if random.randrange(1, 3) == 1:
                logging.info(f'{self.me()}: burning cpu...')
                while True: pass
            else:
                logging.info(f'{self.me()}: cooling cpu...')
                time.sleep(9999999)

        if random.randrange(1, 10) == 5:
            # Crash
            logging.info(f'{self.me()}: crashing...')
            self.last_crash = time.time()
            a = 0 / 0
        else:
            logging.info(f'{self.me()}: working...')
            time.sleep(random.randrange(200, 1000) / 1000)

        # The returned tuple (topic, message) defines to which topic to produce the message
        out_topic = None
        return (out_topic, message)

if __name__ == '__main__':
    try:
        demo_worker = DemoWorker()
        logging.info(f'{demo_worker.me()}: Hello!')
        demo_worker.start()
    except:
        logging.exception(f"Mule {uwsgi.mule_id()}: I'm gonna die...")
    finally:
        logging.error(f"Mule {uwsgi.mule_id()}: I'm dead")
    # throttle restarting
    time.sleep(10)
