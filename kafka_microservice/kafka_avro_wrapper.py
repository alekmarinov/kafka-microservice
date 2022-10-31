import time
import json
import logging
import avro.schema
from confluent_kafka import KafkaError, Producer
from confluent_kafka.avro import AvroConsumer, AvroProducer
from confluent_kafka.avro.serializer import SerializerError
from confluent_kafka.schema_registry import SchemaRegistryClient


CONSUMER_POLL_TIMEOUT = 10

class KafkaAvroWrapper:
    
    class Timeout(KafkaError):
        def __init__(self):
            super().__init__(KafkaError._TIMED_OUT,
                             f'No messages in the last {CONSUMER_POLL_TIMEOUT}s')

    class MissingArgument(Exception):
        def __init__(self, argument):
            super().__init__(f'Missing argument {argument}')

    def __init__(
        self,
        consumer_args,
        topics,
        poll_timeout=CONSUMER_POLL_TIMEOUT
    ):
        # Setup consumer
        if 'bootstrap.servers' not in consumer_args:
            raise KafkaAvroWrapper.MissingArgument("consumer_args['bootstrap.servers']")
        if 'schema.registry.url' not in consumer_args:
            raise KafkaAvroWrapper.MissingArgument("consumer_args['schema.registry.url']")
        self.consumer = AvroConsumer(consumer_args)
        self.consumer.subscribe(topics)
        self.bootstrap_servers = consumer_args['bootstrap.servers']
        schema_registry_url = consumer_args['schema.registry.url']
        self.schema_registry_client = SchemaRegistryClient({'url': schema_registry_url})
        self.producers = dict(
            schemaless=Producer({"bootstrap.servers": self.bootstrap_servers})
        )
        self.poll_timeout = poll_timeout

    def __iter__(self):
        while True:
            msg = self.consumer.poll(self.poll_timeout)
            if msg is None:
                # poll timeout, notify caller
                yield KafkaAvroWrapper.Timeout(), None, None, None
                continue
            yield msg.error(), msg.topic(), msg.value(), msg.offset()

    def get_producer_by_schema(self, schema_name):
        if schema_name in self.producers:
            return self.producers[schema_name]
        schema = self.schema_registry_client.get_latest_version(schema_name)
        schema = self.schema_registry_client.get_schema(schema.schema_id)
        schema = avro.schema.Parse(schema.schema_str)
        producer = AvroProducer(
            {
                'bootstrap.servers': self.bootstrap_servers,
                'schema.registry.url': self.schema_registry_url,
            },
            default_value_schema=schema,
        )
        self.producers[schema_name] = producer
        return producer

    def produce(self, topic, value, schema_name='schemaless'):
        producer = self.get_producer_by_schema(schema_name)
        if type(value) is dict:
            value = json.dumps(value)
        producer.produce(topic=topic, value=value)

    def flush(self):
        for producer in self.producers.values():
            producer.flush()
