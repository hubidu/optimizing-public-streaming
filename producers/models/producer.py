"""Producer base-class providing common utilites and functionality"""
import logging
import time
import os

from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

from config import Config

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        self.broker_properties = {
            'bootstrap.servers': Config.KAFKA_BOOTSTRAP_SERVERS,
            'schema.registry.url': Config.KAFKA_SCHEMA_REGISTRY_URL
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic(self.topic_name)
            Producer.existing_topics.add(self.topic_name)

        self.producer = AvroProducer(
            self.broker_properties, default_key_schema=self.key_schema, default_value_schema=self.value_schema)

    def create_topic(self, topic_name):
        """Creates the producer topic if it does not already exist"""
        admin_client = AdminClient({
            "bootstrap.servers": os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        })
        admin_client.create_topics([NewTopic(
            topic_name, num_partitions=self.num_partitions, replication_factor=self.num_replicas)])
        logger.info(f"Created topic {topic_name}")

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        self.producer.flush()

        self.producer.close()
        logger.info("producer has been closed")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
