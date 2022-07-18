"""Producer base-class providing common utilites and functionality"""
from os import environ

import logging
import time

from avro.schema import RecordSchema
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient


logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Default value for broper properties. Specific producers can redefine them
    DEFAULT_BROKER_PROPERTIES = {
        "enable.idempotence": True,
        "queue.buffering.max.ms": 10
    }

    # Default value for topic configuration. Specific producers can redefine them
    DEFAULT_TOPIC_CONFIG = {
         "compression.type": "lz4",
         "log.cleanup.policy": "delete"
    }

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name: str,
        key_schema: RecordSchema,
        value_schema: RecordSchema = None,
        num_partitions: int = 1,
        num_replicas: int = 1,
        broker_url: str = None,
        schema_registry_url: str = None,
        broker_properties: dict = None,
        topic_config: dict = None
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas
        self.topic_config = topic_config or {}
        self._broker_url = broker_url or environ.get("BROKER_URL")
        self._schema_registry_url = schema_registry_url or environ.get("SCHEMA_REGISTRY_URL")

        if not self._broker_url:
            raise ValueError("Broker URL was not supplied as an argument nor a BROKER_URL environment variable")

        if not self._schema_registry_url:
            raise ValueError("Schema Registry URL was not supplied as an argument nor a SCHEMA_REGISTRY_URL environment variable")

        # Renamed the properties to match the most up-to-date documentation
        self.broker_properties = {
            "metadata.broker.list": self.broker_url,
            "client.id": self.__class__.__name__,
            **(broker_properties or Producer.DEFAULT_BROKER_PROPERTIES),
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        self.producer = AvroProducer(
            self._broker_properties,
            schema_registry=CachedSchemaRegistryClient(self.schema_registry_url)
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""

        logger.info("Creating topic '%s' if it does not exists", self.topic_name)

        try:
            admin_client = AdminClient(self.broker_properties)

            if self.topic_name in admin_client.list_topics().topics:
                logger.info("Topic '%s' already exists", self.topic_name)
                return

            logger.info("Attempting to create topic '%s'", self.topic_name)

            topic_futures = admin_client.create_topics([
                NewTopic(
                    self.topic_name,
                    num_partitions=self.num_partitions,
                    replication_factor=self.num_replicas,
                    config={
                        **Producer.DEFAULT_TOPIC_CONFIG,
                        **self.topic_config
                    }
                ),
            ], operation_timeout=5.0)

            # Since we're only creating one topic, we don't need to iterate over create_topics
            # futures dictionary.
            future_result = topic_futures[self.topic_name].result()

            logger.debug("topic_name: %s, future_result: %s", self.topic_name, future_result)
        except Exception as e:
            logger.exception("Exception raised while trying to create a topic. topic_name = %s", self.topic_name)
            raise e

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        
        try:
            self.producer.purge()
            self.producer.flush()
        except Exception as e:
            logger.exception("Exception raised while trying to close a produce. topic_name = %s", self.topic_name)
            raise e

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))