"""Defines core consumer functionality"""
import re
import logging

from os import environ

import confluent_kafka
from confluent_kafka import Consumer, OFFSET_BEGINNING
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from tornado import gen


logger = logging.getLogger(__name__)


class KafkaConsumer:
    """Defines the base kafka consumer class"""

    @staticmethod
    def error_cb(error):
        logger.error("error: %s", error)

    def __init__(
        self,
        topic_name_pattern,
        message_handler,
        is_avro=True,
        offset_earliest=False,
        sleep_secs=1.0,
        consume_timeout=0.1,
    ):
        """Creates a consumer object for asynchronous use"""
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest
        self.group_id = f'{topic_name_pattern}-group'

        self.broker_properties = {
            "bootstrap.servers": environ.get("BROKER_URL") or "plaintext://localhost:9092",
            "group.id": self.group_id,
            "allow.auto.create.topics": False,
            "auto.offset.reset": "earliest" if offset_earliest else "latest",
            "error_cb": KafkaConsumer.error_cb
        }

        if is_avro:
            self.broker_properties["schema.registry.url"] = environ.get("SCHEMA_REGISTRY_URL") or "http://localhost:8081"
            self.consumer = AvroConsumer(
                self.broker_properties
            )
        else:
            self.consumer = Consumer(
                self.broker_properties
            )

        self.consumer.subscribe([self.topic_name_pattern], on_assign=self.on_assign)

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""
        if self.offset_earliest:
            for partition in partitions:
                partition.offset = OFFSET_BEGINNING
        logger.info("partitions assigned for %s", self.topic_name_pattern)
        consumer.assign(partitions)

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(self.sleep_secs)

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""
        try:
            message = self.consumer.poll(self.consume_timeout)

            if message is None:
                logger.debug("%s: No mesage was received", self.group_id)
            elif message.error() is not None:
                logger.info("%s: Error recieved polling message: %s", self.group_id, message.error())
            else:
                logger.debug("%s: Consumed message. key: %s, message: %s",
                    self.group_id, message.key(), message.value())
                self.message_handler(message)
                return 1
        except KeyboardInterrupt:
            raise
        except:
            logger.exception("%s: Exception raised while consuming message", self.group_id) 

        return 0


    def close(self):
        """Cleans up any open kafka consumers"""
        self.consumer.close()
