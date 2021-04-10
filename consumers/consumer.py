"""Defines core consumer functionality"""
import logging

from confluent_kafka import Consumer, OFFSET_BEGINNING
from confluent_kafka.avro import AvroConsumer, CachedSchemaRegistryClient
from tornado import gen

logger=logging.getLogger(__name__)


class KafkaConsumer:
    """Defines the base kafka consumer class"""

    def __init__(
            self,
            id,
            topic_name_pattern,
            message_handler,
            is_avro=True,
            offset_earliest=False,
            sleep_secs=1.0,
            consume_timeout=0.1,
    ):
        """Creates a consumer object for asynchronous use"""
        self.id=id
        self.topic_name_pattern=topic_name_pattern
        self.message_handler=message_handler
        self.sleep_secs=sleep_secs
        self.consume_timeout=consume_timeout
        self.offset_earliest=offset_earliest

        #
        #
        # TODO: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        #
        self.broker_properties={
            "bootstrap.servers": "PLAINTEXT://localhost:9092",
            "schema.registry.url": "http://localhost:8081"
        }

        # TODO: Create the Consumer, using the appropriate type.
        auto_offset_reset="earliest" if offset_earliest else "latest"
        config={
            "client.id": f"{self.id}",
            "bootstrap.servers": self.broker_properties["bootstrap.servers"],
            "group.id": f"{self.id}.group",
            "auto.offset.reset": auto_offset_reset
        }

        if is_avro is True:
            self.consumer=AvroConsumer(
                config=config,
                schema_registry=CachedSchemaRegistryClient(
                    {"url": self.broker_properties["schema.registry.url"]}
                )
            )
        else:
            self.consumer=Consumer(config)
        #
        #
        # TODO: Configure the AvroConsumer and subscribe to the topics. Make sure to think about
        # how the `on_assign` callback should be invoked.
        #
        #
        self.consumer.subscribe([topic_name_pattern], on_assign=self.on_assign)

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""
        # TODO: If the topic is configured to use `offset_earliest` set the partition offset to
        # the beginning or earliest

        if self.offset_earliest:
            logger.info(f"consumer {self.id} has offset_earliest={self.offset_earliest}...")
            for partition in partitions:
                partition.offset=OFFSET_BEGINNING

        logger.info("partitions assigned for %s", self.topic_name_pattern)
        consumer.assign(partitions)

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            num_results=1
            while num_results > 0:
                num_results=self._consume()
            await gen.sleep(self.sleep_secs)

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""
        #
        #
        # TODO: Poll Kafka for messages. Make sure to handle any errors or exceptions.
        # Additionally, make sure you return 1 when a message is processed, and 0 when no message
        # is retrieved.
        #
        #
        while True:
            msg=self.consumer.poll(timeout=self.consume_timeout)
            if msg is None:
                logger.debug(f"no message received by {self.id}")
                return 0
            elif msg.error() is not None:
                logger.error(f"error in {self.id}: {msg.error()}")
            else:
                self.message_handler(msg)
                logger.debug(f"consumer {self.id} consumed message {msg.key()}: {msg.value()}")
                return 1

    def close(self):
        """Cleans up any open kafka consumers"""
        #
        #
        # TODO: Cleanup the kafka consumer
        #
        #
        self.consumer.close()
