"""Producer base-class providing common utilites and functionality"""
import logging
import time

from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient

logger=logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics=set([])

    def __init__(
            self,
            topic_name,
            key_schema,
            value_schema=None,
            num_partitions=1,
            num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name=topic_name
        self.key_schema=key_schema
        self.value_schema=value_schema
        self.num_partitions=num_partitions
        self.num_replicas=num_replicas

        #
        #
        # TODO: Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!
        #
        #
        self.broker_properties={
            "bootstrap.servers": "PLAINTEXT://localhost:9092",  # TODO: Use one bootstrap server or more?
            "schema.registry.url": "http://localhost:8081"
        }

        # If the topic does not already exist, we try to create it
        if self.topic_name not in Producer.existing_topics:
            Producer.existing_topics.add(self.topic_name)

        schema_registry=CachedSchemaRegistryClient(
            {"url": self.broker_properties["schema.registry.url"]}
        )

        self.producer=AvroProducer(
            config={"bootstrap.servers": self.broker_properties["bootstrap.servers"]},
            default_key_schema=self.key_schema,
            default_value_schema=self.value_schema,
            schema_registry=schema_registry
        )

    def create_topic(self, topic_name):
        """Creates the producer topic if it does not already exist"""
        #
        #
        # TODO: Write code that creates the topic for this producer if it does not already exist on
        # the Kafka Broker.
        #
        #
        logger.info(f"create_topic(self, topic_name={topic_name})")

        client=AdminClient(
            {"bootstrap.servers": self.broker_properties["bootstrap.servers"]}
        )

        futures=client.create_topics(
            [
                NewTopic(
                    topic=topic_name,
                    num_partitions=5,
                    replication_factor=1,
                    config={
                        "cleanup.policy": "compact",
                        "compression.type": "lz4",
                        "delete.retention.ms": 100,
                        'file.delete.delay.ms': 100
                    }
                )
            ]
        )

        for topic, future in futures.items():
            try:
                future.result()
                logger.info(f"created topic {topic_name}")
            except Exception as e:
                logger.info(f"failed to create topic {topic_name}: {e}")
                raise

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        #
        #
        # TODO: Write cleanup code for the Producer here
        #
        #
        self.producer.flush()

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
