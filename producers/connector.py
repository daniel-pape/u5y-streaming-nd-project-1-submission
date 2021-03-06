"""Configures a Kafka Connector for Postgres Station data"""
import json
import logging

import requests

logger=logging.getLogger(__name__)

import configparser

from pathlib import Path

config=configparser.ConfigParser()
config.read(f"{Path().resolve().parent}/topics.ini")

POSTGRES_STATIONS_EXPORT_TOPIC_PREFIX=config['topics']['POSTGRES_STATIONS_EXPORT_TOPIC_PREFIX']
POSTGRES_STATIONS_EXPORT_TOPIC_TABLE=config['topics']['POSTGRES_STATIONS_EXPORT_TOPIC_TABLE']
KAFKA_CONNECT_URL="http://localhost:8083/connectors"
CONNECTOR_NAME=f"{POSTGRES_STATIONS_EXPORT_TOPIC_TABLE}"


def configure_connector():
    """Starts and configures the Kafka Connect connector"""
    logging.debug("creating or updating kafka connect connector...")

    resp=requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        logger.info("connector already created skipping recreation")
        return

    # TODO: Complete the Kafka Connect Config below.
    # Directions: Use the JDBC Source Connector to connect to Postgres. Load the `stations` table
    # using incrementing mode, with `stop_id` as the incrementing column name.

    # Make sure to think about what an appropriate topic prefix would be, and how frequently Kafka
    # Connect should run this connector (hint: not very often!)

    resp=requests.post(
        KAFKA_CONNECT_URL,
        headers={"Content-Type": "application/json"},
        data=json.dumps({
            "name": CONNECTOR_NAME,
            "config": {
                "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
                "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                "key.converter.schemas.enable": "false",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter.schemas.enable": "false",
                "batch.max.rows": "500",
                "connection.url": "jdbc:postgresql://postgres:5432/cta",
                # TODO: Understand why I need to use the Docker URL instead of the Host URL here!
                "connection.user": "cta_admin",
                "connection.password": "chicago",
                "table.whitelist": f"{POSTGRES_STATIONS_EXPORT_TOPIC_TABLE}",
                "mode": "incrementing",
                "incrementing.column.name": "stop_id",
                "topic.prefix": f"{POSTGRES_STATIONS_EXPORT_TOPIC_PREFIX}",
                "poll.interval.ms": "300000",
            }
        }),
    )

    ## Ensure a healthy response was given
    resp.raise_for_status()
    print("connector created successfully")
