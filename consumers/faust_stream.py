"""Defines trends calculations for stations"""
import configparser
import logging
from pathlib import Path

import faust

logger=logging.getLogger(__name__)

config=configparser.ConfigParser()
config.read(f"{Path().resolve().parent}/topics.ini")

STATIONS_TOPIC_V1=config['topics']['STATIONS_TOPIC_V1']
TRANSFORMED_STATIONS_TOPIC=config['topics']['TRANSFORMED_STATIONS_TOPIC']
POSTGRES_STATIONS_EXPORT_TOPIC=config['topics']['POSTGRES_STATIONS_EXPORT_TOPIC']

# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


# TODO: Define a Faust Stream that ingests data from the Kafka Connect stations topic and
#   places it into a new topic with only the necessary information.
app=faust.App(
    id="faust.stations.stream",
    broker="kafka://localhost:9092",
    store="memory://"
)

# TODO: Define the input Kafka Topic. Hint: What topic did Kafka Connect output to?
topic=app.topic(POSTGRES_STATIONS_EXPORT_TOPIC, value_type=Station)
# TODO: Define the output Kafka Topic
out_topic=app.topic(STATIONS_TOPIC_V1, partitions=1, value_type=TransformedStation)
# TODO: Define a Faust Table
table=app.Table(
    TRANSFORMED_STATIONS_TOPIC,
    default=TransformedStation,
    partitions=1,
    changelog_topic=out_topic
)


#
#
# TODO: Using Faust, transform input `Station` records into `TransformedStation` records. Note that
# "line" is the color of the station. So if the `Station` record has the field `red` set to true,
# then you would set the `line` of the `TransformedStation` record to the string `"red"`
#
#
@app.agent(topic)
async def transformation(stations):
    # The topic POSTGRES_STATIONS_EXPORT_TOPIC and the PostGres SQL table it stems from
    # contains rows belonging not to the blue, red or green line. Therefore
    # we apply a filter on the stations first:
    async for s in stations.filter(lambda s: s.blue or s.red or s.green):
        transformed_station=TransformedStation(
            station_id=s.station_id,
            station_name=s.station_name,
            order=s.order,
            line="blue" if s.blue else ("red" if s.red else "green")
        )

        logger.info("faust streaming data...")

        # See Lesson 6, Section 16:
        table[transformed_station.station_id]=transformed_station


if __name__ == "__main__":
    app.main()
