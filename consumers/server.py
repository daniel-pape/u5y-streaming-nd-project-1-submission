"""Defines a Tornado Server that consumes Kafka Event data for display"""
import configparser
import logging
import logging.config
from pathlib import Path

import tornado.ioloop
import tornado.template
import tornado.web

import topic_check
from consumer import KafkaConsumer
from models import Lines, Weather

# Import logging before models to ensure configuration is picked up
logging.config.fileConfig(f"{Path(__file__).parents[0]}/logging.ini")
logger=logging.getLogger(__name__)

config=configparser.ConfigParser()
config.read(f"{Path().resolve().parent}/topics.ini")

WEATHER_TOPIC_V1=config['topics']['WEATHER_TOPIC_V1']
STATIONS_TOPIC_V1=config['topics']['STATIONS_TOPIC_V1']
TURNSTILE_SUMMARY_TOPIC=config['topics']['TURNSTILE_SUMMARY_TOPIC']
ARRIVALS_TOPIC_PREFIX=config['topics']['ARRIVALS_TOPIC_PREFIX']

class MainHandler(tornado.web.RequestHandler):
    """Defines a web request handler class"""

    template_dir=tornado.template.Loader(f"{Path(__file__).parents[0]}/templates")
    template=template_dir.load("status.html")

    def initialize(self, weather, lines):
        """Initializes the handler with required configuration"""
        self.weather=weather
        self.lines=lines

    def get(self):
        """Responds to get requests"""
        logging.debug("rendering and writing handler template")
        self.write(
            MainHandler.template.generate(weather=self.weather, lines=self.lines)
        )


def run_server():
    """Runs the Tornado Server and begins Kafka consumption"""
    if topic_check.topic_exists(TURNSTILE_SUMMARY_TOPIC) is False:
        logger.fatal(
            "ensure that the KSQL Command has run successfully before running the web server!"
        )
        exit(1)
    if topic_check.topic_exists(STATIONS_TOPIC_V1) is False:
        logger.fatal(
            "ensure that Faust Streaming is running successfully before running the web server!"
        )
        exit(1)

    weather_model=Weather()
    lines=Lines()

    application=tornado.web.Application(
        [(r"/", MainHandler, {"weather": weather_model, "lines": lines})]
    )
    application.listen(8888)

    # Build kafka consumers
    consumers=[
        KafkaConsumer(
            id="weather.consumer",
            topic_name_pattern=WEATHER_TOPIC_V1,
            message_handler=weather_model.process_message,
            offset_earliest=True,
        )
        ,
        KafkaConsumer(
            id="stations.table.consumer",
            topic_name_pattern=STATIONS_TOPIC_V1,
            message_handler=lines.process_message,
            offset_earliest=True,
            is_avro=False,
        )
        ,
        KafkaConsumer(
            id="arrival.consumer",
            topic_name_pattern=f"^{ARRIVALS_TOPIC_PREFIX}",
            message_handler=lines.process_message,
            offset_earliest=True
        )
        ,
        KafkaConsumer(
            id="turnstile.summary.consumer",
            topic_name_pattern=TURNSTILE_SUMMARY_TOPIC,
            message_handler=lines.process_message,
            offset_earliest=True,
            is_avro=False,
        )
    ]

    try:
        logger.info(
            "open a web browser to http://localhost:8888 to see the Transit Status Page"
        )
        for consumer in consumers:
            tornado.ioloop.IOLoop.current().spawn_callback(consumer.consume)

        tornado.ioloop.IOLoop.current().start()
    except KeyboardInterrupt as e:
        logger.info("shutting down server")
        tornado.ioloop.IOLoop.current().stop()
        for consumer in consumers:
            consumer.close()


if __name__ == "__main__":
    run_server()
