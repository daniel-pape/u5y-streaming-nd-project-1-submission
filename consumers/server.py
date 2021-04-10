"""Defines a Tornado Server that consumes Kafka Event data for display"""
import logging
import logging.config
from pathlib import Path

from config.config import WEATHER_TOPIC_V1
from config.config import STATIONS_TOPIC_V1
from config.config import TURNSTILE_SUMMARY_TOPIC
from config.config import ARRIVALS_TOPIC_PREFIX


import tornado.ioloop
import tornado.template
import tornado.web

# Import logging before models to ensure configuration is picked up
logging.config.fileConfig(f"{Path(__file__).parents[0]}/logging.ini")

from consumer import KafkaConsumer
from models import Lines, Weather
import topic_check

logger=logging.getLogger(__name__)


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
