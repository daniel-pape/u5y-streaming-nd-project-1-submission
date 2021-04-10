"""Configures KSQL to combine station and turnstile data"""
import json
import logging

import requests

import topic_check
from config.config import TURNSTILE_SUMMARY_TOPIC
from config.config import TURNSTILE_TOPIC_V1

logger = logging.getLogger(__name__)


KSQL_URL = "http://localhost:8088"

#
# TODO: Complete the following KSQL statements.
# TODO: For the first statement, create a `turnstile` table from your turnstile topic.
#       Make sure to use 'avro' datatype!


# TODO: For the second statment, create a `turnstile_summary` table by selecting from the
#       `turnstile` table and grouping on station_id.
#       Make sure to cast the COUNT of station id to `count`

#       TODO: Make sure to set the value format to JSON ... WHY?!?!?



KSQL_STATEMENT = f"""
    CREATE TABLE turnstile (
        station_id      INT,
        station_name    VARCHAR,
        line            VARCHAR
    ) WITH (
        KAFKA_TOPIC='{TURNSTILE_TOPIC_V1}',
        VALUE_FORMAT='AVRO',
        KEY='station_id'  
    );
    
    CREATE TABLE turnstile_summary
      WITH(VALUE_FORMAT='JSON') AS
        SELECT station_id, COUNT(station_id) AS count
        FROM turnstile
        GROUP BY station_id;
"""

def execute_statement():
    if topic_check.topic_exists(TURNSTILE_TOPIC_V1) is False:
        logger.fatal(
            f"ensure that the topic {TURNSTILE_TOPIC_V1} has successfully been created before running this application!"
        )
        exit(1)



    """Executes the KSQL statement against the KSQL API"""
    if topic_check.topic_exists(TURNSTILE_SUMMARY_TOPIC) is True:
        logger.info(f"topic {TURNSTILE_SUMMARY_TOPIC} already exists...")
        return

    logger.info("executing ksql statement...")

    resp = requests.post(
        f"{KSQL_URL}/ksql",
        headers={"Content-Type": "application/vnd.ksql.v1+json"},
        data=json.dumps(
            {
                "ksql": KSQL_STATEMENT,
                "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"},
            }
        ),
    )

    # Ensure that a 2XX status code was returned
    resp.raise_for_status()


if __name__ == "__main__":
    execute_statement()
