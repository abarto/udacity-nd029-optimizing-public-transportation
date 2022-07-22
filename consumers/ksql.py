"""Configures KSQL to combine station and turnstile data"""
import json
import logging

from os import environ

import requests

import topic_check


logger = logging.getLogger(__name__)


KSQL_URL = broker=environ.get("KSQL_URL") or "http://localhost:8088"

# Shouldn't 'turnstile' be a stream?
KSQL_STATEMENT = """
CREATE STREAM turnstile (
    timestamp BIGINT,
    station_id BIGINT,
    station_name VARCHAR,
    line INT
) WITH (
    KAFKA_TOPIC='com.udacity.nd029.p1.v1.turnstile',
    VALUE_FORMAT='AVRO',
    KEY='timestamp'
);

CREATE TABLE turnstile_summary WITH (VALUE_FORMAT='JSON') AS
  SELECT station_id, COUNT() as count FROM turnstile GROUP BY station_id;
"""


def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    if topic_check.topic_exists("TURNSTILE_SUMMARY") is True:
        return

    logging.debug("executing ksql statement...")

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
