"""Configures KSQL to combine station and turnstile data"""
import json
import logging

import requests

import topic_check


logger = logging.getLogger(__name__)


KSQL_URL = "http://localhost:8088"

KSQL_STATEMENT = """
SET 'auto.offset.reset' = 'earliest';

CREATE TABLE turnstile (
    station_id int,
    station_name varchar,
    line integer
) WITH (
    kafka_topic='cta.turnstiles',
    key='station_id',
    value_format='avro'
);

CREATE TABLE turnstile_summary
WITH (
    kafka_topic='cta.turnstiles-summary',
    value_format='json'
)
AS
    SELECT station_id, station_name, count(*) as count
    FROM turnstile
    GROUP BY station_id, station_name
;
"""


def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    if topic_check.topic_exists("cta.turnstiles-summary") is True:
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

    try:
        resp.raise_for_status()
    except requests.exceptions.HTTPError as e:
        logger.error(f"Error with KSQL POST request - {e}")


if __name__ == "__main__":
    execute_statement()
