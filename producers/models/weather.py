"""Methods pertaining to weather data"""
import json
import logging
import random
import urllib.parse

from os import environ
from enum import IntEnum
from pathlib import Path
from typing import Any, ClassVar, Final

import requests

from confluent_kafka import avro

from models.common import time_millis
from models.producer import Producer


logger = logging.getLogger(__name__)


class Weather(Producer):
    """Defines a simulated weather model"""

    status: ClassVar["status"] = IntEnum(
        "status", "sunny partly_cloudy cloudy windy precipitation", start=0
    )

    topic_name: ClassVar[str] = "com.udacity.nd029.p1.v1.weather"

    rest_proxy_url: ClassVar[str] = environ.get("REST_PROXY_URL") or "http://localhost:8082"

    # We're doing a load/dumps combination to validate the schemas
    key_schema: ClassVar[str] = json.dumps(
        avro.load(f"{Path(__file__).parents[0]}/schemas/weather_key.json").to_json()
    )
    value_schema: ClassVar[str] = json.dumps(
        avro.load(f"{Path(__file__).parents[0]}/schemas/weather_value.json").to_json()
    )

    _WINTER_MONTHS: Final[set[str]] = set((0, 1, 2, 3, 10, 11))
    _SUMMER_MONTHS: Final[set[str]] = set((6, 7, 8))

    _headers: ClassVar[dict[str, str]] = {
        "Content-Type": "application/vnd.kafka.avro.v2+json"
    }

    def __init__(self, month):
        # We don't set the schemas since we're going to use REST proxy
        super().__init__(
            Weather.topic_name,
            key_schema=None,
            value_schema=None,
            num_partitions=4,
            num_replicas=1,
            create_producer=False
        )

        self.status = Weather.status.sunny
        self.temp = 70.0
        
        if month in Weather._WINTER_MONTHS:
            self.temp = 40.0
        elif month in Weather._SUMMER_MONTHS:
            self.temp = 85.0

    def _set_weather(self, month):
        """Returns the current weather"""
        mode = 0.0
        
        if month in Weather._SUMMER_MONTHS:
            mode = -1.0
        elif month in Weather._SUMMER_MONTHS:
            mode = 1.0
        
        self.temp += min(max(-20.0, random.triangular(-10.0, 10.0, mode)), 100.0)
        self.status = random.choice(list(Weather.status))

    def run(self, month):
        self._set_weather(month)

        response = requests.post(
            f"{Weather.rest_proxy_url}/topics/{Weather.topic_name}",
            headers=Weather._headers,
            json={
                "key_schema": Weather.key_schema,
                "value_schema": Weather.value_schema,
                "records": [
                    {
                        "key": {"timestamp": time_millis()},
                        "value": {
                            "temperature": self.temp,
                            "status": self.status.name  # Sending the label instead of the enum valuue
                        },
                    }
                ]
            }
        )

        response.raise_for_status()

        logger.debug(
            "sent weather data to kafka, temp: %s, status: %s",
            self.temp,
            self.status.name,
        )
