"""Contains functionality related to Weather"""
import logging

from enum import IntEnum


logger = logging.getLogger(__name__)

_STATUS = IntEnum(
    "status", "sunny partly_cloudy cloudy windy precipitation", start=0
)


class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"

    def process_message(self, message):
        """Handles incoming weather data"""

        value = message.value()

        self.temperature = value["temperature"]
        self.status = _STATUS(value["status"])
