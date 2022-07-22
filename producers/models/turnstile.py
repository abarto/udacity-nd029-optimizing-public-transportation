"""Creates a turnstile data producer"""
from datetime import timedelta
import logging
from pathlib import Path
from typing import ClassVar, TYPE_CHECKING

from confluent_kafka import avro

from models.common import get_topic_safe_station_name, time_millis
from models.producer import Producer
from models.turnstile_hardware import TurnstileHardware

if TYPE_CHECKING:
    from datetime import datetime, timedelta
    from avro.schema import RecordSchema
    from models.station import Station

logger = logging.getLogger(__name__)


class Turnstile(Producer):
    key_schema: ClassVar["RecordSchema"] = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_key.json")
    value_schema: ClassVar["RecordSchema"] = avro.load(
       f"{Path(__file__).parents[0]}/schemas/turnstile_value.json"
    )

    def __init__(self, station: "Station"):
        """Create the Turnstile"""
        super().__init__(
            'com.udacity.nd029.p1.v1.turnstile',  # Creating a single topic as per https://knowledge.udacity.com/questions/874361
            key_schema=Turnstile.key_schema,
            value_schema=Turnstile.value_schema,
            num_partitions=4,
            num_replicas=1,
        )
        self.station = station
        self.turnstile_hardware = TurnstileHardware(station)

    def run(self, timestamp: "datetime", time_step: "timedelta"):
        """Simulates riders entering through the turnstile."""
        num_entries: int = self.turnstile_hardware.get_entries(timestamp, time_step)

        for _ in range(num_entries):
            self.producer.produce(
                topic=self.topic_name,
                key={"timestamp": time_millis()},
                value={
                    "station_id": self.station.station_id,
                    "station_name": self.station.name,
                    "line": self.station.color
                }
            )
