"""Methods pertaining to loading and configuring CTA "L" station data."""
import logging

from pathlib import Path
from typing import ClassVar, Optional, TYPE_CHECKING
from this import d

from confluent_kafka import avro

from models import Turnstile
from models.common import get_topic_safe_station_name, time_millis
from models.producer import Producer

if TYPE_CHECKING:
    from avro.schema import RecordSchema
    from models.train import Train


logger = logging.getLogger(__name__)


class Station(Producer):
    """Defines a single station"""

    key_schema: ClassVar["RecordSchema"] = avro.load(f"{Path(__file__).parents[0]}/schemas/arrival_key.json")
    value_schema: ClassVar["RecordSchema"] = avro.load(f"{Path(__file__).parents[0]}/schemas/arrival_value.json")

    def __init__(self,
        station_id: str,
        name: str,
        color: int,
        direction_a: Optional["Station"] = None,
        direction_b: Optional["Station"] = None
    ):
        self.name = name
        station_name = get_topic_safe_station_name(name)

        topic_name = f"com.udacity.nd029.p1.v1.arrival.{station_name}"
        super().__init__(
            topic_name,
            key_schema=Station.key_schema,
            value_schema=Station.value_schema,
            num_partitions=10,
            num_replicas=1
        )

        self.station_id = int(station_id)
        self.color = color
        self.dir_a = direction_a
        self.dir_b = direction_b
        self.a_train = None
        self.b_train = None
        self.turnstile = Turnstile(self)

    def run(self,
        train: "Train",
        direction: str,
        prev_station_id: str,
        prev_direction: str
    ):
        """Simulates train arrivals at this station"""
        self.producer.produce(
            topic=self.topic_name,
            key={"timestamp": time_millis()},
            value={
                "station_id": self.station_id,
                "train_id": train.train_id,
                "direction": direction,
                "line": self.color.name,
                "train_status": train.status.name,
                "prev_station_id": prev_station_id,
                "prev_direction": prev_direction
            }
        )

    def __str__(self) -> str:
        return "Station | {:^5} | {:<30} | Direction A: | {:^5} | departing to {:<30} | Direction B: | {:^5} | departing to {:<30} | ".format(
            self.station_id,
            self.name,
            self.a_train.train_id if self.a_train is not None else "---",
            self.dir_a.name if self.dir_a is not None else "---",
            self.b_train.train_id if self.b_train is not None else "---",
            self.dir_b.name if self.dir_b is not None else "---",
        )

    def __repr__(self) -> str:
        return str(self)

    def arrive_a(
        self,
        train: "Train",
        prev_station_id: Optional[int],
        prev_direction: Optional[str]
    ):
        """Denotes a train arrival at this station in the 'a' direction"""
        self.a_train = train
        self.run(train, "a", prev_station_id, prev_direction)

    def arrive_b(
        self,
        train: "Train",
        prev_station_id: Optional[int],
        prev_direction: Optional[str]
    ):
        """Denotes a train arrival at this station in the 'b' direction"""
        self.b_train = train
        self.run(train, "b", prev_station_id, prev_direction)

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        self.turnstile.close()
        super(Station, self).close()
