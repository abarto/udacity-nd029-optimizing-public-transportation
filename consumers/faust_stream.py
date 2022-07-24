"""Defines trends calculations for stations"""
from dataclasses import dataclass
import logging

from os import environ
from typing import Final

import faust


logger = logging.getLogger(__name__)


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


_STATION_FLAG_TO_COLOR_MAP: Final[dict[tuple[bool, bool, bool], int]] = {
    # red, blue, green
    (True, False, False): "red",
    (False, True, False): "blue",
    (False, False, True): "green"
}


app = faust.App("stations-stream", broker= environ.get("BROKER_URL") or "kafka://localhost:9092", store="memory://")


stations_topic = app.topic(
    "com.udacity.nd029.p1.v1.stations",
    value_type=Station
)


transformed_stations_topic = app.topic(
    "com.udacity.nd029.p1.v1.transformedstations",
    value_type=TransformedStation,
    partitions=1
)


table = app.Table(
   "transformed_station",
   default=TransformedStation,
   partitions=1,
   changelog_topic=transformed_stations_topic,
)


@app.agent(stations_topic)
async def transform_stations(stations):
    async for station in stations:
        table[station.stop_id] = TransformedStation(
            station_id=station.stop_id,
            station_name=station.station_name,
            order=station.order,
            line=_STATION_FLAG_TO_COLOR_MAP.get((station.red, station.blue, station.green), "unknown")
        )

if __name__ == "__main__":
    app.main()
