from time import time


def time_millis() -> int:
    """Use this function to get the key for Kafka Events"""
    return int(round(time.time() * 1000))


def get_topic_safe_station_name(station_name: str) -> str:
    """Converts a station name into a topic name safe string"""
    return (
        station_name.lower()
            .replace("/", "_and_")
            .replace(" ", "_")
            .replace("-", "_")
            .replace("'", "")
    )
