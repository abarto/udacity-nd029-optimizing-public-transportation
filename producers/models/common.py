def get_topic_safe_station_name(station_name: str) -> str:
    """Converts a station name into a topic name safe string"""
    return (
        station_name.lower()
            .replace("/", "_and_")
            .replace(" ", "_")
            .replace("-", "_")
            .replace("'", "")
    )
