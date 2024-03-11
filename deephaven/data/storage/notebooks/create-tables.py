from deephaven.json import json, instant_
from deephaven.stream.kafka.consumer import (
    consume,
    object_processor_spec,
    TableType,
    KeyValueSpec,
    ALL_PARTITIONS_SEEK_TO_BEGINNING,
)

config = {"bootstrap.servers": "redpanda:9092"}

current_schema = {
    "name": str,
    "region": str,
    "country": str,
    "lat": float,
    "lon": float,
    "tz_id": str,
    "localtime_epoch": instant_(number_format="s"),
    "last_updated_epoch": instant_(number_format="s"),
    "temp_c": float,
    "temp_f": float,
    "wind_mph": float,
    "wind_kph": float,
    "wind_degree": int,
    "wind_dir": str,
    "pressure_mb": float,
    "pressure_in": float,
    "precip_mm": float,
    "precip_in": float,
    "humidity": int,
    "cloud": int,
    "feelslike_c": float,
    "feelslike_f": float,
    "uv": float,
    "gust_mph": float,
    "gust_kph": float,
    "co": float,
    "no2": float,
    "o3": float,
    "so2": float,
    "pm2_5": float,
    "pm10": float
}

historical_schema = {
    "name": str,
    "region": str,
    "country": str,
    "lat": float,
    "lon": float,
    "tz_id": str,
    "time_epoch": instant_(number_format="s"),
    "temp_c": float,
    "temp_f": float,
    "wind_mph": float,
    "wind_kph": float,
    "wind_degree": int,
    "wind_dir": str,
    "pressure_mb": float,
    "pressure_in": float,
    "precip_mm": float,
    "precip_in": float,
    "humidity": int,
    "cloud": int,
    "feelslike_c": float,
    "feelslike_f": float,
    "uv": float,
    "gust_mph": float,
    "gust_kph": float,
    "co": float,
    "no2": float,
    "o3": float,
    "so2": float,
    "pm2_5": float,
    "pm10": float
}
    #"us-epa-index": int,
    #"gb-defra-index": int})

# Up-to-date "blink" table for Texas grid

current = consume(
        kafka_config=config,
        topic="current",
        offsets=ALL_PARTITIONS_SEEK_TO_BEGINNING,
        key_spec=KeyValueSpec.IGNORE,
        value_spec=object_processor_spec(json(current_schema)),
        table_type=TableType.ring(1050)) \
    .drop_columns([
        "KafkaPartition",
        "KafkaOffset",
        "KafkaTimestamp"]) \
    .update("tz_id = parseTimeZone(tz_id)") \
    .rename_columns([
        "localtime = localtime_epoch",
        "last_updated = last_updated_epoch"])


# Up-to-date "blink" table for current location

current_here = consume(
        kafka_config=config,
        topic="current-here",
        offsets=ALL_PARTITIONS_SEEK_TO_BEGINNING,
        key_spec=KeyValueSpec.IGNORE,
        value_spec=object_processor_spec(json(current_schema)),
        table_type=TableType.ring(1)) \
    .drop_columns([
        "KafkaPartition",
        "KafkaOffset",
        "KafkaTimestamp"]) \
    .update("tz_id = parseTimeZone(tz_id)") \
    .rename_columns([
        "localtime = localtime_epoch",
        "last_updated = last_updated_epoch"])


# Historical table for Texas grid

historical = consume(
        kafka_config=config,
        topic="history",
        offsets=ALL_PARTITIONS_SEEK_TO_BEGINNING,
        key_spec=KeyValueSpec.IGNORE,
        value_spec=object_processor_spec(json(historical_schema)),
        table_type=TableType.append()) \
    .drop_columns([
        "KafkaPartition",
        "KafkaOffset",
        "KafkaTimestamp"]) \
    .update("tz_id = parseTimeZone(tz_id)") \
    .rename_columns("time = time_epoch")

# Historical table for current location

historical_here = consume(
        kafka_config=config,
        topic="history-here",
        offsets=ALL_PARTITIONS_SEEK_TO_BEGINNING,
        key_spec=KeyValueSpec.IGNORE,
        value_spec=object_processor_spec(json(historical_schema)),
        table_type=TableType.append()) \
    .drop_columns([
        "KafkaPartition",
        "KafkaOffset",
        "KafkaTimestamp"]) \
    .update("tz_id = parseTimeZone(tz_id)") \
    .rename_columns("time = time_epoch")

p_historical = historical.partition_by("name")
