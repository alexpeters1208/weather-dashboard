from deephaven.json import json, instant_
from deephaven.stream.kafka.consumer import (
    consume,
    object_processor_spec,
    TableType,
    KeyValueSpec,
    ALL_PARTITIONS_SEEK_TO_BEGINNING,
)

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

# Consume and create real time "blink" table for grid of locations

current = consume(
    {"bootstrap.servers": "redpanda:9092"},
    "current",
    offsets=ALL_PARTITIONS_SEEK_TO_BEGINNING,
    key_spec=KeyValueSpec.IGNORE,
    value_spec=object_processor_spec(json(current_schema)),
    table_type=TableType.ring(265) # this is not an ideal solution
)

current = current \
    .drop_columns([
        "KafkaPartition",
        "KafkaOffset",
        "KafkaTimestamp"]) \
    .update("tz_id = parseTimeZone(tz_id)") \
    .rename_columns([
        "localtime = localtime_epoch",
        "last_updated = last_updated_epoch"])


# Create and consume real-time table for current location

current_here = consume(
    {"bootstrap.servers": "redpanda:9092"},
    "current-here",
    offsets=ALL_PARTITIONS_SEEK_TO_BEGINNING,
    key_spec=KeyValueSpec.IGNORE,
    value_spec=object_processor_spec(json(current_schema)),
    table_type=TableType.ring(1) # this is not an ideal solution
)

current_here = current_here \
    .drop_columns([
        "KafkaPartition",
        "KafkaOffset",
        "KafkaTimestamp"]) \
    .update("tz_id = parseTimeZone(tz_id)") \
    .rename_columns([
        "localtime = localtime_epoch",
        "last_updated = last_updated_epoch"])


# Consume and create historical append table

historical = consume(
    {"bootstrap.servers": "redpanda:9092"},
    "history",
    offsets=ALL_PARTITIONS_SEEK_TO_BEGINNING,
    key_spec=KeyValueSpec.IGNORE,
    value_spec=object_processor_spec(json(historical_schema)),
    table_type=TableType.append()
)

historical = historical \
    .drop_columns([
        "KafkaPartition",
        "KafkaOffset",
        "KafkaTimestamp"]) \
    .update("tz_id = parseTimeZone(tz_id)") \
    .rename_columns("time = time_epoch")

historical_here = consume(
    {"bootstrap.servers": "redpanda:9092"},
    "history-here",
    offsets=ALL_PARTITIONS_SEEK_TO_BEGINNING,
    key_spec=KeyValueSpec.IGNORE,
    value_spec=object_processor_spec(json(historical_schema)),
    table_type=TableType.append()
)

historical_here = historical_here \
    .drop_columns([
        "KafkaPartition",
        "KafkaOffset",
        "KafkaTimestamp"]) \
    .update("tz_id = parseTimeZone(tz_id)") \
    .rename_columns("time = time_epoch")

p_historical = historical.partition_by("name")
