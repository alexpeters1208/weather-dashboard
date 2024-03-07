from deephaven.json import json, instant_
from deephaven.stream.kafka.consumer import (
    consume,
    object_processor_spec,
    TableType,
    KeyValueSpec,
    ALL_PARTITIONS_SEEK_TO_BEGINNING,
)

# Consume and create real time "blink" table

current = consume(
    {"bootstrap.servers": "redpanda:9092"},
    "current",
    offsets=ALL_PARTITIONS_SEEK_TO_BEGINNING,
    key_spec=KeyValueSpec.IGNORE,
    value_spec=object_processor_spec(
        json({
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
            "gust_kph": float})
    ),
    table_type=TableType.ring(265), # this is not an ideal solution
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

current_imperial = current \
    .drop_columns(["temp_c", "wind_kph", "pressure_mb", "precip_mm", "feelslike_c", "gust_kph"])

current_metric = current \
    .drop_columns(["temp_f", "wind_mph", "pressure_in", "precip_in", "feelslike_f", "gust_mph"])

test = current.snapshot_when(current.view("stamped_time = now()"), "stamped_time", incremental=True)


# Consume and create historical append table

historical = consume(
    {"bootstrap.servers": "redpanda:9092"},
    "history",
    offsets=ALL_PARTITIONS_SEEK_TO_BEGINNING,
    key_spec=KeyValueSpec.IGNORE,
    value_spec=object_processor_spec(
        json({
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
            "gust_kph": float})
    ),
    table_type=TableType.append(),
)

historical = historical \
    .drop_columns([
        "KafkaPartition",
        "KafkaOffset",
        "KafkaTimestamp"]) \
    .update("tz_id = parseTimeZone(tz_id)") \
    .rename_columns("time = time_epoch")

historical_imperial = historical \
    .drop_columns(["temp_c", "wind_kph", "pressure_mb", "precip_mm", "feelslike_c", "gust_kph"])

historical_metric = historical \
    .drop_columns(["temp_f", "wind_mph", "pressure_in", "precip_in", "feelslike_f", "gust_mph"])

p_historical_imperial = historical_imperial.partition_by("name")
p_historical_metric = historical_metric.partition_by("name")
