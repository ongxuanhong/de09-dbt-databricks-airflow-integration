"""
S3 Avro → StreamingDataFrame → Delta.

Reads Avro records from S3 (e.g. Debezium CDC), optionally prints with metadata,
and sinks to a Delta table on object storage.

The S3 file source is a one-shot batch source: it lists files under the prefix
at run time, processes them, then completes. It does not watch for new files.
To ingest new bronze data (e.g. after the S3 sink connector writes more Avro):
  - Run this app on a schedule (e.g. cron), or
  - Consume from Kafka (postgres1.* topics) for continuous ingestion instead of S3.

Docs:
- S3 Source: https://quix.io/docs/quix-streams/connectors/sources/amazon-s3-source.html
"""
import os
from typing import Any

from quixstreams import Application
from quixstreams.kafka.configuration import ConnectionConfig

from delta_sink import DeltaSink
from s3_avro_source import S3FileSourceWithAvro


def _create_connection_config() -> ConnectionConfig | None:
    broker = "localhost:29092"
    return ConnectionConfig(bootstrap_servers=broker)


def _key_setter(record: dict) -> str:
    """Kafka key from record. Quix FileSink writes _key."""
    key = record.get("_key")
    if key is None:
        return ""
    return key if isinstance(key, str) else str(key)


def _value_setter(record: dict) -> dict[str, Any]:
    """Kafka value from record. Quix FileSink writes _value."""
    return record.get("_value") if isinstance(record.get("_value"), dict) else record


def _timestamp_setter(record: dict) -> int:
    """Kafka timestamp (ms). Quix FileSink writes _timestamp in ms."""
    ts = record.get("_timestamp")
    if ts is None:
        return 0
    try:
        return int(ts)
    except (TypeError, ValueError):
        return 0


def main() -> None:
    bucket = os.getenv("S3_SOURCE_BUCKET", "bronze")
    filepath = os.getenv("S3_SOURCE_FILEPATH", "event_streaming/postgres1.inventory.customers")
    region = os.getenv("AWS_REGION", "us-west-2")
    endpoint_url = os.getenv("AWS_ENDPOINT_URL_S3", "http://localhost:9000")
    # MinIO default credentials when using local MinIO; override with AWS_* env for real AWS or custom MinIO
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
    file_format = os.getenv("S3_SOURCE_FORMAT", "avro").lower()
    compression = os.getenv("S3_SOURCE_COMPRESSION") or None
    replay_speed = float(os.getenv("S3_SOURCE_REPLAY_SPEED", "0.5"))
    skip_bucket_check = os.getenv("S3_SKIP_BUCKET_CHECK", "true").lower() in ("1", "true", "yes")

    if not bucket:
        raise ValueError("Set S3_SOURCE_BUCKET (and optionally S3_SOURCE_FILEPATH)")

    source = S3FileSourceWithAvro(
        filepath=filepath,
        bucket=bucket,
        region_name=region,
        endpoint_url=endpoint_url,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        key_setter=_key_setter,
        value_setter=_value_setter,
        timestamp_setter=_timestamp_setter,
        file_format=file_format,
        compression=compression,
        has_partition_folders=True,
        replay_speed=replay_speed,
        skip_bucket_check=skip_bucket_check,
    )

    delta_table_uri = os.getenv(
        "DELTA_TABLE_URI",
        "s3://silver/event_streaming/postgres1.inventory.customers",
    )
    if not delta_table_uri:
        raise ValueError("Set DELTA_TABLE_URI for the Delta sink")

    connection_config = _create_connection_config()
    app = Application(
        broker_address=connection_config,
        consumer_group="stream-ingestions-s3",
        auto_offset_reset="latest",
    )
    sdf = app.dataframe(source=source)
    if os.getenv("STREAM_PRINT", "false").lower() in ("1", "true", "yes"):
        sdf.print(metadata=True)
    sdf.sink(DeltaSink(table_uri=delta_table_uri))
    app.run()


if __name__ == "__main__":
    main()
