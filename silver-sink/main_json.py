"""
S3 files → StreamingDataFrame → S3 (JSON).

Reads records from S3 (JSON or Parquet), optionally prints with metadata,
and sinks to S3 as JSON using the community S3 file source and sink.

The S3 file source is a one-shot batch source: it lists files under the prefix
**once** when the run starts, then processes that snapshot and exits. It does
**not** watch for new files. Files created after the list (e.g. ...+0000000004.json)
will not be seen until you run this script again. To ingest new data regularly,
run on a schedule (e.g. cron) or consume from Kafka for continuous ingestion.

Docs:
- S3 Source: https://quix.io/docs/quix-streams/connectors/sources/amazon-s3-source.html
- S3 Sink: https://quix.io/docs/quix-streams/connectors/sinks/amazon-s3-sink.html
"""
import os
from typing import Any

from quixstreams import Application
from quixstreams.kafka.configuration import ConnectionConfig
from quixstreams.sinks.community.file.formats import JSONFormat
from quixstreams.sinks.community.file.s3 import S3FileSink
from quixstreams.sources.community.file.s3 import S3FileSource


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
    file_format = os.getenv("S3_SOURCE_FORMAT", "json").lower()
    compression = os.getenv("S3_SOURCE_COMPRESSION") or None
    replay_speed = float(os.getenv("S3_SOURCE_REPLAY_SPEED", "0.5"))

    if not bucket:
        raise ValueError("Set S3_SOURCE_BUCKET (and optionally S3_SOURCE_FILEPATH)")

    source = S3FileSource(
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
    )

    sink_bucket = os.getenv("S3_SINK_BUCKET", "silver")
    sink_directory = os.getenv(
        "S3_SINK_DIRECTORY",
        "event_streaming/postgres1.inventory.customers",
    )
    sink_compress = os.getenv("S3_SINK_COMPRESS", "false").lower() in ("1", "true", "yes")
    if not sink_bucket:
        raise ValueError("Set S3_SINK_BUCKET for the S3 file sink")

    file_sink = S3FileSink(
        bucket=sink_bucket,
        region_name=region,
        directory=sink_directory,
        format=JSONFormat(compress=sink_compress),
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        endpoint_url=endpoint_url,
    )

    connection_config = _create_connection_config()
    app = Application(
        broker_address=connection_config,
        consumer_group="stream-ingestions-s3",
        auto_offset_reset="latest",
    )
    sdf = app.dataframe(source=source)
    if os.getenv("STREAM_PRINT", "false").lower() in ("1", "true", "yes"):
        sdf.print(metadata=True)
    sdf.sink(file_sink)
    app.run()


if __name__ == "__main__":
    main()
