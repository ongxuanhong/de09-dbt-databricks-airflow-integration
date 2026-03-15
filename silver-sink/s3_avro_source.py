"""
S3FileSource with Avro format support.

Rewrites/extends Quix S3FileSource to support file_format="avro" by providing
an AvroFormat that yields records in the same shape as the other formats
({_key, _value, _timestamp}).

Usage:
    from s3_avro_source import S3FileSourceWithAvro as S3FileSource
    source = S3FileSource(..., file_format="avro")
"""
import logging
from io import BytesIO
from os import getenv
from pathlib import Path
from typing import Any, BinaryIO, Callable, Iterable, Optional, Union

import botocore.exceptions

from quixstreams.sources import (
    ClientConnectFailureCallback,
    ClientConnectSuccessCallback,
)
from quixstreams.sources.community.file.base import FileSource
from quixstreams.sources.community.file.compressions import CompressionName
from quixstreams.sources.community.file.formats import Format, FormatName

try:
    from boto3 import client as boto_client
    from mypy_boto3_s3 import S3Client
except ImportError as exc:
    raise ImportError(
        f"Package {exc.name} is missing: "
        'run "pip install quixstreams[s3]" to use S3FileSource'
    ) from exc

logger = logging.getLogger(__name__)

__all__ = (
    "S3FileSourceWithAvro",
    "AvroFormat",
    "MissingS3Bucket",
)


class MissingS3Bucket(Exception): ...


_AVRO_JSON_TYPE_KEYS = frozenset(
    {"boolean", "bytes", "double", "float", "int", "long", "string", "array", "map"}
)


def _avro_value_to_logical(value: Any) -> Any:
    """
    Convert Avro JSON-decoded union/type wrappers to logical Python values.

    - Union types as dicts like {"string": "x", "long": None, ...} are collapsed.
    - Lists of {"key": k, "value": v} (Avro map representation) are turned into dicts.
    Result is plain nested dicts (e.g. Debezium CDC payloads).
    """
    if value is None:
        return None
    if isinstance(value, dict):
        if _AVRO_JSON_TYPE_KEYS.issuperset(value.keys()):
            for v in value.values():
                if v is not None:
                    return _avro_value_to_logical(v)
            return None
        return {k: _avro_value_to_logical(v) for k, v in value.items()}
    if isinstance(value, list):
        # Avro map/record as list of {"key": k, "value": v} → plain dict
        if value and all(
            isinstance(item, dict) and set(item.keys()) == {"key", "value"}
            for item in value
        ):
            return {
                _avro_value_to_logical(item["key"]): _avro_value_to_logical(
                    item["value"]
                )
                for item in value
            }
        return [_avro_value_to_logical(v) for v in value]
    return value


class AvroFormat(Format):
    """
    Format for reading Avro container files produced by Quix FileSink (or compatible).

    Yields dicts with {_key, _value, _timestamp}. If the Avro record already has
    these fields they are used; otherwise the whole record is treated as _value
    with _key and _timestamp defaulted.
    """

    def __init__(self, compression: Optional[CompressionName] = None) -> None:
        try:
            import fastavro  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                "Package fastavro is required for Avro format: "
                'run "pip install fastavro" (or quixstreams[avro] if available)'
            ) from exc
        super().__init__(compression)

    def deserialize(self, filestream: BinaryIO) -> Iterable[dict[str, Any]]:
        import fastavro

        reader = fastavro.reader(filestream)
        for record in reader:
            if not isinstance(record, dict):
                yield {"_key": None, "_timestamp": 0, "_value": _avro_value_to_logical(record)}
                continue
            record = _avro_value_to_logical(record)
            if not isinstance(record, dict):
                yield {"_key": None, "_timestamp": 0, "_value": record}
                continue
            ts = int(record.get("_timestamp") or 0)
            key = record.get("_key")
            value = (
                {k: v for k, v in record.items() if k not in ("_key", "_timestamp")}
                if "_key" in record and "_timestamp" in record
                else record
            )
            yield {"_key": key, "_timestamp": ts, "_value": value}


class S3FileSourceWithAvro(FileSource):
    """
    S3 file source that supports JSON, Parquet, and Avro formats.

    Same as Quix S3FileSource but accepts file_format="avro" and uses AvroFormat
    to deserialize Avro files. Use skip_bucket_check=True to avoid HeadBucket
    when the principal has List/Get but not HeadBucket (e.g. 403 Forbidden).
    """

    def __init__(
        self,
        filepath: Union[str, Path],
        bucket: str,
        region_name: Optional[str] = getenv("AWS_REGION"),
        aws_access_key_id: Optional[str] = getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key: Optional[str] = getenv("AWS_SECRET_ACCESS_KEY"),
        endpoint_url: Optional[str] = getenv("AWS_ENDPOINT_URL_S3"),
        key_setter: Optional[Callable[[object], object]] = None,
        value_setter: Optional[Callable[[object], object]] = None,
        timestamp_setter: Optional[Callable[[object], int]] = None,
        has_partition_folders: bool = False,
        file_format: Union[Format, FormatName, str] = "json",
        compression: Optional[CompressionName] = None,
        replay_speed: float = 1.0,
        name: Optional[str] = None,
        shutdown_timeout: float = 30,
        on_client_connect_success: Optional[ClientConnectSuccessCallback] = None,
        on_client_connect_failure: Optional[ClientConnectFailureCallback] = None,
        skip_bucket_check: bool = False,
    ):
        if file_format == "avro":
            file_format = AvroFormat(compression=compression)
        super().__init__(
            filepath=filepath,
            key_setter=key_setter,
            value_setter=value_setter,
            timestamp_setter=timestamp_setter,
            file_format=file_format,
            compression=compression,
            has_partition_folders=has_partition_folders,
            replay_speed=replay_speed,
            name=name or f"s3_{bucket}_{Path(filepath).name}",
            shutdown_timeout=shutdown_timeout,
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure,
        )
        self._bucket = bucket
        self._credentials = {
            "region_name": region_name,
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "endpoint_url": endpoint_url,
        }
        self._client: Optional[S3Client] = None
        self._skip_bucket_check = skip_bucket_check

    def setup(self) -> None:
        if self._client is None:
            self._client = boto_client("s3", **self._credentials)
            if not self._skip_bucket_check:
                try:
                    self._client.head_bucket(Bucket=self._bucket)
                except botocore.exceptions.ClientError as e:
                    if "404" in str(e):
                        raise MissingS3Bucket(
                            f'S3 bucket "{self._bucket}" does not exist.'
                        ) from e
                    raise

    def get_file_list(self, filepath: Union[str, Path]) -> Iterable[Path]:
        resp = self._client.list_objects(
            Bucket=self._bucket,
            Prefix=str(filepath),
            Delimiter="/",
        )
        for folder in resp.get("CommonPrefixes", []):
            yield from self.get_file_list(folder["Prefix"])

        for file in resp.get("Contents", []):
            if file["Size"] > 0:
                yield Path(file["Key"])

    def read_file(self, filepath: Path) -> BytesIO:
        data = self._client.get_object(Bucket=self._bucket, Key=str(filepath))[
            "Body"
        ].read()
        return BytesIO(data)

    def file_partition_counter(self) -> int:
        self.setup()
        resp = self._client.list_objects(
            Bucket=self._bucket, Prefix=f"{self._filepath}/", Delimiter="/"
        )
        self._close_client()
        return len(resp.get("CommonPrefixes", []))

    def stop(self) -> None:
        self._close_client()
        super().stop()

    def _close_client(self) -> None:
        logger.debug("Closing S3 client session...")
        if self._client:
            try:
                self._client.close()
            except Exception as e:
                logger.error("S3 client session exited non-gracefully: %s", e)
            self._client = None
