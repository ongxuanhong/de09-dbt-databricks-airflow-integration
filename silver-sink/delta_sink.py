import json
import os
import time
from datetime import datetime
from typing import Any, Sequence

import pyarrow as pa
from deltalake import write_deltalake
from quixstreams.sinks import BatchingSink, SinkBackpressureError, SinkBatch

class DeltaSink(BatchingSink):
    """Write consumed Kafka records into a Delta table on object storage with optional date partitioning.

    For Debezium CDC payloads, set payload_key='after' to store the change row as separate columns
    (id, first_name, last_name, email, ...) instead of a single JSON payload column.
    """

    def __init__(
        self,
        table_uri: str,
        timestamp_column: str = "ts_ms",
        partition_columns: Sequence[str] = ("year", "month", "day"),
        payload_key: str = "after",
    ) -> None:
        super().__init__()
        self.table_uri = table_uri
        self.partition_columns = list(partition_columns)
        self.timestamp_column = timestamp_column
        self.payload_key = payload_key
        self.endpoint_url = (
            os.getenv("AWS_ENDPOINT_URL_S3") or "http://localhost:9000"
        ).strip()
        if not self.endpoint_url.startswith("http"):
            self.endpoint_url = f"http://{self.endpoint_url}"
        self.storage_options = self._create_storage_options()

    def _create_storage_options(self) -> dict[str, str]:
        options: dict[str, str] = {}
        # Custom endpoint (e.g. MinIO) requires explicit credentials so the client
        # does not fall back to AWS credential chain (IMDS 169.254.169.254).
        access_key = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
        secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
        options["AWS_ACCESS_KEY_ID"] = access_key
        options["AWS_SECRET_ACCESS_KEY"] = secret_key
        options["AWS_REGION"] = os.getenv("AWS_REGION", "us-west-2")
        options["AWS_ENDPOINT_URL"] = self.endpoint_url
        if self.endpoint_url.lower().startswith("http://"):
            options["AWS_ALLOW_HTTP"] = options["allow_http"] = "true"
        options["aws_conditional_put"] = "etag"
        path_style = os.getenv("DELTA_S3_FORCE_PATH_STYLE", "true").lower()
        options["AWS_S3_ADDRESSING_STYLE"] = (
            "path" if path_style == "true" else "virtual"
        )
        return options

    @staticmethod
    def _scalar_value(value: Any) -> Any:
        """Convert value to a Delta/Arrow-friendly scalar."""
        if value is None or isinstance(value, (bool, int, float, str)):
            return value
        if isinstance(value, (dict, list)):
            return json.dumps(value, default=str)
        return str(value)

    def _partition_from_timestamp(
        self,
        ts_ms: int | float | None,
    ) -> tuple[str, str, str]:
        """
        Derive (year, month, day) from a millisecond Unix timestamp.

        Returns zero-padded strings so partition paths sort correctly (e.g. month=03, day=05).
        Returns ("0000", "00", "00") if input is missing or invalid.
        """
        if ts_ms is None:
            return "0000", "00", "00"
        try:
            dt = datetime.fromtimestamp(int(ts_ms) / 1000.0)
            return (
                f"{dt.year:04d}",
                f"{dt.month:02d}",
                f"{dt.day:02d}",
            )
        except (TypeError, ValueError, OSError):
            return "0000", "00", "00"

    def _extract_row(self, record: dict[str, Any]) -> dict[str, Any]:
        """Extract a flat row from a CDC record: payload_key (e.g. 'after') fields + partition columns."""
        payload = record.get(self.payload_key)
        if not isinstance(payload, dict):
            payload = {}
        row = {k: self._scalar_value(v) for k, v in payload.items()}
        if self.partition_columns:
            y, m, d = self._partition_from_timestamp(record.get(self.timestamp_column))
            row["year"] = y
            row["month"] = m
            row["day"] = d
        return row

    def _to_arrow_table(self, data: list[Any]) -> pa.Table:
        """Build an Arrow table from CDC records: one column per payload field plus partition columns."""
        records = [item if isinstance(item, dict) else {} for item in data]
        rows = [self._extract_row(r) for r in records]
        if not rows:
            return pa.table({})

        # Stable column order: data columns (sorted) then partition columns.
        data_keys = sorted(
            {k for row in rows for k in row.keys() if k not in self.partition_columns}
        )
        partition_keys = list(self.partition_columns)
        all_columns = data_keys + partition_keys

        # Ensure every row has every key (fill missing with None)
        full_rows = [{c: row.get(c) for c in all_columns} for row in rows]
        return pa.Table.from_pylist(full_rows)

    def _write_to_delta(self, data: list[Any]) -> None:
        if not data:
            return
        print(f"Writing batch of {len(data)} records to Delta table...")
        arrow_table = self._to_arrow_table(data)
        write_deltalake(
            self.table_uri,
            arrow_table,
            mode="append",
            partition_by=self.partition_columns if self.partition_columns else None,
            storage_options=self.storage_options,
        )

    def write(self, batch: SinkBatch) -> None:
        attempts_remaining = 3
        data = [item.value for item in batch]
        if data:
            print(
                f"Writing batch of {len(data)} records to Delta table "
                f"(topic={batch.topic}, table_uri={self.table_uri})..."
            )
        while attempts_remaining:
            try:
                return self._write_to_delta(data)
            except ConnectionError:
                attempts_remaining -= 1
                if attempts_remaining:
                    time.sleep(3)
            except TimeoutError:
                raise SinkBackpressureError(
                    retry_after=30.0,
                    topic=batch.topic,
                    partition=batch.partition,
                ) from None
        err_msg = "Error while writing data to Delta table"
        raise RuntimeError(err_msg)
