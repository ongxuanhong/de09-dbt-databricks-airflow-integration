import json
import os
import time
from typing import Any

import pyarrow as pa
from deltalake import write_deltalake
from quixstreams.sinks import BatchingSink, SinkBackpressureError, SinkBatch


class DeltaSink(BatchingSink):
    """Write consumed Kafka records into a Delta table on object storage."""

    def __init__(self, table_uri: str) -> None:
        super().__init__()
        self.table_uri = table_uri
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

    def _to_arrow_table(self, data: list[Any]) -> pa.Table:
        payloads = [json.dumps(item, default=str) for item in data]
        return pa.table({"payload": payloads})

    def _write_to_delta(self, data: list[Any]) -> None:
        if not data:
            return
        print(f"Writing batch of {len(data)} records to Delta table...")
        arrow_table = self._to_arrow_table(data)
        write_deltalake(
            self.table_uri,
            arrow_table,
            mode="append",
            storage_options=self.storage_options,
        )

    def write(self, batch: SinkBatch) -> None:
        attempts_remaining = 3
        data = [item.value for item in batch]
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
