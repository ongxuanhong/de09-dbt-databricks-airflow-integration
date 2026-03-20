from datetime import datetime, timezone

# Column used for incremental loading and for deriving partition columns
TIMESTAMP_COLUMN = "ts_ms"
TABLE_NAME = "customers"


def _numeric_epoch_to_seconds(value: float) -> float:
    """Normalize numeric epoch to Unix seconds.

    `fromtimestamp` expects seconds. Values like ``1773979114854`` are **milliseconds**
    (13 digits). Order matters: check larger units first so ms is not misread as μs.
    """
    v = float(value)
    av = abs(v)
    if av >= 1e17:  # nanoseconds (e.g. 1.77e18)
        return v / 1e9
    if av >= 1e15:  # microseconds (e.g. 1.77e15)
        return v / 1e6
    if av >= 1e12:  # milliseconds (e.g. 1773979114854)
        return v / 1e3
    return v


def _parse_ts(value):
    """Parse timestamp: ISO string, Unix seconds, or ms/μs/ns epoch numbers to datetime (UTC)."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        sec = _numeric_epoch_to_seconds(value)
        return datetime.fromtimestamp(sec, tz=timezone.utc)
    if isinstance(value, str):
        s = value.strip().replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(s)
        except ValueError:
            pass
    return None


def _add_partition_from_timestamp(record, meta=None):
    """Add year, month, day, hour from the timestamp column for Delta partitioning."""
    out = dict(record)
    ts_val = record.get(TIMESTAMP_COLUMN)
    dt = _parse_ts(ts_val)
    if dt is None:
        # Fallback to now if missing/invalid
        dt = datetime.now(timezone.utc)
    out["year"] = dt.year
    out["month"] = f"{dt.month:02d}"
    out["day"] = f"{dt.day:02d}"
    return out


def get_incremental_last_value(pipe, resource_name: str, cursor_path: str):
    """Read last_value from pipeline state after a run.

    Use this after pipeline.run() to get the cursor value that will be used as
    start_value on the next incremental run. State is stored under
    sources -> <source> -> resources -> <resource_name> -> incremental -> <cursor_path>.
    """
    with pipe.managed_state() as state:
        sources = state.get("sources", {})
        for source_data in sources.values():
            for res_name, res_data in source_data.get("resources", {}).items():
                if res_name != resource_name:
                    continue
                inc = res_data.get("incremental", {})
                cursor_state = inc.get(cursor_path)
                if cursor_state is not None:
                    return cursor_state.get("last_value")
    return None


def run_bronze_to_silver_pipeline() -> None:
    """Execute the bronze → silver dlt pipeline (incremental Delta write)."""
    import dlt
    from dlt.destinations import filesystem as des_filesystem
    from dlt.sources.filesystem import filesystem as src_filesystem
    from dlt.sources.filesystem import read_jsonl

    current_pipeline = dlt.current.pipeline()
    print("Current pipeline:", current_pipeline)

    src_files = src_filesystem(incremental=dlt.sources.incremental("modification_date"))
    des_files = des_filesystem()
    reader = (
        (src_files | read_jsonl())
        .with_name(TABLE_NAME)
        .add_map(_add_partition_from_timestamp)
        .apply_hints(
            incremental=dlt.sources.incremental(TIMESTAMP_COLUMN),
            columns={
                "year": {"partition": True},
                "month": {"partition": True},
                "day": {"partition": True},
            },
        )
    )
    pipeline = dlt.pipeline(
        pipeline_name="bronze_to_silver_pipeline",
        dataset_name="event_streaming",
        destination=des_files,
        progress="log",
    )

    info = pipeline.run(reader, write_disposition="append", table_format="delta")
    print(info)

    last_value = get_incremental_last_value(pipeline, TABLE_NAME, TIMESTAMP_COLUMN)
    print(f"Incremental last_value (next run start): {last_value}")


if __name__ == "__main__":
    run_bronze_to_silver_pipeline()
