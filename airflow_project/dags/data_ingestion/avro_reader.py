"""Chunked Avro reader for dlt filesystem sources (Kafka Connect / Debezium value shape).

Reads Snappy (or other codec) Avro from ``filesystem`` FileItems, then turns fastavro’s
union + map encoding into plain dicts—same idea as a deserialized Kafka topic value.
"""

from __future__ import annotations

from typing import Any, Iterable, Iterator

import dlt
from dlt.sources import TDataItems
from dlt.sources.filesystem import FileItemDict

# Single-branch Avro unions appear as {"string": x}, {"long": x}, {"map": [...]}, …
_BRANCH = frozenset(
    {"string", "long", "int", "float", "double", "boolean", "bytes", "null", "array", "map", "enum", "fixed"}
)


def _is_kv_map(entries: list[Any]) -> bool:
    return not entries or (isinstance(entries[0], dict) and "key" in entries[0] and "value" in entries[0])


def _kafka_connect_value(obj: Any) -> Any:
    """Strip union Nones, unwrap ``{"long": n}``-style branches, map KV lists → dict."""
    if obj is None:
        return None
    if isinstance(obj, list):
        return [_kafka_connect_value(x) for x in obj]
    if not isinstance(obj, dict):
        return obj

    d = {k: v for k, v in obj.items() if v is not None}

    m = d.get("map")
    if isinstance(m, list) and _is_kv_map(m):
        out: dict[str, Any] = {}
        for e in m:
            if not isinstance(e, dict):
                continue
            pk = _kafka_connect_value(e.get("key"))
            if not isinstance(pk, str):
                pk = str(pk)
            out[pk] = _kafka_connect_value(e.get("value"))
        return out

    if len(d) == 1:
        k, v = next(iter(d.items()))
        if k in _BRANCH:
            if k == "array" and isinstance(v, list):
                return [_kafka_connect_value(x) for x in v]
            if k == "map" and isinstance(v, list):
                # Not a Connect KV map (handled above); treat as array of entries
                return [_kafka_connect_value(x) for x in v]
            return _kafka_connect_value(v) if isinstance(v, (dict, list)) else v

    return {k: _kafka_connect_value(v) for k, v in d.items()}


def _read_avro(
    items: Iterable[FileItemDict],
    chunksize: int = 1000,
) -> Iterator[TDataItems]:
    from fastavro import reader as avro_reader

    for file_obj in items:
        with file_obj.open(mode="rb", compression="disable") as f:
            r = avro_reader(f)
            chunk: list[Any] = []
            for record in r:
                chunk.append(_kafka_connect_value(record))
                if len(chunk) >= chunksize:
                    yield chunk
                    chunk = []
            if chunk:
                yield chunk


read_avro = dlt.transformer()(_read_avro)
