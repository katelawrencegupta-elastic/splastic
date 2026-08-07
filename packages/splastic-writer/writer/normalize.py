"""Normalize Splunk / S2S event dicts before classify + ES bulk."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def _strip_host_prefix(host: str) -> str:
    if host.startswith("host::"):
        return host[len("host::") :]
    return host


def _coerce_timestamp(value: Any) -> str | None:
    """Convert Splunk _time (UNIX seconds or ISO8601) to ISO8601 UTC."""
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=timezone.utc).isoformat().replace(
            "+00:00", "Z"
        )
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.fromtimestamp(float(text), tz=timezone.utc).isoformat().replace(
            "+00:00", "Z"
        )
    except ValueError:
        pass
    try:
        # Accept trailing Z
        normalized = text.replace("Z", "+00:00") if text.endswith("Z") else text
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    except ValueError:
        return None


def normalize_event(raw: dict[str, Any]) -> dict[str, Any]:
    """Port of Logstash filter normalize for cooked NDJSON / uncooked lines."""
    event = dict(raw)

    message = event.get("message")
    if not message:
        if event.get("_raw"):
            message = event.pop("_raw")
        elif isinstance(event.get("event"), dict) and event["event"].get("original"):
            message = event["event"]["original"]
        else:
            message = ""
    event["message"] = message if isinstance(message, str) else str(message)

    if not event.get("splunk_index"):
        splunk = event.get("splunk")
        if isinstance(splunk, dict) and splunk.get("index") is not None:
            event["splunk_index"] = str(splunk["index"])
        elif event.get("index") is not None:
            event["splunk_index"] = str(event["index"])
        else:
            event["splunk_index"] = ""

    event.setdefault("sourcetype", "")
    event.setdefault("source", "")
    if event["sourcetype"] is None:
        event["sourcetype"] = ""
    if event["source"] is None:
        event["source"] = ""

    host = event.get("host")
    if host is None:
        event["host"] = ""
    else:
        event["host"] = _strip_host_prefix(str(host))

    ts = _coerce_timestamp(event.get("_time"))
    if ts:
        event["@timestamp"] = ts
    elif not event.get("@timestamp"):
        event["@timestamp"] = (
            datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        )
    event.pop("_time", None)

    tags = event.get("tags")
    if tags is None:
        event["tags"] = []
    elif isinstance(tags, str):
        event["tags"] = [tags]
    elif not isinstance(tags, list):
        event["tags"] = list(tags)

    return event
