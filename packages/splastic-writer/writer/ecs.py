"""Apply classify results onto documents for Elasticsearch."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from writer.classify import ClassifiedEvent, data_stream_name


@dataclass(frozen=True)
class Routing:
    target_stream: str


def apply_classify(
    event: dict[str, Any],
    classified: ClassifiedEvent,
    *,
    namespace: str,
    fallback: bool = False,
) -> tuple[dict[str, Any], Routing]:
    """Mutate a copy of ``event`` with ECS / splunk fields; return routing."""
    doc = dict(event)
    stream = data_stream_name(classified.dataset, namespace)

    doc["event"] = {
        **(doc.get("event") if isinstance(doc.get("event"), dict) else {}),
        "kind": classified.kind.value,
        "dataset": classified.dataset,
    }
    doc["data_stream"] = {
        "type": "logs",
        "dataset": classified.dataset,
        "namespace": namespace,
    }
    doc["splunk"] = {
        **(doc.get("splunk") if isinstance(doc.get("splunk"), dict) else {}),
        "classify_reason": classified.reason,
        "index": str(doc.get("splunk_index") or ""),
    }

    tags = list(doc.get("tags") or [])
    if fallback and "_classify_failed" not in tags:
        tags.append("_classify_failed")
    doc["tags"] = tags

    return doc, Routing(target_stream=stream)
