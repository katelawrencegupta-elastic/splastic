"""Uncooked Splunk TCP acceptor (plain lines / JSON objects)."""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Callable

import orjson

logger = logging.getLogger("writer.uncooked")

IngestFn = Callable[[dict[str, Any]], Awaitable[None]]


def parse_uncooked_line(line: str) -> dict[str, Any]:
    text = line.strip()
    if not text:
        return {}
    if text.startswith("{"):
        try:
            obj = orjson.loads(text)
            if isinstance(obj, dict):
                tags = list(obj.get("tags") or [])
                if "splunk_tcp_39997" not in tags:
                    tags.append("splunk_tcp_39997")
                obj["tags"] = tags
                return obj
        except orjson.JSONDecodeError:
            pass
    return {
        "message": text,
        "sourcetype": "",
        "source": "",
        "host": "",
        "splunk_index": "",
        "tags": ["splunk_tcp_39997"],
    }


async def handle_uncooked_client(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    ingest: IngestFn,
) -> None:
    peer = writer.get_extra_info("peername")
    logger.info("uncooked connection from %s", peer)
    try:
        while True:
            raw = await reader.readline()
            if not raw:
                break
            try:
                line = raw.decode("utf-8", errors="replace")
            except Exception:
                continue
            event = parse_uncooked_line(line)
            if not event:
                continue
            await ingest(event)
    except Exception as exc:
        logger.exception("uncooked handler error from %s: %s", peer, exc)
    finally:
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
        logger.info("uncooked connection closed %s", peer)
