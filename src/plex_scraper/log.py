"""Minimal structured JSON logging. No secrets ever pass through here."""
from __future__ import annotations

import json
import logging
import sys
import time


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created))
            + f".{int(record.msecs):03d}Z",
            "level": record.levelname.lower(),
            "event": record.getMessage(),
        }
        extra = getattr(record, "fields", None)
        if extra:
            for key, value in extra.items():
                payload[key] = value
        if record.exc_info and record.exc_info[0] is not None:
            payload["exc"] = record.exc_info[0].__name__
        return json.dumps(payload, default=str)


def setup_logging(level: str = "info") -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(getattr(logging, level.upper(), logging.INFO))


def event(event_name: str, **fields) -> None:
    """Emit one structured event line."""
    logging.getLogger("events").info(event_name, extra={"fields": fields})
