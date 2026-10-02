"""
Logging setup shared by the API, the poller and the worker.

Two output formats, chosen by the LOG_FORMAT environment variable:

  text (default)  human-readable, for your terminal:
      2026-10-02 17:04:37,623 INFO poller: fetched=43 accepted=40 ...

  json            one JSON object per line, for running in Docker:
      {"ts": "2026-10-02T11:34:37.623Z", "level": "INFO", "service": "poller", "msg": "fetched=43 ..."}

WHY JSON: a log aggregator (Datadog, Google Cloud Logging, Grafana Loki) can
index each field, so you can search `service=worker AND level=ERROR` instead of
grepping text. Same information, but machine-searchable.
"""

import json
import logging
import os
import sys
import time


class JsonFormatter(logging.Formatter):
    def __init__(self, service: str):
        super().__init__()
        self.service = service

    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created)) + f".{int(record.msecs):03d}Z",
            "level": record.levelname,
            "service": self.service,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info:
            entry["exc"] = self.formatException(record.exc_info)
        return json.dumps(entry, ensure_ascii=False)


def setup_logging(service: str) -> None:
    handler = logging.StreamHandler(sys.stdout)
    if os.environ.get("LOG_FORMAT", "text").lower() == "json":
        handler.setFormatter(JsonFormatter(service))
    else:
        handler.setFormatter(logging.Formatter(f"%(asctime)s %(levelname)s {service}: %(message)s"))
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(logging.INFO)
    # The HTTP libraries log every request at INFO. Which logger name depends on
    # the SDK version (newer ones use "httpx2"), so silence them all.
    for noisy in ("httpx", "httpx2", "httpcore"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
