"""Structured logging.

Every log line is a JSON object when ``GRIDCARBON_LOG_JSON=1`` (the default in
containers and CI) and a coloured key=value line otherwise. ``run_id`` and
``partition`` are bound once per pipeline run so every downstream line can be
correlated without threading arguments through each function.
"""

from __future__ import annotations

import logging
import os
import sys
from typing import Any

import structlog


def configure_logging(level: str | None = None) -> None:
    level_name = (level or os.environ.get("GRIDCARBON_LOG_LEVEL", "INFO")).upper()
    json_output = os.environ.get("GRIDCARBON_LOG_JSON", "0") == "1"

    logging.basicConfig(format="%(message)s", stream=sys.stderr, level=level_name)

    renderer: Any
    if json_output:
        renderer = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer(colors=sys.stderr.isatty())

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.getLevelName(level_name)),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
        cache_logger_on_first_use=False,
    )


def get_logger(name: str) -> Any:
    return structlog.get_logger(name)


def bind_run_context(**kwargs: Any) -> None:
    structlog.contextvars.bind_contextvars(**kwargs)


def clear_run_context() -> None:
    structlog.contextvars.clear_contextvars()
