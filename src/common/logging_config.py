"""Structured JSON logging configuration."""

import logging
import sys


def setup_logging(service_name: str, level: int = logging.INFO) -> None:
    formatter = logging.Formatter(
        fmt='{"time":"%(asctime)s","service":"' + service_name + '","level":"%(levelname)s","logger":"%(name)s","message":"%(message)s"}',
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()
    root.addHandler(handler)
