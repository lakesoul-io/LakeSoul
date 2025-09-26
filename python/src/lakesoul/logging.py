from __future__ import annotations
from datetime import datetime, timedelta, timezone
import logging
import colorlog
from typing_extensions import override


def str2level(level: str) -> int:
    if level == "info":
        return logging.INFO
    elif level == "error":
        return logging.ERROR
    elif level == "warn":
        return logging.WARNING
    elif level == "debug":
        return logging.DEBUG
    else:
        # in pyo3 log use 5 for tracing level in rust
        return 5


class LogFormatter(colorlog.ColoredFormatter):
    @override
    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None):
        beijing_tz = timezone(timedelta(hours=8))
        ct = datetime.fromtimestamp(record.created, tz=beijing_tz)
        if datefmt:
            s = ct.strftime(datefmt)
        else:
            s = ct.strftime("%Y-%m-%d %H:%M:%S")
        return s


def init_logger(level: str):
    root_logger = logging.getLogger()
    root_logger.setLevel(str2level(level))
    handler = colorlog.StreamHandler()
    handler.setFormatter(
        LogFormatter(
            "[%(asctime)s] %(bold)s [%(filename)s:%(lineno)d] %(log_color)s %(levelname)s %(reset)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            log_colors={
                "DEBUG": "light_black",
                "INFO": "green",
                "WARNING": "yellow",
                "ERROR": "red",
            },
            secondary_log_colors={},
        )
    )
    root_logger.addHandler(handler)
