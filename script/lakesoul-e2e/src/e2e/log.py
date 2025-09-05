# SPDX-FileCopyrightText: 2025 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0
from datetime import datetime, timedelta, timezone
import logging
import colorlog


def str2level(level: str):
    match level:
        case "info":
            return logging.INFO
        case "error":
            return logging.ERROR
        case "warn":
            return logging.WARNING
        case _:
            return logging.DEBUG


class LogFormatter(colorlog.ColoredFormatter):
    def formatTime(self, record, datefmt=None):
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
                "INFO": "light_white",
                "WARNING": "yellow",
                "ERROR": "red",
            },
            secondary_log_colors={},
        )
    )
    root_logger.addHandler(handler)
