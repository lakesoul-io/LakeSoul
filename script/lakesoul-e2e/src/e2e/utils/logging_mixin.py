# SPDX-FileCopyrightText: 2025 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations


import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from logging import Logger
    from abc import _T


class LoggingMixin:
    """Convenience super-class to have a logger configured with the class name."""

    _log: logging.Logger | None = None

    # Parent logger used by this class. It should match one of the loggers defined in the
    # `logging_config_class`. By default, this attribute is used to create the final name of the logger, and
    # will prefix the `_logger_name` with a separating dot.
    _log_config_logger_name: str | None = None

    _logger_name: str | None = None

    def __init__(self, context=None):
        super().__init__()

    @staticmethod
    def _create_logger_name(
        logged_class: type[_T],
        log_config_logger_name: str | None = None,
        class_logger_name: str | None = None,
    ) -> str:
        """
        Generate a logger name for the given `logged_class`.

        By default, this function returns the `class_logger_name` as logger name. If it is not provided,
        the {class.__module__}.{class.__name__} is returned instead. When a `parent_logger_name` is provided,
        it will prefix the logger name with a separating dot.
        """
        logger_name: str = (
            class_logger_name
            if class_logger_name is not None
            else f"{logged_class.__module__}.{logged_class.__name__}"
        )

        if log_config_logger_name:
            return (
                f"{log_config_logger_name}.{logger_name}"
                if logger_name
                else log_config_logger_name
            )
        return logger_name

    @classmethod
    def _get_log(cls, obj: Any, clazz: type[_T]) -> Logger:
        if obj._log is None:
            logger_name: str = cls._create_logger_name(
                logged_class=clazz,
                log_config_logger_name=obj._log_config_logger_name,
                class_logger_name=obj._logger_name,
            )
            # obj._log = color_logger(logger_name)
            obj._log = logging.getLogger(logger_name)
        return obj._log

    @classmethod
    def logger(cls) -> Logger:
        """Return a logger."""
        return LoggingMixin._get_log(cls, cls)

    @property
    def log(self) -> Logger:
        """Return a logger."""
        return LoggingMixin._get_log(self, self.__class__)
