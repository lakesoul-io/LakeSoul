# SPDX-FileCopyrightText: 2025 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

from e2e.utils.logging_mixin import LoggingMixin


class BaseHook(LoggingMixin):
    def __init__(self, logger_name: str | None = None):
        super().__init__()
        self._log_config_logger_name = "flow.hooks"
        self._logger_name = logger_name
