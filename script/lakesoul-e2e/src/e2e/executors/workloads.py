# SPDX-FileCopyrightText: 2025 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations


from enum import Enum

from e2e.operators.base import BaseOperator


class TaskState(Enum):
    SUCCESS = 0
    FAILED = 1
    INIT = 2
    QUEUED = 3


class ExecuteTask:
    """Execute the given Task."""

    def __init__(self, operator: BaseOperator) -> None:
        self.operator = operator
        self._state = TaskState.INIT

    @property
    def task_id(self) -> str:
        return self.operator.task_id

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, state):
        self._state = state

    def exec(self):
        self.operator.execute()

    def is_back(self) -> bool:
        return self.operator.is_back
