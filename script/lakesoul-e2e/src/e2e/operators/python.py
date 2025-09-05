# SPDX-FileCopyrightText: 2025 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations
from typing import Any, Callable, Collection, Mapping

from e2e.operators.base import BaseOperator
from e2e.utils.operator_helper import KeywordParameters
from e2e.utils import random_str


class PythonOperator(BaseOperator):
    def __init__(
        self,
        *,
        task_id: str | None = None,
        python_callable: Callable,
        op_args: Collection[Any] | None = None,
        op_kwargs: Mapping[str, Any] | None = None,
        **kwargs,
    ) -> None:
        if not task_id:
            task_id = f"PythonOperator-{random_str()}"
        super().__init__(task_id=task_id, **kwargs)

        if not callable(python_callable):
            raise RuntimeError("xxxx")

        self.python_callable = python_callable
        self.op_args = op_args or ()
        self.op_kwargs = op_kwargs or {}

    def execute(self) -> Any:
        self.op_kwargs = self.determine_kwargs()
        return_value = self.execute_callable()
        return return_value

    def execute_callable(self) -> Any:
        return self.python_callable(*self.op_args, **self.op_kwargs)

    def determine_kwargs(self) -> Mapping[str, Any]:
        return KeywordParameters.determine(
            self.python_callable, self.op_args, self.op_kwargs
        ).unpacking()
