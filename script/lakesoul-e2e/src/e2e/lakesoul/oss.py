# SPDX-FileCopyrightText: 2025 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0
import logging
from e2e.operators.python import PythonOperator
from e2e.utils import random_str, oss_delete_prefix


def oss_dispatcher(kind, prefix):
    if kind == "delete":
        oss_delete_prefix(prefix)
    else:
        logging.error(f"oss {kind} not implemented")


class OSSOperator(PythonOperator):
    def __init__(
        self,
        *,
        task_id: str | None = None,
        kind: str,  # put delete
        prefix: str,
        **kwargs,
    ) -> None:
        task_id = f"OSS-{random_str()}" if task_id is None else task_id
        op_args = [kind, prefix]
        super().__init__(
            task_id=task_id,
            python_callable=oss_dispatcher,
            op_args=op_args,
            **kwargs,
        )
