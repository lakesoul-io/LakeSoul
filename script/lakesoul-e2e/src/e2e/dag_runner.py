# SPDX-FileCopyrightText: 2025 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations
from collections import defaultdict, deque

from e2e.executors import LocalExecutor, ExecuteTask
from e2e.executors.workloads import TaskState
from e2e.models.dag import DagBag

import time

from e2e.utils.logging_mixin import LoggingMixin


class DAGRunner(LoggingMixin):
    ready_queue: deque[ExecuteTask]

    def __init__(
        self,
        parallelism: int,
        dagbag: DagBag,
        back_num: int = 2,
    ):
        self.dagbag = dagbag

        self.ready_queue = deque()  # no prioiry
        self.task_state_map = defaultdict(lambda: TaskState.INIT)
        self.executor = LocalExecutor(
            self.task_state_map, back_num=back_num, parallelism=parallelism
        )
        for _, v in self.dagbag.dags.items():
            for op in v.roots:
                task = ExecuteTask(op)
                self.task_state_map[task.task_id] = task.state
                self.ready_queue.appendleft(task)

    def run(self):
        try:
            self.executor.start()
            self._run_loop()
        except Exception:
            self.log.exception("exception")
            raise
        finally:
            self.terminate()

    def terminate(self):
        try:
            self.executor.end()
        except Exception:
            self.log.exception("in end")

    def _check_task(self):
        new_queue = deque()
        for t in self.ready_queue:
            if self.task_state_map[t.task_id] == TaskState.FAILED:
                raise RuntimeError(f"{t.task_id} failed")
            elif self.task_state_map[t.task_id] == TaskState.SUCCESS:
                for down in t.operator.downstream_list:
                    ids = down.upstream_task_ids
                    if all(self.task_state_map[id] == TaskState.SUCCESS for id in ids):
                        new_t = ExecuteTask(down)
                        self.task_state_map[new_t.task_id] = new_t.state
                        new_queue.append(new_t)

            else:
                new_queue.append(t)

        self.ready_queue = new_queue

    def _run_loop(self):
        self.log.info("let's start")
        while True:
            for t in self.ready_queue:
                if t.state == TaskState.INIT:
                    self.executor.queue_workload(t)
            self.executor.heartbeat()
            self._check_task()
            if len(self.ready_queue) == 0:
                break
            time.sleep(1)
