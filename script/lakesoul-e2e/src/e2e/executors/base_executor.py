# SPDX-FileCopyrightText: 2025 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations
from typing import Sequence


from e2e.utils.logging_mixin import LoggingMixin

from e2e.executors.workloads import ExecuteTask

PARALLELISM: int = 4


class BaseExecutor(LoggingMixin):
    name: None | str = None
    back_num: int

    def __init__(self, back_num, parallelism: int = PARALLELISM):
        super().__init__()
        self.parallelism: int = parallelism
        self.queued_tasks: dict[str, ExecuteTask] = {}
        self.running: set[str] = set()
        self.back_num = back_num

        if self.parallelism <= 0:
            raise ValueError("parallelism is set to 0 or lower")

    def __repr__(self):
        return f"{self.__class__.__name__}(parallelism={self.parallelism})"

    def start(self):  # pragma: no cover
        """Executors may need to get things started."""

    def queue_workload(self, workload: ExecuteTask) -> None:
        if not isinstance(workload, ExecuteTask):
            raise ValueError(
                f"Un-handled workload kind {type(workload).__name__!r} in {type(self).__name__}"
            )
        self.queued_tasks[workload.task_id] = workload

    def _process_workloads(self, workloads: Sequence[ExecuteTask]) -> None:
        """
        Process the given workloads.

        This method must be implemented by subclasses to define how they handle
        the execution of workloads (e.g., queuing them to workers, submitting to
        external systems, etc.).

        :param workloads: List of workloads to process
        """
        raise NotImplementedError(
            f"{type(self).__name__} must implement _process_workloads()"
        )

    def has_task(self, exec_id: str) -> bool:
        """
        Check if a task is either queued or running in this executor.

        :param task_instance: TaskInstance
        :return: True if the task is known to this executor
        """
        return (
            exec_id in self.queued_tasks
            or exec_id in self.running
            or exec_id in self.queued_tasks
            or exec_id in self.running
        )

    def end(self) -> None:  # pragma: no cover
        """Wait synchronously for the previously submitted job to complete."""
        raise NotImplementedError

    def terminate(self):
        """Get called when the daemon receives a SIGTERM."""
        raise NotImplementedError

    @property
    def slots_available(self):
        """Number of new tasks this executor instance can accept."""
        return self.parallelism - len(self.running) - len(self.queued_tasks)

    @property
    def slots_occupied(self):
        """Number of tasks this executor instance is currently managing."""
        return len(self.running) + len(self.queued_tasks)

    def heartbeat(self) -> None:
        num_running_tasks = len(self.running)

        open_slots = self.parallelism - num_running_tasks

        self.trigger_tasks(open_slots)

        self.sync()

    def trigger_tasks(self, open_slots: int):
        """
        Initiate async execution of the queued tasks, up to the number of available slots.

        :param open_slots: Number of open slots
        """
        sorted_queue = list(self.queued_tasks.items())  # TODO add priority here
        workload_list = []

        for _ in range(min((open_slots, len(self.queued_tasks)))):
            _key, item = sorted_queue.pop(0)

            workload_list.append(item)
        if workload_list:
            self._process_workloads(workload_list)

    def sync(self) -> None:
        """
        Sync will get called periodically by the heartbeat method.

        Executors should override this to perform gather statuses.
        """
