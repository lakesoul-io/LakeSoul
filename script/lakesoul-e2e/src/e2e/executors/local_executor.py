# SPDX-FileCopyrightText: 2025 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0


from __future__ import annotations

import ctypes
import logging

from multiprocessing import Queue, SimpleQueue
import multiprocessing.sharedctypes
import os
from typing import TYPE_CHECKING

import psutil
from setproctitle import setproctitle

from .base_executor import BaseExecutor
from .workloads import ExecuteTask, TaskState


from enum import Enum


if TYPE_CHECKING:
    TaskStateType = tuple[ExecuteTask, Exception | None]


class ExecutorStatus(Enum):
    INIT = 0
    RUNNING = 1
    FINISHED = 2


def _run_worker(
    logger_name: str,
    input: SimpleQueue[ExecuteTask | None],
    output: Queue[TaskStateType],
    unread_messages: multiprocessing.sharedctypes.Synchronized[int],
):
    import signal

    # Ignore ctrl-c in this process -- we don't want to kill _this_ one. we let tasks run to completion
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    log = logging.getLogger(logger_name)
    log.info("Worker starting up pid=%d", os.getpid())

    while True:
        setproctitle("worker -- LocalExecutor: <idle>")
        try:
            workload = input.get()
        except EOFError:
            log.info(
                "Failed to read tasks from the task queue because the other "
                "end has closed the connection. Terminating worker %s.",
                multiprocessing.current_process().name,
            )
            break

        if workload is None:
            # Received poison pill, no more tasks to run
            return

        if not isinstance(workload, ExecuteTask):
            raise ValueError(
                f"LocalExecutor does not know how to handle {type(workload)}"
            )

        # Decrement this as soon as we pick up a message off the queue
        with unread_messages:
            unread_messages.value -= 1
        try:
            _execute_work(log, workload)

            output.put((workload, None))
        except Exception as e:
            log.exception("uhoh")
            workload.state = TaskState.FAILED
            output.put((workload, e))


def _execute_work(log: logging.Logger, workload: ExecuteTask) -> None:
    try:
        workload.exec()
        workload.state = TaskState.SUCCESS
    except Exception as e:
        workload.state = TaskState.FAILED
        log.exception(e)


class LocalExecutor(BaseExecutor):
    """
    LocalExecutor executes tasks locally in parallel.

    It uses the multiprocessing Python library and queues to parallelize the execution of tasks.

    :param parallelism: how many parallel processes are run in the executor
    """

    activity_queue: SimpleQueue[ExecuteTask | None]
    result_queue: SimpleQueue[TaskStateType]
    workers: dict[int, multiprocessing.Process]
    _unread_messages: multiprocessing.sharedctypes.Synchronized[int]
    _status: ExecutorStatus
    _task_state_map: dict[str, TaskState]

    def __init__(
        self,
        taks_state_map,
        back_num: int = 0,
        parallelism: int = 4,
    ):
        super().__init__(back_num=back_num, parallelism=parallelism)
        if self.parallelism < 0:
            raise ValueError("parallelism must be greater than or equal to 0")

        self._status = ExecutorStatus.INIT
        self._task_state_map = taks_state_map

    def start(self) -> None:
        """Start the executor."""
        # We delay opening these queues until the start method mostly for unit tests. ExecutorLoader caches
        # instances, so each test reusues the same instance! (i.e. test 1 runs, closes the queues, then test 2
        # comes back and gets the same LocalExecutor instance, so we have to open new here.)
        self.activity_queue = SimpleQueue()
        self.result_queue = SimpleQueue()
        self.workers = {}

        # Mypy sees this value as `SynchronizedBase[c_uint]`, but that isn't the right runtime type behaviour
        # (it looks like an int to python)
        self._unread_messages = multiprocessing.Value(ctypes.c_uint)
        self._status = ExecutorStatus.RUNNING

    def _check_workers(self):
        # Reap any dead workers
        to_remove = set()
        for pid, proc in self.workers.items():
            if not proc.is_alive():
                to_remove.add(pid)
                proc.close()

        if to_remove:
            self.workers = {
                pid: proc for pid, proc in self.workers.items() if pid not in to_remove
            }

        with self._unread_messages:
            num_outstanding = self._unread_messages.value

        if num_outstanding <= 0 or self.activity_queue.empty():
            # Nothing to do. Future enhancement if someone wants: shut down workers that have been idle for N
            # seconds
            return

        # If we're using spawn in multiprocessing (default on macOS now) to start tasks, this can get called a
        # via `sync()` a few times before the spawned process actually starts picking up messages. Try not to
        # create too much
        need_more_workers = (len(self.workers) - self.back_num) < num_outstanding
        if need_more_workers and (
            self.parallelism == 0 or len(self.workers) < self.parallelism
        ):
            # This only creates one worker, which is fine as we call this directly after putting a message on
            # activity_queue in execute_async
            self._spawn_worker()

    def _spawn_worker(self):
        p = multiprocessing.Process(
            target=_run_worker,
            kwargs={
                "logger_name": self.log.name,
                "input": self.activity_queue,
                "output": self.result_queue,
                "unread_messages": self._unread_messages,
            },
        )
        p.start()
        if TYPE_CHECKING:
            assert p.pid  # Since we've called start
        self.workers[p.pid] = p

    def sync(self) -> None:
        """Sync will get called periodically by the heartbeat method."""
        self._read_results()
        self._check_workers()

    def _read_results(self):
        while not self.result_queue.empty():
            task, e = self.result_queue.get()
            self._task_state_map[task.task_id] = task.state
            if task.state == TaskState.FAILED:
                self.log.error(e)

    def end(self) -> None:
        """End the executor."""
        self.log.info(
            "Shutting down LocalExecutor"
            "; waiting for running tasks to finish.  Signal again if you don't want to wait."
        )

        for proc in self.workers.values():
            self.kill_proc_tree(proc.pid)

        # Process any extra results before closing
        self._read_results()

        self.activity_queue.close()
        self.result_queue.close()

    def kill_proc_tree(self, pid, including_parent=True):
        try:
            parent = psutil.Process(pid)
        except psutil.NoSuchProcess:
            return
        children = parent.children(recursive=True)
        # 先杀子进程
        for child in children:
            try:
                child.kill()
            except psutil.NoSuchProcess:
                pass
        gone, alive = psutil.wait_procs(children)
        if including_parent:
            try:
                parent.kill()
                parent.wait()
            except psutil.NoSuchProcess:
                pass

    def terminate(self):
        """Terminate the executor is not doing anything."""

    def queue_workload(
        self,
        workload: ExecuteTask,
    ):
        workload.state = TaskState.QUEUED
        self._task_state_map[workload.task_id] = workload.state
        if workload.is_back():
            self.back_num += 1
        self.activity_queue.put(workload)
        with self._unread_messages:
            self._unread_messages.value += 1
        self._check_workers()

    @property
    def status(self) -> ExecutorStatus:
        return self._status
