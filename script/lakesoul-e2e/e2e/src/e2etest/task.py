# SPDX-FileCopyrightText: LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

from typing import List, Optional
from e2etest.s3 import s3_list_prefix_dir
from e2etest.vars import (
    E2E_DATA_DIR,
)
import time


class SubTask:
    """Subprocess Wrapper"""

    def run(self, **conf):
        pass


class CheckParquetSubTask(SubTask):
    """Check whether the data file is generated successfully"""

    def __init__(self, timeout):
        self.timeout = timeout

    def run(self, **conf):
        start = time.time()
        while True:
            all_objects = s3_list_prefix_dir(E2E_DATA_DIR)
            if len(all_objects) == 1:
                return
            if time.time() - start > self.timeout:
                break
        raise RuntimeError("data init failed")


class Task:
    """Task combine a sink task with a source task"""

    def __init__(self, sink: Optional[SubTask], source: Optional[SubTask]) -> None:
        self.sink = sink
        self.source = source

    def run(self):
        if self.sink:
            self.sink.run()
        if self.source:
            self.source.run()


class TaskRunner:
    """Run tasks in queue"""

    def __init__(self, tasks: List[Task]) -> None:
        self.tasks = tasks

    def run(self):
        for t in self.tasks:
            t.run()
