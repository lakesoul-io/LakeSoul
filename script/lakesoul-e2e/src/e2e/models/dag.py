# SPDX-FileCopyrightText: 2025 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations
from abc import ABCMeta, abstractmethod
from collections import defaultdict, deque
import os
from pathlib import Path
import re
import signal
import sys
from typing import TYPE_CHECKING, Any, Iterable, List, Self, Sequence, Set
import zipfile


import attrs
from e2e.models.mixins import DependencyMixin
from e2e.utils.file import (
    get_unique_dag_module_name,
    list_py_file_paths,
    might_contain_dag,
)
from e2e.utils.logging_mixin import LoggingMixin

if TYPE_CHECKING:
    from e2e.operators.base import BaseOperator


KEY_REGEX = re.compile(r"^[\w.-]+$")
CAMELCASE_TO_SNAKE_CASE_REGEX = re.compile(r"(?!^)([A-Z]+)")

_DAG_HASH_ATTRS = frozenset(
    {
        "dag_id",
        "task_ids",
        "fileloc",
    }
)


def validate_key(k: str, max_length: int = 250):
    """Validate value used as a key."""
    if not isinstance(k, str):
        raise TypeError(f"The key has to be a string and is {type(k)}:{k}")
    if (length := len(k)) > max_length:
        raise ValueError(
            f"The key has to be less than {max_length} characters, not {length}"
        )
    if not KEY_REGEX.match(k):
        raise ValueError(
            f"The key {k!r} has to be made of alphanumeric characters, dashes, "
            f"dots, and underscores exclusively"
        )


class DAGNode(DependencyMixin, metaclass=ABCMeta):
    dag: DAG | None
    upstream_task_ids: set[str]
    downstream_task_ids: set[str]

    def __init__(self):
        self.upstream_task_ids = set()
        self.downstream_task_ids = set()
        super().__init__()

    def get_dag(self) -> DAG | None:
        return self.dag

    @property
    @abstractmethod
    def node_id(self) -> str:
        raise NotImplementedError()

    def has_dag(self) -> bool:
        return self.dag is not None

    @property
    def dag_id(self) -> str:
        """Returns dag id if it has one or an adhoc/meaningless ID."""
        if self.dag:
            return self.dag.dag_id
        return "_in_memory_dag_"

    @property
    @abstractmethod
    def roots(self) -> Sequence[DAGNode]:
        raise NotImplementedError()

    @property
    @abstractmethod
    def leaves(self) -> Sequence[DAGNode]:
        raise NotImplementedError()

    def _set_relatives(
        self,
        task_or_task_list: DependencyMixin | Sequence[DependencyMixin],
        upstream: bool = False,
    ) -> None:
        """Set relatives for the task or task list."""

        from e2e.operators.base import BaseOperator

        if not isinstance(task_or_task_list, Sequence):
            task_or_task_list = [task_or_task_list]

        task_list: List[BaseOperator] = []
        for task_object in task_or_task_list:
            task_object.update_relative(
                self,
                not upstream,
            )
            relatives = task_object.leaves if upstream else task_object.roots
            for task in relatives:
                if not isinstance(task, BaseOperator):
                    raise TypeError(
                        f"Relationships can only be set between Operators; received {task.__class__.__name__}"
                    )
                task_list.append(task)

        # relationships can only be set if the tasks share a single DAG. Tasks
        # without a DAG are assigned to that DAG.
        dags: set[DAG] = {
            task.dag
            for task in [*self.roots, *task_list]
            if task.has_dag() and task.dag
        }

        if len(dags) > 1:
            raise RuntimeError(
                f"Tried to set relationships between tasks in more than one DAG: {dags}"
            )
        if len(dags) == 1:
            dag = dags.pop()
        else:
            raise ValueError(
                "Tried to create relationships between tasks that don't have DAGs yet. "
                f"Set the DAG for at least one task and try again: {[self, *task_list]}"
            )

        if not self.has_dag():
            # If this task does not yet have a dag, add it to the same dag as the other task.
            self.dag = dag

        for task in task_list:
            if dag and not task.has_dag():
                # If the other task does not yet have a dag, add it to the same dag as this task and
                dag.add_task(task)  # type: ignore[arg-type]
            if upstream:
                task.downstream_task_ids.add(self.node_id)
                self.upstream_task_ids.add(task.node_id)
            else:
                self.downstream_task_ids.add(task.node_id)
                task.upstream_task_ids.add(self.node_id)

    def set_downstream(self, other: DependencyMixin | Sequence[DependencyMixin]):
        self._set_relatives(other, upstream=False)

    def set_upstream(self, other: DependencyMixin | Sequence[DependencyMixin]):
        self._set_relatives(other, upstream=True)

    @property
    def downstream_list(self) -> Iterable[BaseOperator]:
        """List of nodes directly downstream."""
        if not self.dag:
            raise RuntimeError(f"Operator {self} has not been assigned to a DAG yet")
        return [self.dag.get_task(tid) for tid in self.downstream_task_ids]

    @property
    def upstream_list(self) -> Iterable[BaseOperator]:
        """List of nodes directly upstream."""
        if not self.dag:
            raise RuntimeError(f"Operator {self} has not been assigned to a DAG yet")
        return [self.dag.get_task(tid) for tid in self.upstream_task_ids]

    def get_direct_relative_ids(self, upstream: bool = False) -> Set[str]:
        if upstream:
            return self.upstream_task_ids
        return self.downstream_task_ids


def _all_after_dag_id_to_kw_only(cls, fields: list[attrs.Attribute]):
    i = iter(fields)
    f = next(i)
    if f.name != "dag_id":
        raise RuntimeError("dag_id was not the first field")
    yield f

    for f in i:
        yield f.evolve(kw_only=True)


def _default_fileloc() -> str:
    # Skip over this frame, and the 'attrs generated init'
    back = sys._getframe().f_back
    if not back or not (back := back.f_back):
        # We expect two frames back, if not we don't know where we are
        return ""
    return back.f_code.co_filename if back else ""


@attrs.define(repr=False, field_transformer=_all_after_dag_id_to_kw_only, slots=False)  # type: ignore
class DAG:
    dag_id: str = attrs.field(kw_only=False, validator=lambda i, a, v: validate_key(v))
    fileloc: str = attrs.field(init=False, factory=_default_fileloc)
    relative_fileloc: str | None = attrs.field(init=False, default=None)
    task_dict: dict[str, BaseOperator] = attrs.field(factory=dict, init=False)

    def validate(self):
        return NotImplementedError()

    def __enter__(self) -> Self:
        from .contextmanager import DagContext

        DagContext.push(self)
        return self

    def __exit__(self, _type, _value, _tb):
        from .contextmanager import DagContext

        _ = DagContext.pop()

    def get_task(self, task_id: str) -> BaseOperator:
        if task_id in self.task_dict:
            return self.task_dict[task_id]
        raise RuntimeError(f"Task {task_id} not found")

    def set_dependency(self, upstream_task_id, downstream_task_id):
        """Set dependency between two tasks that already have been added to the DAG using add_task()."""
        self.get_task(upstream_task_id).set_downstream(
            self.get_task(downstream_task_id)
        )

    @property
    def tasks(self) -> List[BaseOperator]:
        return list(self.task_dict.values())

    @property
    def roots(self) -> List[BaseOperator]:
        """Return nodes with no parents. These are first to execute and are called roots or root nodes."""
        return [task for task in self.tasks if not task.upstream_list]

    @property
    def leaves(self) -> List[BaseOperator]:
        """Return nodes with no children. These are last to execute and are called leaves or leaf nodes."""
        return [task for task in self.tasks if not task.downstream_list]

    def check_cycle(self) -> None:
        """
        Check to see if there are any cycles in the DAG.

        :raises AirflowDagCycleException: If cycle is found in the DAG.
        """
        # default of int is 0 which corresponds to CYCLE_NEW
        CYCLE_NEW = 0
        CYCLE_IN_PROGRESS = 1
        CYCLE_DONE = 2

        visited: dict[str, int] = defaultdict(int)
        path_stack: deque[str] = deque()
        task_dict = self.task_dict

        def _check_adjacent_tasks(task_id, current_task):
            """Return first untraversed child task, else None if all tasks traversed."""
            for adjacent_task in current_task.get_direct_relative_ids():
                if visited[adjacent_task] == CYCLE_IN_PROGRESS:
                    msg = (
                        f"Cycle detected in DAG: {self.dag_id}. Faulty task: {task_id}"
                    )
                    raise RuntimeError(msg)
                if visited[adjacent_task] == CYCLE_NEW:
                    return adjacent_task
            return None

        for dag_task_id in self.task_dict.keys():
            if visited[dag_task_id] == CYCLE_DONE:
                continue
            path_stack.append(dag_task_id)
            while path_stack:
                current_task_id = path_stack[-1]
                if visited[current_task_id] == CYCLE_NEW:
                    visited[current_task_id] = CYCLE_IN_PROGRESS
                task = task_dict[current_task_id]
                child_to_check = _check_adjacent_tasks(current_task_id, task)
                if not child_to_check:
                    visited[current_task_id] = CYCLE_DONE
                    path_stack.pop()
                else:
                    path_stack.append(child_to_check)

    def add_task(self, task: BaseOperator):
        task_id = task.node_id
        self.task_dict[task_id] = task
        task.dag = self

    def __hash__(self):
        hash_components: list[Any] = [type(self)]
        for c in _DAG_HASH_ATTRS:
            # If it is a list, convert to tuple because lists can't be hashed
            if isinstance(getattr(self, c, None), list):
                val = tuple(getattr(self, c))
            else:
                val = getattr(self, c, None)
            try:
                hash(val)
                hash_components.append(val)
            except TypeError:
                hash_components.append(repr(val))
        return hash(tuple(hash_components))


class DagBag(LoggingMixin):
    def __init__(self, dag_folder: str | Path, collect_dags: bool = True) -> None:
        self.dag_folder = Path(dag_folder).resolve()
        self.dags: dict[str, DAG] = {}
        if collect_dags:
            self.collect_dags(None)

    def collect_dags(self, dag_folder: str | Path | None):
        dag_folder = dag_folder or self.dag_folder
        files_to_parse = list_py_file_paths(dag_folder)
        for filepath in files_to_parse:
            try:
                self.process_file(
                    filepath,
                )
            except Exception as e:
                self.log.exception(e)

    def process_file(self, filepath):
        from e2e.models.contextmanager import DagContext

        if filepath is None or not os.path.isfile(filepath):
            return
        DagContext.autoregistered_dags.clear()
        if filepath.endswith(".py") or not zipfile.is_zipfile(filepath):
            mods = self._load_modules_from_file(filepath)
        else:
            mods = self._load_modules_from_zip(filepath)
        self._process_modules(mods)

        return

    def _process_modules(self, mods):
        from e2e.models.contextmanager import DagContext

        top_level_dags = {
            (o, m)
            for m in mods
            for o in m.__dict__.values()
            if isinstance(o, (DAG, DAG))
        }

        top_level_dags.update(DagContext.autoregistered_dags)

        DagContext.current_autoregister_module_name = None
        DagContext.autoregistered_dags.clear()

        for dag, mod in top_level_dags:
            dag.fileloc = mod.__file__
            relative_fileloc = self._get_relative_fileloc(dag.fileloc)
            dag.relative_fileloc = relative_fileloc
            try:
                dag.validate()
                self.bag_dag(dag=dag)
            except Exception as _:
                self.log.exception("Failed to bag_dag: %s", dag.fileloc)
        return

    def bag_dag(self, dag: DAG):
        dag.check_cycle()  # throws exception if a task cycle is found

        prev_dag = self.dags.get(dag.dag_id)
        if prev_dag and prev_dag.fileloc != dag.fileloc:
            raise RuntimeError("dup")
        self.dags[dag.dag_id] = dag
        self.log.debug("Loaded DAG %s", dag)

    def _get_relative_fileloc(self, filepath: str) -> str:
        """
        Get the relative file location for a given filepath.

        :param filepath: Absolute path to the file
        :return: Relative path from bundle_path, or original filepath if no bundle_path
        """
        return filepath

    def _load_modules_from_file(self, filepath):
        from e2e.models.contextmanager import DagContext

        def handler(signum, frame):
            """Handle SIGSEGV signal and let the user know that the import failed."""
            msg = f"Received SIGSEGV signal while processing {filepath}."
            self.log.error(msg)

        try:
            signal.signal(signal.SIGSEGV, handler)
        except ValueError:
            self.log.warning(
                "SIGSEGV signal handler registration failed. Not in the main thread"
            )

        if not might_contain_dag(filepath, False):
            # Don't want to spam user with skip messages
            if not self.has_logged:
                self.has_logged = True
                self.log.info("File %s assumed to contain no DAGs. Skipping.", filepath)
            return []

        self.log.debug("Importing %s", filepath)
        mod_name = get_unique_dag_module_name(filepath)

        if mod_name in sys.modules:
            del sys.modules[mod_name]

        DagContext.current_autoregister_module_name = mod_name

        def parse(mod_name, filepath):
            try:
                import importlib.machinery

                loader = importlib.machinery.SourceFileLoader(mod_name, filepath)
                import importlib.util

                spec = importlib.util.spec_from_loader(mod_name, loader)
                new_module = importlib.util.module_from_spec(spec)  # type: ignore
                sys.modules[spec.name] = new_module  # type: ignore
                loader.exec_module(new_module)
                return [new_module]
            except KeyboardInterrupt:
                # re-raise ctrl-c
                raise
            except BaseException as _:
                # Normally you shouldn't catch BaseException, but in this case we want to, as, pytest.skip
                # raises an exception which does not inherit from Exception, and we want to catch that here.
                # This would also catch `exit()` in a dag file
                DagContext.autoregistered_dags.clear()
                self.log.exception("Failed to import: %s", filepath)
                return []

        return parse(mod_name, filepath)

    def _load_modules_from_zip(self, filepath):
        return NotImplementedError()

    @property
    def dag_ids(self) -> List[str]:
        """
        Get DAG ids.

        :return: a list of DAG IDs in this bag
        """
        return list(self.dags)
