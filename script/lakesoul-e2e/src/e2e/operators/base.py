# SPDX-FileCopyrightText: 2025 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import abc
from dataclasses import dataclass, field
from functools import wraps
import inspect
from types import FunctionType, MethodType
from typing import Any, List, Sequence, TypeVar, cast

from e2e.models.dag import DAG, DAGNode, validate_key
from e2e.models.mixins import DependencyMixin


__all__ = []

T = TypeVar("T", bound=FunctionType | MethodType)


class AbstractOperator(DAGNode):
    operator_class: type[BaseOperator] | dict[str, Any]
    priority_weight: int
    task_id: str
    is_setup: bool = False
    is_teardown: bool = False

    @property
    def dag_id(self) -> str:
        """Returns dag id if it has one or an adhoc + owner."""
        dag = self.get_dag()
        if dag:
            return dag.dag_id
        return "adhoc"

    @property
    def node_id(self) -> str:
        return self.task_id


class BaseOperatorMeta(abc.ABCMeta):
    @classmethod
    def _apply_defaults(cls, func: T) -> T:
        sig_cache = inspect.signature(func)
        non_variadic_params = {
            name: param
            for (name, param) in sig_cache.parameters.items()
            if param.name != "self"
            and param.kind not in (param.VAR_POSITIONAL, param.VAR_KEYWORD)
        }
        non_optional_args = {
            name
            for name, param in non_variadic_params.items()
            if param.default == param.empty and name != "task_id"
        }

        @wraps(func)
        def apply_defaults(self: BaseOperator, *args: Any, **kwargs: Any) -> Any:
            from e2e.models.contextmanager import DagContext

            if args:
                raise TypeError("Use keyword arguments when initializing operators")

            instantiated_from_mapped = kwargs.pop(
                "_airflow_from_mapped",
                getattr(self, "_BaseOperator__from_mapped", False),
            )

            dag: DAG | None = kwargs.get("dag")
            if dag is None:
                dag = DagContext.get_current()
                if dag is not None:
                    kwargs["dag"] = dag

            missing_args = non_optional_args.difference(kwargs)
            if len(missing_args) == 1:
                raise TypeError(f"missing keyword argument {missing_args.pop()!r}")
            if missing_args:
                display = ", ".join(repr(a) for a in sorted(missing_args))
                raise TypeError(f"missing keyword arguments {display}")

            if not hasattr(self, "_BaseOperator__init_kwargs"):
                object.__setattr__(self, "_BaseOperator__init_kwargs", {})
            object.__setattr__(
                self, "_BaseOperator__from_mapped", instantiated_from_mapped
            )

            result = func(
                self,
                **kwargs,
            )

            # Store the args passed to init -- we need them to support task.map serialization!
            self._BaseOperator__init_kwargs.update(kwargs)  # type: ignore

            # Set upstream task defined by XComArgs passed to template fields of the operator.
            # BUT: only do this _ONCE_, not once for each class in the hierarchy
            return result

        apply_defaults.__non_optional_args = non_optional_args  # type: ignore
        apply_defaults.__param_names = set(non_variadic_params)  # type: ignore

        return cast("T", apply_defaults)

    def __new__(cls, name, bases, namespace, **kwargs):
        new_cls = super().__new__(cls, name, bases, namespace, **kwargs)
        # We patch `__init__` only if the class defines it.
        first_superclass = new_cls.mro()[1]
        if new_cls.__init__ is not first_superclass.__init__:
            new_cls.__init__ = cls._apply_defaults(new_cls.__init__)
        return new_cls


@dataclass(repr=False)
class BaseOperator(AbstractOperator, metaclass=BaseOperatorMeta):
    _dag: DAG | None = field(init=False, default=None)

    def __setattr__(self, key: str, value: Any) -> None:
        if converter := getattr(self, f"_convert_{key}", None):
            value = converter(value)
        return super().__setattr__(key, value)

    def __init__(
        self, *, task_id: str, dag: DAG | None = None, is_back=False, **kwargs
    ):
        super().__init__()
        self.task_id = task_id
        self.is_back = is_back
        validate_key(self.task_id)
        if dag is not None:
            # overrite DAGNODE's dag
            self.dag = dag  # type: ignore

    @property
    def dag(self) -> DAG:
        if dag := self._dag:
            return dag
        raise RuntimeError("no dag")

    @dag.setter
    def dag(self, dag: DAG | None) -> None:
        """Operators can be assigned to one DAG, one time. Repeat assignments to that same DAG are ok."""
        self._dag = dag

    def pre_execute(
        self,
    ):
        """Execute right before self.execute() is called."""

    def execute(
        self,
    ) -> Any:
        """
        Derive when creating an operator.

        The main method to execute the task. Context is the same dictionary used
        as when rendering jinja templates.

        Refer to get_template_context for more context.
        """
        raise NotImplementedError()

    def post_execute(self, result: Any = None):
        """
        Execute right after self.execute() is called.

        It is passed the execution context and any results returned by the operator.
        """

    @property
    def roots(self) -> List[BaseOperator]:
        return [self]

    @property
    def leaves(self) -> List[BaseOperator]:
        return [self]

    def _convert__dag(self, dag: DAG | None) -> DAG | None:
        # Called automatically by __setattr__ method

        if dag is None:
            return dag

        if not isinstance(dag, DAG):
            raise TypeError(f"Expected DAG; received {dag.__class__.__name__}")
        if self._dag is not None and self._dag is not dag:
            raise ValueError(f"The DAG assigned to {self} can not be changed.")

        elif dag.task_dict.get(self.task_id) is not self:
            dag.add_task(self)
        return dag


def chain(*tasks: DependencyMixin | Sequence[DependencyMixin]) -> None:
    pass


def cross_downstream(
    from_tasks: Sequence[DependencyMixin],
    to_tasks: DependencyMixin | Sequence[DependencyMixin],
):
    pass


def chain_linear(*elements: DependencyMixin | Sequence[DependencyMixin]):
    pass
