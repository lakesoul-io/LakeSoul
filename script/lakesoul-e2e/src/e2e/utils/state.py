# SPDX-FileCopyrightText: 2025 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

from enum import Enum


class JobState(str, Enum):
    """All possible states that a Job can be in."""

    RUNNING = "running"
    SUCCESS = "success"
    RESTARTING = "restarting"
    FAILED = "failed"

    def __str__(self) -> str:
        return self.value


class TerminalTIState(str, Enum):
    """States that a Task Instance can be in that indicate it has reached a terminal state."""

    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"  # A user can raise a AirflowSkipException from a task & it will be marked as skipped
    UPSTREAM_FAILED = "upstream_failed"
    REMOVED = "removed"

    def __str__(self) -> str:
        return self.value


class IntermediateTIState(str, Enum):
    """States that a Task Instance can be in that indicate it is not yet in a terminal or running state."""

    SCHEDULED = "scheduled"
    QUEUED = "queued"
    RESTARTING = "restarting"
    UP_FOR_RETRY = "up_for_retry"
    UP_FOR_RESCHEDULE = "up_for_reschedule"
    DEFERRED = "deferred"

    def __str__(self) -> str:
        return self.value


class TaskInstanceState(str, Enum):
    """
    All possible states that a Task Instance can be in.

    Note that None is also allowed, so always use this in a type hint with Optional.
    """

    # The scheduler sets a TaskInstance state to None when it's created but not
    # yet run, but we don't list it here since TaskInstance is a string enum.
    # Use None instead if need this state.

    # Set by the scheduler
    REMOVED = TerminalTIState.REMOVED  # Task vanished from DAG before it ran
    SCHEDULED = (
        IntermediateTIState.SCHEDULED
    )  # Task should run and will be handed to executor soon

    # Set by the task instance itself
    QUEUED = IntermediateTIState.QUEUED  # Executor has enqueued the task
    RUNNING = "running"  # Task is executing
    SUCCESS = TerminalTIState.SUCCESS  # Task completed
    RESTARTING = (
        IntermediateTIState.RESTARTING
    )  # External request to restart (e.g. cleared when running)
    FAILED = TerminalTIState.FAILED  # Task errored out
    UP_FOR_RETRY = IntermediateTIState.UP_FOR_RETRY  # Task failed but has retries left
    UP_FOR_RESCHEDULE = (
        IntermediateTIState.UP_FOR_RESCHEDULE
    )  # A waiting `reschedule` sensor
    UPSTREAM_FAILED = (
        TerminalTIState.UPSTREAM_FAILED
    )  # One or more upstream deps failed
    SKIPPED = TerminalTIState.SKIPPED  # Skipped by branching or some other mechanism
    DEFERRED = IntermediateTIState.DEFERRED  # Deferrable operator waiting on a trigger

    def __str__(self) -> str:
        return self.value
