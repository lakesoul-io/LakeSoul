# SPDX-FileCopyrightText: 2025 LakeSoul Contributors
#
# SPDX-License-Identifier: Apache-2.0

import os
from collections import namedtuple
import contextlib
import signal
from subprocess import Popen, PIPE, STDOUT
from tempfile import TemporaryDirectory, gettempdir
from collections.abc import Iterator

from e2e.hooks.base import BaseHook

SubprocessResult = namedtuple("SubprocessResult", ["exit_code", "output"])


@contextlib.contextmanager
def working_directory(cwd: str | None = None) -> Iterator[str]:
    with contextlib.ExitStack() as stk:
        if cwd is None:
            cwd = stk.enter_context(TemporaryDirectory(prefix="flow"))
        yield cwd


class SubprocessHook(BaseHook):
    """Hook for running processes with the ``subprocess`` module."""

    def __init__(self, **kwargs) -> None:
        self.sub_process: Popen[bytes] | None = None
        super().__init__(**kwargs)

    def run_command(
        self,
        command: list[str],
        env: dict[str, str] | None = None,
        output_encoding: str = "utf-8",
        cwd: str | None = None,
    ) -> SubprocessResult:
        """
        Execute the command.

        If ``cwd`` is None, execute the command in a temporary directory which will be cleaned afterwards.
        If ``env`` is not supplied, ``os.environ`` is passed

        :param command: the command to run
        :param env: Optional dict containing environment variables to be made available to the shell
            environment in which ``command`` will be executed.  If omitted, ``os.environ`` will be used.
            Note, that in case you have Sentry configured, original variables from the environment
            will also be passed to the subprocess with ``SUBPROCESS_`` prefix. See:
            https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/logging-monitoring/errors.html for details.
        :param output_encoding: encoding to use for decoding stdout
        :param cwd: Working directory to run the command in.
            If None (default), the command is run in a temporary directory.
        :return: :class:`namedtuple` containing ``exit_code`` and ``output``, the last line from stderr
            or stdout
        """
        self.log.info("Tmp dir root location: %s", gettempdir())
        with working_directory(cwd=cwd) as cwd:

            def pre_exec():
                # Restore default signal disposition and invoke setsid
                for sig in ("SIGPIPE", "SIGXFZ", "SIGXFSZ"):
                    if hasattr(signal, sig):
                        signal.signal(getattr(signal, sig), signal.SIG_DFL)
                os.setsid()

            self.log.info("Running command: %s", command)

            self.sub_process = Popen(
                command,
                stdout=PIPE,
                stderr=STDOUT,
                cwd=cwd,
                env=env if env or env == {} else os.environ,
                preexec_fn=pre_exec,
            )

            self.log.info("Output:")
            line = ""
            if self.sub_process is None:
                raise RuntimeError("The subprocess should be created here and is None!")
            if self.sub_process.stdout is not None:
                for raw_line in iter(self.sub_process.stdout.readline, b""):
                    line = raw_line.decode(
                        output_encoding, errors="backslashreplace"
                    ).rstrip()
                    self.log.info("%s", line)

            self.sub_process.wait()

            self.log.info(
                "Command exited with return code %s", self.sub_process.returncode
            )
            return_code: int = self.sub_process.returncode

        return SubprocessResult(exit_code=return_code, output=line)

    def send_sigterm(self):
        """Send SIGTERM signal to ``self.sub_process`` if one exists."""
        self.log.info("Sending SIGTERM signal to process group")
        if self.sub_process and hasattr(self.sub_process, "pid"):
            os.killpg(os.getpgid(self.sub_process.pid), signal.SIGTERM)
