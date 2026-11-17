# SPDX-FileCopyrightText: 2026 LakeSoul Contributors
# SPDX-License-Identifier: Apache-2.0
"""Supervisor safety tests: no LakeSoul import, database, HDF5 read or benchmark."""

import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

SOURCE = (
    Path(__file__).resolve().parents[1] / "examples/vector-search/run_e2e_benchmark.py"
)
if not SOURCE.exists():
    raise unittest.SkipTest(
        "run_e2e_benchmark.py is not shipped in the repository; "
        "the supervisor script is exercised from its development checkout"
    )
spec = importlib.util.spec_from_file_location("vector_benchmark_runner", SOURCE)
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


class RunnerTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.args = runner.arguments(
            [
                "--data-dir",
                str(self.root),
                "--scratch-root",
                str(self.root),
                "--output-dir",
                str(self.root / "output"),
            ]
        )
        self.token = "vbe2e_" + "a" * 32
        self.group = self.root / self.token
        self.group.mkdir()
        (self.group / ".owner").write_text(self.token)

    def test_containment_requires_exact_owned_child(self):
        self.assertEqual(
            runner.verified_group(self.root, self.group, self.token), self.group
        )
        for unsafe in (
            self.root,
            self.root.parent,
            self.group / "table",
            self.root / "other",
        ):
            with self.subTest(path=unsafe), self.assertRaises(RuntimeError):
                runner.verified_group(self.root, unsafe, self.token)
        (self.group / ".owner").write_text("different run")
        with self.assertRaises(RuntimeError):
            runner.verified_group(self.root, self.group, self.token)

    def test_containment_rejects_symlink(self):
        outside = self.root / "outside"
        outside.mkdir()
        link = self.root / ("vbe2e_" + "b" * 32)
        link.symlink_to(outside, target_is_directory=True)
        with self.assertRaises(RuntimeError):
            runner.verified_group(self.root, link, link.name)
        marker = self.group / ".owner"
        marker.unlink()
        marker.symlink_to(outside / "owner")
        with self.assertRaises(RuntimeError):
            runner.verified_group(self.root, self.group, self.token)

    def test_command_explicitly_overrides_expensive_bench_defaults(self):
        cmd = runner.command(
            self.args, "sift", "baseline", self.group, self.root / "out", "unique"
        )
        values = dict(zip(cmd[4::2], cmd[5::2]))
        for key, value in {
            "base-rows": "900000",
            "groups": "10:10000",
            "query-rows": "200",
            "sweep-queries": "200",
            "build-index-queries": "0",
            "build-exact-queries": "0",
            "snapshot-queries": "0",
            "snapshot-exact": "0",
            "sweep-exact-queries": "0",
            "build-nprobe-sweep": "16,128,512",
            "nprobe-sweep": "16,128,512",
            "nlist": "1024",
            "buckets": "4",
            "bits": "7",
            "top-k": "100",
            "workers": "1",
            "daft-threads": "16",
            "search-parallelism": "1",
        }.items():
            self.assertEqual(values["--" + key], value)
        self.assertIn("--keep", cmd)
        self.assertNotIn("--skip-upserts", cmd)
        self.assertEqual(values["--table-path"], (self.group / "table").as_uri())
        self.assertIn(
            "--normalize",
            runner.command(
                self.args, "glove", "optimized", self.group, self.root / "out", "unique"
            ),
        )

    def test_environment_removes_inherited_cache_and_isolates_build(self):
        inherited = {
            "PYTHONPATH": "/unwanted",
            "LAKESOUL_CACHE": "0",
            "LAKESOUL_CACHE_PATH": "/shared",
            "LAKESOUL_CACHE_DIR": "/shared",
            "LAKESOUL_CACHE_SIZE": "100GiB",
            "OMP_NUM_THREADS": "128",
        }
        with patch.dict(os.environ, inherited):
            env = runner.environment(self.args, "/baseline", self.group)
            self.assertEqual(env["PYTHONPATH"], "/baseline")
            self.assertNotIn("LAKESOUL_CACHE", env)
            self.assertNotIn("LAKESOUL_CACHE_PATH", env)
            self.assertNotIn("LAKESOUL_CACHE_DIR", env)
            self.assertNotIn("LAKESOUL_CACHE_SIZE", env)
            self.assertEqual(env["LAKESOUL_INDEX_CACHE_BYTES"], str(8 * 1024**3))
            self.assertEqual(env["RAYON_NUM_THREADS"], "16")
            self.assertEqual(env["OPENBLAS_NUM_THREADS"], "1")
            self.assertEqual(env["OMP_NUM_THREADS"], "1")
            self.args.cache_mode = "on"
            env = runner.environment(self.args, "/optimized", self.group)
            self.assertEqual(env["LAKESOUL_CACHE"], "1")
            self.assertEqual(env["LAKESOUL_CACHE_PATH"], str(self.group / "cache"))
            self.assertEqual(env["LAKESOUL_CACHE_SIZE"], "1GiB")
            self.assertEqual(os.environ["LAKESOUL_CACHE_PATH"], "/shared")

    def test_child_failure_still_drops_and_removes_only_owned_group(self):
        self.args.output_dir.mkdir()

        def fail(cmd, env, log, *args, **kwargs):
            table = Path(cmd[cmd.index("--table-path") + 1].removeprefix("file://"))
            table.mkdir()
            log.write_text('BENCH_JSON {"partial": true}\n')
            runner.dump(
                log.with_suffix(".process.json"),
                {"process_group_exited": True, "exit_returncode": -15},
            )
            raise TimeoutError("simulated timeout")

        with patch.object(
            runner, "catalog", return_value={"name_absent": True}
        ) as catalog:
            with patch.object(runner, "run", side_effect=fail):
                with self.assertRaisesRegex(RuntimeError, "simulated timeout"):
                    runner.group_run(self.args, "sift", "baseline", None, 1)
        self.assertEqual(
            [call.args[2] for call in catalog.call_args_list], ["check", "drop"]
        )
        report_path = next(self.args.output_dir.glob("*/report.json"))
        report = json.loads(report_path.read_text())
        self.assertTrue(report["cleanup"]["scratch_absent"])
        self.assertTrue(report["cleanup"]["table_path_absent"])
        self.assertEqual(
            json.loads(report_path.with_name("stdout-results.json").read_text()),
            [{"partial": True}],
        )
        self.assertTrue(self.group.exists())  # Unrelated scratch group is untouched.

    def test_outputs_are_exclusive(self):
        path = self.root / "report.json"
        runner.dump(path, {"first": True})
        with self.assertRaises(FileExistsError):
            runner.dump(path, {"first": False})
        self.assertIn("true", path.read_text())


if __name__ == "__main__":
    unittest.main()
