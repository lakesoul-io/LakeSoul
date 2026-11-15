# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors
"""Pure tests: no LakeSoul import, HDF5, database, MinIO, or Rust build."""
import gzip
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

SOURCE = Path(__file__).resolve().parents[1] / "examples/vector-search/benchmark_native_batch.py"
SPEC = importlib.util.spec_from_file_location("native_batch_benchmark", SOURCE)
runner = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = runner
SPEC.loader.exec_module(runner)


class Clock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


class FakeAdapter:
    query_count = 1000

    def __init__(self, clock):
        self.clock = clock
        self.singles, self.batches = [], []

    def prepared(self, qids, seconds):
        self.clock.now += 17  # Input serialization is outside the timed call.

        def execute():
            self.clock.now += seconds
            return [list(range(runner.K)) for _ in qids]

        def decode(raw):
            self.clock.now += 31  # Result validation/decoding is outside timing.
            return runner.Groups(list(qids), raw)

        return runner.Prepared(execute, decode)

    def prepare_single(self, qid):
        self.singles.append(qid)
        return self.prepared([qid], .25)

    def prepare_batch(self, qids, *, full):
        self.batches.append((list(qids), full))
        return self.prepared(qids, 2 if full else 3)


class MemoryStore:
    def __init__(self):
        self.data, self.deleted = {}, []
        self.extra_listing = []
        self.fail_delete = False

    def list_files(self, prefix):
        return sorted(key for key in self.data if key.startswith(prefix + "/")) + self.extra_listing

    def read(self, path):
        return self.data[path]

    def write(self, path, data):
        self.data[path] = data

    def delete(self, path):
        self.deleted.append(path)
        if not self.fail_delete:
            del self.data[path]


class ProtocolTests(unittest.TestCase):
    def test_real_counts_and_no_repeat_or_gist_padding(self):
        for dataset in runner.DATASETS:
            n = 1000 if dataset == "gist" else 10_000
            single, batch = runner.real_query_indices(dataset, n, n)
            self.assertEqual(single, list(range(200)))
            self.assertEqual(batch, list(range(n)))
            self.assertEqual(len(batch), len(set(batch)))
            with self.assertRaises(runner.ProtocolError):
                runner.real_query_indices(dataset, n - 1, n)
            with self.assertRaises(runner.ProtocolError):
                runner.real_query_indices(dataset, n, n - 1)

    def test_original_dataset_parameters(self):
        expected = {"sift": (256, (64,)), "glove": (1024, (512,)),
                    "gist": (256, (64,)), "deep": (1024, (32, 64))}
        for dataset, (nlist, probes) in expected.items():
            spec = runner.DATASETS[dataset]
            self.assertEqual((spec.nlist, spec.probes, spec.bits), (nlist, probes, 7))
        self.assertEqual(runner.EXACT_OVERFETCH, 1)

    def test_timed_native_execution_exactly_once_and_chunk_timing_separate(self):
        clock = Clock()
        adapter = FakeAdapter(clock)
        truth = [list(range(runner.K)) for _ in range(1000)]
        saved = []
        result = runner.measure(adapter, "gist", truth, clock=clock,
                                save_groups=lambda phase, groups, recalls: saved.append((phase, len(groups.qids))))
        self.assertEqual(adapter.singles, list(range(200)))
        full = [qids for qids, is_full in adapter.batches if is_full]
        chunks = [qids for qids, is_full in adapter.batches if not is_full]
        self.assertEqual(full, [list(range(1000))])
        self.assertEqual([qid for chunk in chunks for qid in chunk], list(range(1000)))
        self.assertEqual(len(chunks), 10)
        self.assertEqual(result["single"]["wall_seconds"], 50)
        self.assertEqual(result["single"]["qps"], 4)
        batch = result["batch"]
        self.assertEqual(batch["native_calls"], 1)
        self.assertEqual(batch["completed_vectors"], 1000)
        self.assertEqual(batch["wall_seconds"], 2)
        self.assertEqual(batch["qps"], 500)  # Never 1000 / (2 + 30).
        sampling = batch["percentile_sampling"]
        self.assertEqual(sampling["wall_seconds"], 30)
        self.assertEqual(sampling["p50_ms"], 30)
        self.assertTrue(sampling["outside_full_timing"])
        self.assertEqual(batch["recall_per_query"], [1.0] * 1000)
        self.assertEqual(saved, [("single", 200), ("batch", 1000)])

    def test_short_result_is_not_reported_as_full_qps(self):
        clock = Clock()
        adapter = FakeAdapter(clock)
        original = adapter.prepare_batch

        def incomplete(qids, *, full):
            call = original(qids, full=full)
            if full:
                decode = call.decode
                call.decode = lambda raw: decode(raw[:-1])
            return call

        adapter.prepare_batch = incomplete
        with self.assertRaises(runner.ProtocolError):
            runner.measure(adapter, "gist", [list(range(100))] * 1000, clock=clock)
        self.assertEqual(len(adapter.batches), 1)  # Fail before percentile sampling.

    def test_zero_negative_and_nonfinite_wall_rejected(self):
        prepared = runner.Prepared(lambda: "raw", lambda raw: raw)
        for end in (0, -1, float("nan"), float("inf")):
            with self.subTest(end=end), self.assertRaises(runner.ProtocolError):
                runner.timed(prepared, iter([0, end]).__next__)

    def test_recall_is_per_query_not_combined_or_overflowed(self):
        truth = [list(range(100)), list(range(100, 200))]
        groups = runner.Groups([0, 1], [list(range(100)), list(range(200, 300))])
        self.assertEqual(runner.validate_groups(groups, [0, 1], truth, 1000), [1, 0])
        groups.ids.append(list(range(100)))
        with self.assertRaises(runner.ProtocolError):
            runner.validate_groups(groups, [0, 1], truth, 1000)

    def test_duplicate_missing_qid_short_k_and_invalid_id_rejected(self):
        truth = [list(range(100)), list(range(100))]
        cases = [runner.Groups([0, 0], [list(range(100))] * 2),
                 runner.Groups([0], [list(range(100))]),
                 runner.Groups([0, 1], [list(range(99)), list(range(100))]),
                 runner.Groups([0, 1], [[0] * 100, list(range(100))]),
                 runner.Groups([0, 1], [list(range(99)) + [-1], list(range(100))]),
                 runner.Groups([0, 1], [list(range(99)) + [1000], list(range(100))])]
        for groups in cases:
            with self.subTest(groups=groups.qids), self.assertRaises(runner.ProtocolError):
                runner.validate_groups(groups, [0, 1], truth, 1000)

    def test_sql_interleaved_rows_qids_and_ranks_preserved(self):
        rows = [(qid, rank, rank - 1, float(rank)) for rank in range(100, 0, -1) for qid in (7, 3)]
        groups = runner.sql_groups(rows, [3, 7], inline=False)
        self.assertEqual(groups.qids, [3, 7])
        self.assertEqual(groups.ids, [list(range(100))] * 2)
        inline = [(0, rank, rank - 1, float(rank)) for rank in range(1, 101)]
        self.assertEqual(runner.sql_groups(inline, [501], inline=True).qids, [501])

    def test_sql_unexpected_qid_rank_and_distance_rejected(self):
        rows = [(0, rank, rank - 1, float(rank)) for rank in range(1, 101)]
        cases = [rows[:-1], rows + [rows[0]], [(99, *rows[0][1:])] + rows[1:],
                 [(0, 0, 0, 0.0)] + rows[1:], [(0, 1, 0, float("nan"))] + rows[1:],
                 [(0, 1, 0, float("inf"))] + rows[1:], [(0, 1, 0, 1000.0)] + rows[1:]]
        for malformed in cases:
            with self.subTest(first=malformed[0]), self.assertRaises(runner.ProtocolError):
                runner.sql_groups(malformed, [0], inline=False)

    def test_empty_sql_stream_is_not_retried(self):
        from unittest.mock import Mock
        inner = types.SimpleNamespace(sql_stream=Mock(return_value=iter(())),
                                      sql=Mock(side_effect=AssertionError("must not retry")))
        session = types.SimpleNamespace(_inner=inner)
        self.assertEqual(runner.one_shot_sql(session, "SELECT native_batch"), [])
        inner.sql_stream.assert_called_once_with("SELECT native_batch")
        inner.sql.assert_not_called()

    def test_sql_uses_native_function_and_explicit_overfetch(self):
        config = {"dataset": "gist", "nprobe": 64, "rerank": "exact"}
        query = types.SimpleNamespace(tolist=lambda: [.1, .2])
        full = runner.sql_batch(config, [query], [0], full=True, target="target", query_table="queries")
        sample = runner.sql_batch(config, [query], [0], full=False, target="target", query_table="queries")
        self.assertIn("FROM lakesoul_vector_search_batch_from_table(", full)
        self.assertIn("'queries', 'query_id', 'query_vector', 100, 64, 'L2', 'exact', 1)", full)
        self.assertIn("FROM lakesoul_vector_search_batch(", sample)
        self.assertNotIn("UNION", full + sample)
        self.assertNotIn("ORDER BY", full + sample)


class OwnershipTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.parent = Path(self.temp.name).resolve()
        self.token = "a" * 32
        self.root = self.parent / f"nbatch_{self.token}_gist"
        self.root.mkdir()
        runner.dump_new(self.root / runner.MARKER, {"token": self.token})

    def test_local_cleanup_exact_owned_root_and_unrelated_survive(self):
        unrelated = self.parent / "unrelated"
        unrelated.mkdir()
        (unrelated / "keep").write_text("keep")
        for target in (self.parent, self.parent.parent, unrelated, self.root / "cache"):
            with self.assertRaises(runner.ProtocolError):
                runner.remove_owned_root(target, self.parent, self.token, "gist")
        runner.remove_owned_root(self.root, self.parent, self.token, "gist")
        self.assertFalse(self.root.exists())
        self.assertTrue((unrelated / "keep").exists())

    def test_local_marker_mismatch_and_symlink_rejected(self):
        marker = self.root / runner.MARKER
        marker.write_text(json.dumps({"token": "different"}))
        with self.assertRaises(runner.ProtocolError):
            runner.verified_root(self.root, self.parent, self.token, "gist")
        marker.unlink()
        outside = self.parent / "outside"
        outside.write_text(json.dumps({"token": self.token}))
        marker.symlink_to(outside)
        with self.assertRaises(runner.ProtocolError):
            runner.verified_root(self.root, self.parent, self.token, "gist")

    def test_cache_cleanup_requires_cache_marker_and_exact_child(self):
        cache = self.root / "cache/python_index_64"
        cache.mkdir(parents=True)
        with self.assertRaises(runner.ProtocolError):
            runner.remove_owned_cache(cache, self.root, self.parent, self.token, "gist")
        runner.dump_new(cache / runner.MARKER, {"token": self.token})
        runner.remove_owned_cache(cache, self.root, self.parent, self.token, "gist")
        self.assertFalse(cache.exists())
        with self.assertRaises(runner.ProtocolError):
            runner.remove_owned_cache(self.parent, self.root, self.parent, self.token, "gist")

    def test_cache_initializer_clears_only_data_child_not_owner_marker(self):
        cache = self.root / "cache/python_exact_64"
        cache.mkdir(parents=True)
        runner.dump_new(cache / runner.MARKER, {"token": self.token})
        env = runner.worker_environment(cache, "query", {})
        data = Path(env["LAKESOUL_CACHE_PATH"])
        self.assertEqual(data, cache / "data")
        data.mkdir()
        (data / "cached-page").write_bytes(b"data")
        # The native DiskCache initializes by clearing its own contents.
        for item in data.iterdir():
            item.unlink()
        self.assertEqual(json.loads((cache / runner.MARKER).read_text())["token"], self.token)
        runner.remove_owned_cache(cache, self.root, self.parent, self.token, "gist")
        self.assertFalse(cache.exists())

    def test_s3_cleanup_only_exact_owned_prefix(self):
        store = MemoryStore()
        owned = runner.OwnedObjects(store, self.token, "gist")
        owned.create()
        store.data[owned.prefix + "/target/_vector_index/a"] = b"test"
        unrelated = "lakesoul-bench2/minio_deep/keep"
        store.data[unrelated] = b"keep"
        result = owned.cleanup()
        self.assertTrue(result["verified_empty"])
        self.assertEqual(store.data, {unrelated: b"keep"})
        self.assertEqual(store.deleted[-1], owned.marker)

    def test_s3_prefix_escape_detected_before_any_delete(self):
        for escaped in ("lakesoul-bench2/elsewhere/keep", "lakesoul-bench2/native-batch-v2/" + self.token + "/gist/../deep/keep"):
            store = MemoryStore()
            owned = runner.OwnedObjects(store, self.token, "gist")
            owned.create()
            store.extra_listing = [escaped]
            with self.assertRaises(runner.ProtocolError):
                owned.cleanup()
            self.assertEqual(store.deleted, [])

    def test_s3_unowned_and_failed_deletes_keep_marker(self):
        store = MemoryStore()
        owned = runner.OwnedObjects(store, self.token, "gist")
        owned.create()
        store.data[owned.marker] = b'{"token":"other"}'
        with self.assertRaises(runner.ProtocolError):
            owned.cleanup()
        self.assertEqual(store.deleted, [])
        store.data[owned.marker] = json.dumps({"token": self.token}).encode()
        store.data[owned.prefix + "/target/file"] = b"data"
        store.fail_delete = True
        with self.assertRaises(runner.ProtocolError):
            owned.cleanup()
        self.assertIn(owned.marker, store.data)
        self.assertNotIn(owned.marker, store.deleted)

    def test_no_cleanup_while_owned_process_group_alive(self):
        runner.dump_new(self.parent / "owned-process.json", {"pid": 12345})
        with patch.object(runner, "process_group_alive", return_value=True):
            with self.assertRaises(runner.ProtocolError):
                runner.ensure_exited(self.parent)

    def test_environment_overrides_inherited_cache_and_preserves_stderr(self):
        inherited = {"LAKESOUL_VECTOR_MEMORY_BYTES": "8GiB", "LAKESOUL_INDEX_CACHE_BYTES": "100GiB",
                     "LAKESOUL_CACHE": "0", "LAKESOUL_CACHE_PATH": "/shared", "PYTHONPATH": "/other"}
        env = runner.worker_environment(self.root / "cache", "query", inherited)
        self.assertEqual(env["LAKESOUL_VECTOR_MEMORY_BYTES"], "512MiB")
        self.assertEqual(env["LAKESOUL_CACHE_SIZE"], "1GiB")
        self.assertEqual(env["LAKESOUL_VECTOR_LAZY_INDEX"], "auto")
        self.assertEqual(env["LAKESOUL_VECTOR_CACHE"], "1")
        self.assertNotIn("LAKESOUL_INDEX_CACHE_BYTES", env)
        self.assertEqual(inherited["LAKESOUL_CACHE_PATH"], "/shared")

    def test_result_artifact_contains_qids_ids_ranks_and_is_exclusive(self):
        groups = runner.Groups([17], [list(range(100))], [list(map(float, range(100)))])
        record = runner.save_result_groups(self.parent, "batch", groups, [.98])
        with gzip.open(record["path"], "rt") as stream:
            row = json.loads(stream.readline())
        self.assertEqual(row["query_id"], 17)
        self.assertEqual(row["ids"], list(range(100)))
        self.assertEqual(row["ranks"], list(range(1, 101)))
        self.assertEqual(row["recall"], .98)
        with self.assertRaises(FileExistsError):
            runner.save_result_groups(self.parent, "batch", groups, [.98])

    def test_supervisor_finishes_cleanup_before_next_dataset(self):
        args = runner.arguments(["--datasets", "gist", "sift", "--apis", "python", "--rerank", "index",
                                 "--scratch-root", str(self.parent / "scratch"), "--output-dir", str(self.parent / "output")])
        store = MemoryStore()
        calls = []

        def fake_stage(config, stage, out, env, timeout):
            calls.append((config["dataset"], stage))
            return {"kind": stage, "dataset": config["dataset"]}

        with patch.object(runner, "ArrowStore", return_value=store), \
             patch.object(runner, "build_fingerprint", return_value={}), \
             patch.object(runner, "execute_stage", side_effect=fake_stage):
            out = runner.supervisor(args)
        self.assertEqual(calls, [(name, stage) for name in ("gist", "sift")
                                 for stage in ("setup", "build", "query", "cleanup")])
        records = [json.loads(line) for line in (out / "results.v2.jsonl").read_text().splitlines()]
        self.assertEqual([row["kind"] for row in records],
                         ["protocol", "setup", "build", "query", "cleanup", "setup", "build", "query", "cleanup"])
        self.assertEqual(store.data, {})
        self.assertEqual(list(args.scratch_root.glob("nbatch_*")), [])

    def test_failure_cleans_first_dataset_and_does_not_start_next(self):
        args = runner.arguments(["--datasets", "gist", "sift", "--apis", "python", "--rerank", "index",
                                 "--scratch-root", str(self.parent / "scratch"), "--output-dir", str(self.parent / "output")])
        store = MemoryStore()
        store.data["lakesoul-bench2/minio_deep/keep"] = b"keep"
        calls = []

        def fake_stage(config, stage, out, env, timeout):
            calls.append((config["dataset"], stage))
            if stage == "build":
                raise RuntimeError("simulated engine failure")
            return {"kind": stage, "dataset": config["dataset"]}

        with patch.object(runner, "ArrowStore", return_value=store), \
             patch.object(runner, "build_fingerprint", return_value={}), \
             patch.object(runner, "execute_stage", side_effect=fake_stage):
            with self.assertRaisesRegex(RuntimeError, "simulated engine failure"):
                runner.supervisor(args)
        self.assertEqual(calls, [("gist", "setup"), ("gist", "build"), ("gist", "cleanup")])
        self.assertEqual(store.data, {"lakesoul-bench2/minio_deep/keep": b"keep"})
        self.assertEqual(list(args.scratch_root.glob("nbatch_*")), [])

    def test_child_error_preserves_stderr_and_exit_evidence(self):
        process = types.SimpleNamespace(pid=12345, returncode=7, wait=lambda **kwargs: 7)

        def fake_popen(command, **kwargs):
            kwargs["stderr"].write(b"native engine failure\\n")
            kwargs["stdout"].write(b"partial diagnostics\\n")
            return process

        out = self.parent / "child-logs"
        with patch.object(runner.subprocess, "Popen", side_effect=fake_popen), \
             patch.object(runner, "stop_process") as stop:
            with self.assertRaisesRegex(runner.ProtocolError, "exited 7"):
                runner.run_child(["not-executed"], {}, out, 1)
        stop.assert_called_once_with(process)
        self.assertEqual((out / "stderr.log").read_bytes(), b"native engine failure\\n")
        self.assertTrue(json.loads((out / "process.json").read_text())["process_group_exited"])

    def test_timeout_records_joined_exit_before_cleanup(self):
        process = types.SimpleNamespace(pid=12345, returncode=-15)
        process.wait = lambda **kwargs: (_ for _ in ()).throw(runner.subprocess.TimeoutExpired("mock", 1))
        out = self.parent / "timeout-logs"
        with patch.object(runner.subprocess, "Popen", return_value=process), \
             patch.object(runner, "stop_process") as stop:
            with self.assertRaises(runner.subprocess.TimeoutExpired):
                runner.run_child(["not-executed"], {}, out, 1)
        stop.assert_called_once_with(process)
        self.assertTrue(json.loads((out / "process.json").read_text())["process_group_exited"])

    def test_empty_owned_object_prefix_is_safe_to_retry(self):
        owned = runner.OwnedObjects(MemoryStore(), self.token, "gist")
        self.assertTrue(owned.cleanup()["already_absent"])

    def test_output_file_does_not_overwrite_old_raw(self):
        path = self.parent / "results.json"
        runner.dump_new(path, {"old": True})
        with self.assertRaises(FileExistsError):
            runner.dump_new(path, {"new": True})
        self.assertEqual(json.loads(path.read_text()), {"old": True})


if __name__ == "__main__":
    unittest.main()
