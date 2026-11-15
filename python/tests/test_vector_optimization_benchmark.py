# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright 2026 LakeSoul contributors
"""Pure A/B harness tests: no LakeSoul, DB, MinIO, HDF5 or Rust build."""
from contextlib import contextmanager
from dataclasses import asdict
import gzip
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

SOURCE = Path(__file__).resolve().parents[1] / "examples/vector-search/benchmark_vector_optimization.py"
SPEC = importlib.util.spec_from_file_location("vector_optimization_benchmark", SOURCE)
runner = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = runner
SPEC.loader.exec_module(runner)


@contextmanager
def no_resources(_server_pid):
    yield {"python": {"fake": True}, "pgwire": None}


class Clock:
    now = 0.0
    def __call__(self):
        return self.now


class FakeAdapter:
    query_count = 1000
    def __init__(self, clock, sql=False):
        self.clock, self.sql = clock, sql
        self.singles, self.full, self.chunks = [], [], []
    def prepare(self, qids, elapsed, *, distances=True):
        self.clock.now += 17  # Serialization/preparation is outside timing.
        def execute():
            self.clock.now += elapsed
            return [list(range(100)) for _ in qids]
        def decode(raw):
            self.clock.now += 31  # Validation is also outside native timing.
            return runner.base.Groups(list(qids), raw,
                                      [list(map(float, range(100))) for _ in qids] if self.sql and distances else None)
        return runner.base.Prepared(execute, decode)
    def prepare_single(self, qid):
        self.singles.append(qid)
        return self.prepare([qid], .25, distances=False)
    def prepare_batch(self, qids, *, full):
        (self.full if full else self.chunks).append(list(qids))
        return self.prepare(qids, 2 if full else 3)


class MemoryStore:
    def __init__(self):
        self.data, self.deleted = {}, []
    def list_files(self, prefix):
        return sorted(path for path in self.data if path.startswith(prefix + "/"))
    def write(self, path, value):
        self.data[path] = value
    def read(self, path):
        return self.data[path]
    def delete(self, path):
        self.deleted.append(path)
        del self.data[path]
    def inventory(self, prefix):
        return [{"path": path, "size": len(self.data[path]), "mtime_ns": 1}
                for path in self.list_files(prefix)]


class Fixture(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
    def variant(self, name="A"):
        directory = self.root / name
        package = directory / "python"
        module = package / "lakesoul"
        module.mkdir(parents=True)
        (module / "__init__.py").write_text("# fake wrapper\n")
        extension = module / "_lib.abi3.so"
        extension.write_bytes(b"private mock extension " + name.encode())
        binary = directory / "pgwire"
        binary.write_bytes(b"private mock pgwire " + name.encode())
        source = directory / "source.json"
        source_hash = runner.canonical_hash({"source": name})
        source.write_text(json.dumps({"source_tree_sha256": source_hash, "source_files": [
            {"path": "python/src/lakesoul/__init__.py", "sha256": runner.base.file_hash(module / "__init__.py")},
        ]}))
        receipt = directory / "fresh-build.json"
        receipt.write_text(json.dumps({"status": "passed", "source_verified_before_after": True,
            "source_tree_sha256": source_hash, "source_manifest": str(source),
            "artifacts": [{"kind": "python", "path": str(extension), "sha256": runner.base.file_hash(extension)},
                          {"kind": "pgwire", "path": str(binary), "sha256": runner.base.file_hash(binary)}]}))
        return asdict(runner.fingerprint_variant(name, package, binary, receipt))
    def config(self):
        parent = self.root / "scratch"
        parent.mkdir(exist_ok=True)
        manifest = {"scratch_parent": str(parent), "data_dir": str(self.root), "endpoint": "http://fake:9000",
                    "port": 16543, "metrics_port": 19190, "variants": {"A": {}}}
        config = runner.dataset_config(manifest, "gist", "a" * 32)
        root = Path(config["scratch"])
        root.mkdir()
        runner.base.dump_new(root / runner.base.MARKER, {"token": config["token"]})
        return config


class ProtocolTests(Fixture):
    def test_protocol_preserves_original_dataset_query_sizes_and_parameters(self):
        protocol = runner.methodology(3, 42)
        self.assertEqual(protocol["single_nq"], 200)
        self.assertEqual(protocol["batch_nq"], {"sift": 10000, "glove": 10000, "gist": 1000, "deep": 10000})
        self.assertEqual(protocol["exact_overfetch"], 1)
        self.assertEqual(protocol["hash_buckets"], 4)
        self.assertEqual(protocol["cache"]["L1"], "512MiB")
        self.assertEqual(protocol["cache"]["L2"], "1GiB")
        self.assertEqual({key: (value.nlist, value.bits, value.probes) for key, value in runner.base.DATASETS.items()},
                         {"sift": (256, 7, (64,)), "glove": (1024, 7, (512,)),
                          "gist": (256, 7, (64,)), "deep": (1024, 7, (32, 64))})
        self.assertIn("one owned physical", protocol["index"])
    def test_schedule_has_three_balanced_reproducible_interleaved_pairs(self):
        cases = runner.schedule("deep", runner.APIS, runner.RERANKS, 3, 12)
        self.assertEqual(len(cases), 72)
        self.assertEqual(cases, runner.schedule("deep", runner.APIS, runner.RERANKS, 3, 12))
        self.assertEqual(len({case["id"] for case in cases}), len(cases))
        for index in range(0, len(cases), 6):
            group = cases[index:index + 6]
            self.assertEqual([row["trial"] for row in group], [0, 0, 1, 1, 2, 2])
            self.assertEqual([row["variant"] for row in group[:2]], [row["variant"] for row in group[4:]])
            self.assertEqual([row["variant"] for row in group[:2]], [row["variant"] for row in group[2:4]][::-1])
            for pair in (group[:2], group[2:4], group[4:]):
                self.assertEqual({row["variant"] for row in pair}, {"A", "B"})
        with self.assertRaises(runner.ProtocolError):
            runner.schedule("gist", ["python"], ["index"], 2, 12)
    def test_timing_counts_one_full_call_and_separate_chunk_calls(self):
        clock = Clock()
        adapter = FakeAdapter(clock, sql=True)
        saved = []
        output = runner.measure(adapter, "gist", [list(range(100))] * 1000, clock=clock,
            resource_scope=no_resources, save=lambda phase, groups, recalls: saved.append((phase, len(groups.qids), groups.distances is not None)))
        self.assertEqual(adapter.singles, list(range(200)))
        self.assertEqual(adapter.full, [list(range(1000))])
        self.assertEqual(len(adapter.chunks), 10)
        self.assertEqual([qid for group in adapter.chunks for qid in group], list(range(1000)))
        self.assertEqual(output["single"]["wall_seconds"], 50)
        self.assertEqual(output["single"]["qps"], 4)
        self.assertEqual(output["single"]["first_ms"], 250)
        self.assertEqual(output["single"]["ordinary_single_calls"], 200)
        self.assertEqual(output["batch"]["native_calls"], 1)
        self.assertEqual(output["batch"]["wall_seconds"], 2)
        self.assertEqual(output["batch"]["qps"], 500)
        sampling = output["batch"]["percentile_sampling"]
        self.assertEqual(sampling["wall_seconds"], 30)
        self.assertEqual(sampling["p50_ms"], 30)
        self.assertTrue(sampling["outside_full_timing"])
        self.assertEqual(saved[:2], [("single", 200, False), ("full", 1000, True)])
        self.assertEqual(saved[2:], [(f"chunk-{start:05d}", 100, True) for start in range(0, 1000, 100)])
    def test_partial_full_result_is_not_counted_as_completed_and_not_retried(self):
        clock = Clock()
        adapter = FakeAdapter(clock)
        prepare = adapter.prepare_batch
        def short(qids, *, full):
            prepared = prepare(qids, full=full)
            if full:
                execute = prepared.execute
                prepared.execute = lambda: execute()[:-1]
            return prepared
        adapter.prepare_batch = short
        with self.assertRaises(runner.ProtocolError):
            runner.measure(adapter, "gist", [list(range(100))] * 1000, clock=clock, resource_scope=no_resources)
        self.assertEqual(len(adapter.full), 1)
        self.assertEqual(adapter.chunks, [])
    def test_cli_requires_candidate_artifacts_and_at_least_three_trials(self):
        with patch("sys.stderr"):
            with self.assertRaises(SystemExit):
                runner.arguments([])
            with self.assertRaises(SystemExit):
                runner.arguments(["--trials", "2", "--resume", "/some/run"])
            with self.assertRaises(SystemExit):
                runner.arguments(["--resume", "/some/run", "--cleanup-run", "/same/run"])
        args = runner.arguments(["--resume", "/some/run"])
        self.assertEqual(args.trials, 3)
        self.assertIsNone(args.baseline_python_package)
        self.assertIsNone(args.baseline_pg_binary)
        self.assertIsNone(args.baseline_build_receipt)
        explicit = runner.arguments(["--baseline-python-package", "/private/A/python", "--baseline-pg-binary", "/private/A/pgwire",
                                     "--baseline-build-receipt", "/private/A/build.json", "--candidate-python-package", "/private/B/python",
                                     "--candidate-pg-binary", "/private/B/pgwire", "--candidate-build-receipt", "/private/B/build.json"])
        self.assertEqual(explicit.baseline_python_package, Path("/private/A/python"))
    def test_paired_summary_refuses_partial_duplicate_and_unpaired_trials(self):
        cases = runner.schedule("gist", ["python"], ["index"], 3, 1)
        records = [{"case": case, "single": {"qps": 1 if case["variant"] == "A" else 2, "recall": .9},
                    "batch": {"qps": 10 if case["variant"] == "A" else 30, "recall": 1}} for case in cases]
        summary = runner.paired_summary(records, cases)
        self.assertEqual([pair["batch_qps_ratio_B_over_A"] for pair in summary["paired_results"]], [3] * 3)
        with self.assertRaises(runner.ProtocolError):
            runner.paired_summary(records[:-1], cases)


class NoRegressionAcceptanceTests(unittest.TestCase):
    def pairs(self, single=1.1, batch=1.1, api="python", rerank="index"):
        return [{"api": api, "rerank": rerank, "nprobe": 64, "trial": trial,
                 "single_qps_ratio_B_over_A": single,
                 "batch_qps_ratio_B_over_A": batch} for trial in range(3)]

    def test_single_gain_cannot_offset_batch_regression(self):
        result = runner.no_regression_decision(self.pairs(single=1.15, batch=.999999))
        self.assertFalse(result["accepted"])
        self.assertEqual(len(result["failures"]), 3)
        self.assertEqual({failure["metric"] for failure in result["failures"]},
                         {"batch_qps_ratio_B_over_A"})

    def test_batch_gain_cannot_offset_single_regression(self):
        self.assertFalse(runner.no_regression_decision(self.pairs(single=.999999, batch=2))["accepted"])

    def test_one_trial_or_configuration_regression_cannot_be_averaged_away(self):
        pairs = self.pairs(single=2, batch=2)
        pairs[0]["batch_qps_ratio_B_over_A"] = .99
        self.assertFalse(runner.no_regression_decision(pairs)["accepted"])
        pairs = self.pairs(single=2, batch=2) + self.pairs(single=2, batch=.99, api="datafusion", rerank="exact")
        self.assertFalse(runner.no_regression_decision(pairs)["accepted"])

    def test_equal_throughput_passes_but_latency_regression_does_not(self):
        pairs = self.pairs(single=1, batch=1)
        self.assertTrue(runner.no_regression_decision(pairs)["accepted"])
        pairs[1]["single_warm_p95_ms_ratio_B_over_A"] = 1.000001
        self.assertFalse(runner.no_regression_decision(pairs)["accepted"])
        pairs[1]["single_warm_p95_ms_ratio_B_over_A"] = 1
        pairs[0]["single_first_ms_ratio_B_over_A"] = 1.000001
        self.assertFalse(runner.no_regression_decision(pairs)["accepted"])

    def test_missing_trials_and_invalid_ratios_never_accept(self):
        self.assertFalse(runner.no_regression_decision([])["accepted"])
        self.assertFalse(runner.no_regression_decision(self.pairs()[:2])["accepted"])
        for invalid in (float("nan"), float("inf"), 0, -1, None):
            pairs = self.pairs()
            pairs[0]["single_qps_ratio_B_over_A"] = invalid
            self.assertFalse(runner.no_regression_decision(pairs)["accepted"])

    def test_summary_marks_complete_experiment_as_rejected_improvement(self):
        cases = runner.schedule("sift", ["python"], ["index"], 3, 1)
        records = [{"case": case,
                    "single": {"qps": 100 if case["variant"] == "A" else 120, "recall": .9},
                    "batch": {"qps": 1000 if case["variant"] == "A" else 999, "recall": .9}}
                   for case in cases]
        summary = runner.paired_summary(records, cases)
        self.assertTrue(summary["complete"])
        self.assertFalse(summary["no_regression"]["accepted"])
        self.assertFalse(summary["aggregates"][0]["no_regression"]["accepted"])


class AdapterCompatibilityTests(unittest.TestCase):
    @contextmanager
    def adapter(self, api, rerank="index"):
        from unittest.mock import Mock
        import re
        events, statements, projections = [], [], []
        clock = Clock()
        state = {"qid": 0, "batch_parallelism": 0}

        class Column:
            def __init__(self, qid):
                self.qid = qid
            def to_pylist(self):
                events.append(("consume", self.qid))
                clock.now += .25
                return list(range(100))
        class Result:
            def __init__(self, qid):
                self.qid = qid
            def column(self, name):
                if name != "id":
                    raise AssertionError("ordinary singles must consume the id column only")
                return Column(self.qid)
        def sql(text):
            statements.append(text)
            if text.startswith("SET lakesoul_vector_batch.parallelism"):
                state["batch_parallelism"] = int(text.rsplit("=", 1)[1])
            match = re.search(r"\[([0-9]+)", text)
            if match:
                state["qid"] = int(match[1])
            if not text.startswith("SET "):
                events.append(("execute", state["qid"]))
            return Result(state["qid"])
        def to_sql(query, **kwargs):
            projections.append(kwargs)
            self.assertEqual(kwargs, {"top_k": 100, "metric": "L2", "columns": ["id"]})
            return f'SELECT "id" FROM "target" ORDER BY array_distance(vec, ARRAY[{query[0]}, 0]) LIMIT 100'
        def search(query, **kwargs):
            self.assertEqual(kwargs["columns"], ["id"])
            self.assertNotIn("with_distance", kwargs)
            events.append(("execute", query[0]))
            return Result(query[0])
        table = types.SimpleNamespace(to_sql=Mock(side_effect=to_sql), search=Mock(side_effect=search))
        session = types.SimpleNamespace(sql=Mock(side_effect=sql), set_vector_search=Mock(),
                                        batch_search=Mock(side_effect=AssertionError("singles must not use batch_search")))
        cat = types.SimpleNamespace(table=Mock(return_value=table), session=Mock(return_value=session))
        def fetchall():
            events.append(("consume", state["qid"]))
            clock.now += .25
            return [(key,) for key in range(100)]
        cursor = types.SimpleNamespace(execute=Mock(side_effect=sql), fetchall=Mock(side_effect=fetchall))
        connection = types.SimpleNamespace(cursor=Mock(return_value=cursor), close=Mock())
        psycopg = types.ModuleType("psycopg")
        psycopg.connect = Mock(return_value=connection)
        queries = [types.SimpleNamespace(tolist=lambda qid=qid: [qid, 0]) for qid in range(1000)]
        config = {"dataset": "gist", "api": api, "table": "target", "query_table": "queries", "port": 16543,
                  "endpoint": "http://fake:9000", "rerank": rerank, "nprobe": 64}
        with patch.object(runner.base, "catalog", return_value=cat), \
             patch.dict(sys.modules, {"psycopg": psycopg}):
            adapter = runner.OptimizationAdapter(config, queries)
            yield adapter, clock, events, statements, projections, state, session, cursor

    def test_adapter_reuses_original_single_and_constructor_without_overrides(self):
        self.assertIs(runner.OptimizationAdapter, runner.base.Adapter)
        self.assertIs(runner.OptimizationAdapter.prepare_single, runner.base.Adapter.prepare_single)
        self.assertIs(runner.OptimizationAdapter.__init__, runner.base.Adapter.__init__)

    def test_real_single_builder_keeps_id_only_and_consumes_first_200_in_order(self):
        for api in runner.APIS:
            for rerank in runner.RERANKS:
                with self.subTest(api=api, rerank=rerank), self.adapter(api, rerank) as values:
                    adapter, clock, events, statements, projections, state, session, cursor = values
                    groups = []
                    elapsed = []
                    for qid in range(200):
                        prepared = adapter.prepare_single(qid)
                        raw, seconds = runner.base.timed(prepared, clock)
                        group = prepared.decode(raw)
                        groups.append(group)
                        elapsed.append(seconds)
                    self.assertEqual([group.qids for group in groups], [[qid] for qid in range(200)])
                    self.assertTrue(all(group.distances is None for group in groups))
                    self.assertEqual(events, [(event, qid) for qid in range(200) for event in ("execute", "consume")])
                    self.assertEqual(elapsed, [.25] * 200)
                    session.batch_search.assert_not_called()
                    for text in statements:
                        if text.startswith("SELECT "):
                            self.assertEqual(text.split(" FROM ", 1)[0].replace('"', ''), "SELECT id")
                            self.assertNotIn(" AS distance", text)
                            self.assertNotIn("lakesoul_vector_search_batch", text)
                    if api == "datafusion":
                        self.assertEqual(len(projections), 200)
                        self.assertTrue(all("with_distance" not in kwargs for kwargs in projections))

    def test_sql_batch_keeps_auto_parallelism_and_native_function_without_extra_set(self):
        for api in ("datafusion", "pgwire"):
            for rerank in runner.RERANKS:
                with self.subTest(api=api, rerank=rerank), self.adapter(api, rerank) as values:
                    adapter, clock, events, statements, projections, state, session, cursor = values
                    self.assertEqual(state["batch_parallelism"], 0)
                    self.assertFalse(any("SET lakesoul_vector_batch." in text for text in statements))
                    with patch.object(runner.base, "one_shot_sql", side_effect=lambda _session, text: sql_rows(text)):
                        def sql_rows(text):
                            statements.append(text)
                            return []
                        adapter.prepare_batch(list(range(1000)), full=True).execute()
                        adapter.prepare_batch(list(range(100)), full=False).execute()
                    selects = [text for text in statements if text.startswith("SELECT ")]
                    self.assertIn("FROM lakesoul_vector_search_batch_from_table(", selects[0])
                    self.assertIn("FROM lakesoul_vector_search_batch(", selects[1])
                    self.assertTrue(all(text.startswith("SELECT query_id, rank, id, distance FROM ") for text in selects))
                    self.assertEqual(state["batch_parallelism"], 0)
                    self.assertFalse(any("SET lakesoul_vector_batch." in text for text in statements))


class ArtifactTests(Fixture):
    def test_independent_variants_verify_receipt_source_and_actual_artifacts(self):
        first, second = self.variant("A"), self.variant("B")
        runner.verify_variant(first)
        runner.verify_variant(second)
        self.assertNotEqual(first["extension"], second["extension"])
        self.assertNotEqual(first["pg_binary"], second["pg_binary"])
        self.assertNotEqual(first["source_tree_sha256"], second["source_tree_sha256"])
        Path(second["extension"]).write_bytes(b"changed")
        with self.assertRaises(runner.ProtocolError):
            runner.verify_variant(second)
    def test_mutable_installed_artifacts_and_symlinks_are_rejected(self):
        forbidden = self.root / "installed"
        forbidden.mkdir()
        file = forbidden / "native.so"
        file.write_bytes(b"binary")
        with self.assertRaises(runner.ProtocolError):
            runner.private_artifact(file, forbidden, "extension")
        link = self.root / "symlink.so"
        link.symlink_to(file)
        with self.assertRaises(runner.ProtocolError):
            runner.private_artifact(link, self.root / "other", "extension")
    def test_wrapper_changes_and_receipt_changes_cannot_resume(self):
        variant = self.variant()
        Path(variant["python_package"], "lakesoul/__init__.py").write_text("# wrapper changed\n")
        with self.assertRaises(runner.ProtocolError):
            runner.verify_variant(variant)
    def test_worker_environment_overrides_old_repo_pythonpath_last(self):
        variant = {"python_package": "/private/baseline/python"}
        inherited = {"PYTHONPATH": "/mutable/repo", "LAKESOUL_CACHE_PATH": "/wrong/cache", "PG_SERVICE_PID": "17"}
        env = runner.variant_environment(variant, self.root / "owned-cache", "query", inherited)
        self.assertEqual(env["PYTHONPATH"], variant["python_package"])
        self.assertEqual(env["LAKESOUL_CACHE_PATH"], str(self.root / "owned-cache/data"))
        self.assertEqual(env["LAKESOUL_VECTOR_MEMORY_BYTES"], "512MiB")
        self.assertEqual(env["LAKESOUL_CACHE_SIZE"], "1GiB")
        self.assertNotIn("PG_SERVICE_PID", env)
        self.assertEqual(inherited["PYTHONPATH"], "/mutable/repo")
    def test_vector_memory_matrix_is_query_only_and_frozen_in_protocol(self):
        variant = {"python_package": "/private/A/python"}
        for value, count in (("16MiB", 16 * 1024**2), ("128MiB", 128 * 1024**2), ("1GiB", 1024**3), ("123456", 123456)):
            env = runner.variant_environment(variant, self.root / "cache", "query", {}, vector_memory=value)
            self.assertEqual(env["LAKESOUL_VECTOR_MEMORY_BYTES"], value)
            self.assertEqual(runner.methodology(3, 7, value)["cache"]["L1_bytes"], count)
        stage = runner.variant_environment(variant, self.root / "cache", "build", {}, vector_memory="16MiB")
        self.assertEqual(stage["LAKESOUL_VECTOR_MEMORY_BYTES"], "512MiB")
        self.assertEqual(runner.arguments(["--resume", "/run", "--vector-memory", "64MiB"]).vector_memory, "64MiB")
        for invalid in ("0", "0MiB", "-1", "NaN", "0.5GiB", "all", str(2**63)):
            with self.assertRaises(runner.ProtocolError):
                runner.vector_memory_bytes(invalid)

    def test_actual_loaded_extension_is_verified_not_declared_only(self):
        variant = self.variant()
        fake_lakesoul = types.ModuleType("lakesoul")
        fake_lakesoul.__path__ = []
        fake_extension = types.ModuleType("lakesoul._lib")
        fake_extension.__file__ = variant["extension"]
        fake_lakesoul._lib = fake_extension
        with patch.dict(sys.modules, {"lakesoul": fake_lakesoul, "lakesoul._lib": fake_extension}):
            result = runner.loaded_provenance(variant)
            self.assertEqual(result["loaded_extension_sha256"], variant["extension_sha256"])
            fake_extension.__file__ = variant["pg_binary"]
            with self.assertRaises(runner.ProtocolError):
                runner.loaded_provenance(variant)
    def test_physical_fingerprint_changes_when_manifest_or_physical_files_change(self):
        config = self.config()
        store = MemoryStore()
        owned = runner.base.OwnedObjects(store, config["token"], "gist")
        owned.create()
        root = owned.prefix + "/target/_vector_index/vec/-5/0"
        store.data[root + "/LATEST"] = b"g1v1"
        store.data[root + "/manifests/g1_v1.bin"] = b"manifest"
        store.data[root + "/cluster_000.seg"] = b"cluster"
        with patch.object(runner, "catalog_control_plane_pin", return_value={"current_commits": []}):
            first = runner.physical_index_fingerprint(store, config)
            self.assertEqual(first["storage_backend"], "s3-manifest")
            runner.assert_physical_index(store, config, first)
            store.data[root + "/LATEST"] = b"g1v2"
            with self.assertRaises(runner.ProtocolError):
                runner.assert_physical_index(store, config, first)
        self.assertEqual(store.deleted, [])
    def test_result_artifacts_preserve_scores_and_are_exclusive(self):
        groups = runner.base.Groups([3], [list(range(100))], [list(map(float, range(100)))])
        row = runner.save_groups(self.root, "chunk-00003", groups, [.75])
        with gzip.open(row["path"], "rt") as stream:
            result = json.loads(stream.readline())
        self.assertEqual(result["query_id"], 3)
        self.assertEqual(result["distances"], list(range(100)))
        self.assertEqual(result["ranks"], list(range(1, 101)))
        self.assertEqual(row["query_ids_sha256"], runner.canonical_hash([3]))
        with self.assertRaises(FileExistsError):
            runner.save_groups(self.root, "chunk-00003", groups, [.75])


class CatalogFingerprintTests(Fixture):
    ENV = {"LAKESOUL_PG_URL": "jdbc:postgresql://db.example:15432/meta_db?stringtype=unspecified&sslmode=require",
           "LAKESOUL_PG_USERNAME": "owner", "LAKESOUL_PG_PASSWORD": "secret"}

    def catalog_fixture(self):
        config = self.config()
        store = MemoryStore()
        owned = runner.base.OwnedObjects(store, config["token"], "gist")
        owned.create()
        prefix = runner.normalized_index_prefix(config["target_path"]) + "/_vector_index/vec/-5/0"
        filename = "cluster_0000_v0000.seg"
        store.data[runner.base.BUCKET + "/" + prefix + "/" + filename] = b"segment"
        store.data[owned.prefix + "/target/base.vortex"] = b"data"
        state = {"commit_id": 10, "generation": 1, "version": 2, "header": b"header", "prefix": prefix,
                 "segments": [{"filename": filename, "file_size": 7, "cluster_id": 0, "segment_version": 0,
                               "num_vectors": 100, "birth_version": 1, "data_files": [config["target_path"] + "/base.vortex"]}],
                 "mutation_head": None, "mutation_records": 0, "owner": config["token"],
                 "path_override": None, "lease_rows": [], "query_counters": {}, "drops": [], "no_commits": False}
        commands = []
        connection = types.SimpleNamespace(rollback=lambda: None, close=lambda: None)
        class Cursor:
            result = []
            def execute(self, sql, params=None):
                commands.append((sql, params))
                if "FROM table_info" in sql:
                    name = params[1]
                    target = name == config["table"]
                    path = config["target_path"] if target else config["query_path"]
                    if target and state["path_override"]:
                        path = state["path_override"]
                    self.result = [("target-id" if target else "query-id", "default", name, path,
                                    {"native_batch_owner": state["owner"]}, "schema", ";id")]
                elif "FROM table_name_id" in sql:
                    self.result = [("target-id" if params[1] == config["table"] else "query-id",)]
                elif "FROM table_path_id" in sql:
                    self.result = [("target-id" if params[0] == (state["path_override"] or config["target_path"]) else "query-id",)]
                elif "FROM partition_info" in sql:
                    self.result = [("-5", 1, "{data-commit}", "AppendCommit", "")]
                elif "FROM index_shard" in sql:
                    self.result = [] if state["no_commits"] else [(state["prefix"], 7, state["commit_id"], state["generation"], state["version"],
                        state["header"], state["segments"], state["mutation_head"], state["mutation_records"])]
                elif "FROM index_drop" in sql:
                    self.result = state["drops"]
                elif sql.startswith("BEGIN "):
                    self.result = []
                else:
                    raise AssertionError("unexpected or volatile metadata query: " + sql)
            def fetchall(self):
                import copy
                return copy.deepcopy(self.result)
        connection.cursor = lambda: Cursor()
        def connect(**kwargs):
            self.assertEqual(kwargs, {"host": "db.example", "port": 15432, "dbname": "meta_db",
                                      "user": "owner", "password": "secret", "sslmode": "require"})
            return connection
        catalog_pin = runner.catalog_control_plane_pin
        def reader(value):
            with patch.dict(os.environ, self.ENV):
                return catalog_pin(value, connect=connect)
        return config, store, state, commands, reader

    def test_catalog_backend_without_filesystem_manifest_has_stable_complete_pins(self):
        config, store, state, commands, reader = self.catalog_fixture()
        first = runner.physical_index_fingerprint(store, config, catalog_reader=reader)
        self.assertEqual(first["storage_backend"], "catalog")
        self.assertEqual(first["index_manifests"], [])
        commit = first["catalog_control_plane"]["current_commits"][0]
        self.assertEqual(commit["commit_id"], 10)
        self.assertEqual(commit["header_sha256"], runner.hashlib.sha256(b"header").hexdigest())
        self.assertEqual(commit["segments"], state["segments"])
        self.assertEqual(len(first["catalog_control_plane"]["tables"]), 2)
        for _ in range(3):
            self.assertEqual(runner.physical_index_fingerprint(store, config, catalog_reader=reader), first)
        selects = [(sql, params) for sql, params in commands if "FROM index_shard" in sql]
        self.assertTrue(selects)
        self.assertTrue(all("LIKE" not in sql and "left(sh.index_prefix, length(%s))=%s" in sql for sql, _ in selects))
        self.assertEqual(selects[0][1][1], selects[0][1][2])
        self.assertTrue(selects[0][1][1].endswith("/_vector_index/vec/"))
        self.assertEqual(store.deleted, [])

    def test_current_commit_header_generation_segment_and_mutation_drifts_change_fingerprint(self):
        import copy
        config, store, state, commands, reader = self.catalog_fixture()
        original = copy.deepcopy(state)
        first = runner.physical_index_fingerprint(store, config, catalog_reader=reader)
        mutations = [lambda row: row.update(commit_id=11), lambda row: row.update(generation=2),
                     lambda row: row.update(version=3), lambda row: row.update(header=b"other header"),
                     lambda row: row["segments"][0].update(birth_version=99),
                     lambda row: row.update(mutation_head="mutation-v1", mutation_records=1)]
        for mutate in mutations:
            state.clear()
            state.update(copy.deepcopy(original))
            mutate(state)
            with self.subTest(state=state), patch.object(runner, "catalog_control_plane_pin", side_effect=reader):
                with self.assertRaisesRegex(runner.ProtocolError, "changed between A/B"):
                    runner.assert_physical_index(store, config, first)

    def test_owner_path_shard_prefix_segment_escape_and_missing_commit_fail_closed(self):
        import copy
        config, store, state, commands, reader = self.catalog_fixture()
        original = copy.deepcopy(state)
        mutations = [lambda row: row.update(owner="wrong-owner"),
                     lambda row: row.update(path_override=config["target_path"] + "_other"),
                     lambda row: row.update(prefix=row["prefix"].replace("/target/", "/target_other/")),
                     lambda row: row["segments"][0].update(filename="../escape.seg"),
                     lambda row: row.update(commit_id=None),
                     lambda row: row.update(drops=[(row["prefix"], "tombstoned", "job-id")])]
        for mutate in mutations:
            state.clear()
            state.update(copy.deepcopy(original))
            mutate(state)
            with self.subTest(state=state), self.assertRaises(runner.ProtocolError):
                runner.physical_index_fingerprint(store, config, catalog_reader=reader)
        self.assertEqual(store.deleted, [])

    def test_read_lease_rows_and_query_counters_are_not_physical_control_pins(self):
        config, store, state, commands, reader = self.catalog_fixture()
        first = runner.physical_index_fingerprint(store, config, catalog_reader=reader)
        state["lease_rows"] = [{"lease_id": "new-reader", "expires_at": 999}]
        state["query_counters"] = {"queries": 1234, "hits": 9876}
        self.assertEqual(runner.physical_index_fingerprint(store, config, catalog_reader=reader), first)
        self.assertFalse(any("index_read_lease" in sql or "index_lease" in sql or "query_counters" in sql for sql, _ in commands))

    def test_no_metadata_or_filesystem_pin_and_physical_segment_mismatch_are_rejected(self):
        config, store, state, commands, reader = self.catalog_fixture()
        state["no_commits"] = True
        with self.assertRaisesRegex(runner.ProtocolError, "neither current catalog"):
            runner.physical_index_fingerprint(store, config, catalog_reader=reader)
        state["no_commits"] = False
        state["segments"][0]["file_size"] = 8
        with self.assertRaisesRegex(runner.ProtocolError, "physical inventory"):
            runner.physical_index_fingerprint(store, config, catalog_reader=reader)

    def test_jdbc_parser_uses_primary_validated_environment_and_filters_driver_flags(self):
        kwargs = runner.catalog_connection_kwargs(self.ENV)
        self.assertNotIn("stringtype", kwargs)
        self.assertNotIn("jdbc", kwargs)
        self.assertEqual(kwargs["host"], "db.example")
        self.assertEqual(kwargs["dbname"], "meta_db")
        with self.assertRaises(runner.ProtocolError):
            runner.catalog_connection_kwargs({"LAKESOUL_PG_URL": "file:///tmp/metadata"})


class CompletionTests(Fixture):
    def complete_record(self, api="python"):
        variant = self.variant("A")
        case = runner.schedule("gist", [api], ["index"], 3, 1)[0]
        case = {**case, "variant": "A", "id": f"{api}-index-p64-t00-A"}
        attempt = self.root / "cases" / case["id"] / "attempt-one"
        query = attempt / "query"
        query.mkdir(parents=True)
        @contextmanager
        def resources(_pid):
            yield {"python": {"samples": 2, "kernel_peak_hwm_bytes": 4096,
                               "resource_ru_maxrss_bytes": 4096, "identity_changed": False},
                   "pgwire": {"samples": 2, "kernel_peak_hwm_bytes": 8192,
                               "sampled_peak_rss_bytes": 8192, "identity_changed": False} if api == "pgwire" else None}
        clock = Clock()
        measured = runner.measure(FakeAdapter(clock, sql=api != "python"), "gist", [list(range(100))] * 1000,
            clock=clock, resource_scope=resources, save=lambda *values: runner.save_groups(query, *values))
        provenance = {key: variant[key] for key in ("source_tree_sha256", "python_sources_sha256", "build_receipt_sha256", "pg_binary_sha256")}
        provenance.update({"loaded_extension_sha256": variant["extension_sha256"], "loaded_extension": variant["extension"]})
        record = {"status": "complete", "protocol_version": runner.VERSION, "dataset": "gist", "case": case,
                  "harness_sha256": runner.base.file_hash(runner.Path(runner.__file__)),
                  "helpers_sha256": runner.base.file_hash(runner.HELPER_PATH),
                  "provenance": provenance, "physical_index_sha256": "same-index", **measured}
        runner.base.dump_new(query / "record.json", record)
        receipt = {"record_sha256": runner.base.file_hash(query / "record.json"),
                   "worker_process": {"returncode": 0, "process_group_exited": True}}
        if api == "pgwire":
            receipt["server_process"] = {"returncode": -15, "process_group_exited": True}
        runner.base.dump_new(attempt / "completed.json", receipt)
        return record, case, variant, attempt
    def test_completed_case_resumes_only_with_matching_index_and_all_raw_evidence(self):
        record, case, variant, attempt = self.complete_record()
        actual = runner.case_completed(attempt.parent, case, variant, "same-index")
        self.assertEqual(actual["batch"]["qps"], 500)
        with self.assertRaises(runner.ProtocolError):
            runner.case_completed(attempt.parent, case, variant, "different-index")
        artifact = Path(record["batch"]["result_artifact"]["path"])
        artifact.write_bytes(b"tampered")
        with self.assertRaises(runner.ProtocolError):
            runner.case_completed(attempt.parent, case, variant, "same-index")
    def test_partial_attempt_is_never_counted_even_with_some_result_files(self):
        directory = self.root / "case"
        (directory / "attempt-interrupted").mkdir(parents=True)
        (directory / "attempt-interrupted/partial.json").write_text('{"nq":1000}')
        self.assertIsNone(runner.case_completed(directory, {}, {}, "same-index"))
    def test_invalid_full_count_qps_call_count_and_missing_peak_rejected(self):
        import copy
        record, case, variant, attempt = self.complete_record()
        mutations = [lambda row: row["batch"].update(completed_vectors=999),
                     lambda row: row["batch"].update(qps=250),
                     lambda row: row["batch"].update(native_calls=2),
                     lambda row: row["single"].update(ordinary_single_calls=1),
                     lambda row: row["batch"]["percentile_sampling"].update(outside_full_timing=False),
                     lambda row: row["batch"]["resources"].update(python={})]
        for mutate in mutations:
            bad = copy.deepcopy(record)
            mutate(bad)
            with self.assertRaises(runner.ProtocolError):
                runner.validate_complete_record(bad, case, variant, "same-index", attempt / "query")
    def test_sql_case_needs_saved_distances_and_pg_peak_while_server_alive(self):
        import copy
        record, case, variant, attempt = self.complete_record("pgwire")
        runner.validate_complete_record(record, case, variant, "same-index", attempt / "query")
        bad = copy.deepcopy(record)
        bad["batch"]["resources"]["pgwire"] = None
        with self.assertRaises(runner.ProtocolError):
            runner.validate_complete_record(bad, case, variant, "same-index", attempt / "query")
        single_artifact = record["single"]["result_artifact"]
        with gzip.open(single_artifact["path"], "rt") as stream:
            self.assertNotIn("distances", json.loads(next(stream)))
        artifact = record["batch"]["result_artifact"]
        path = Path(artifact["path"])
        with gzip.open(path, "rt") as stream:
            rows = [json.loads(line) for line in stream]
        for row in rows:
            row.pop("distances")
        with gzip.open(path, "wt") as stream:
            for row in rows:
                stream.write(json.dumps(row) + "\n")
        record["batch"]["result_artifact"]["sha256"] = runner.base.file_hash(path)
        with self.assertRaises(runner.ProtocolError):
            runner.validate_complete_record(record, case, variant, "same-index", attempt / "query")
    def test_sql_chunk_distances_remain_required_with_id_only_single_artifacts(self):
        record, case, variant, attempt = self.complete_record("datafusion")
        runner.validate_complete_record(record, case, variant, "same-index", attempt / "query")
        artifact = record["batch"]["percentile_sampling"]["chunks"][0]["result_artifact"]
        path = Path(artifact["path"])
        with gzip.open(path, "rt") as stream:
            rows = [json.loads(line) for line in stream]
        rows[0].pop("distances")
        with gzip.open(path, "wt") as stream:
            for row in rows:
                stream.write(json.dumps(row) + "\n")
        artifact["sha256"] = runner.base.file_hash(path)
        with self.assertRaisesRegex(runner.ProtocolError, "SQL artifacts require"):
            runner.validate_complete_record(record, case, variant, "same-index", attempt / "query")

    def test_unjoined_worker_is_not_a_completed_case(self):
        record, case, variant, attempt = self.complete_record()
        receipt_path = attempt / "completed.json"
        receipt = json.loads(receipt_path.read_text())
        receipt["worker_process"]["process_group_exited"] = False
        receipt_path.write_text(json.dumps(receipt))
        with self.assertRaises(runner.ProtocolError):
            runner.case_completed(attempt.parent, case, variant, "same-index")


class SamplerTests(unittest.TestCase):
    def test_proc_parser_handles_spaces_in_process_name_and_local_io(self):
        fields = ["S"] + ["0"] * 21
        fields[11], fields[12], fields[19] = "17", "5", "12345"
        files = {"status": "Name:\tpython\nVmRSS:\t100 kB\nVmHWM:\t200 kB\n",
                 "stat": "15 (python worker with spaces) " + " ".join(fields),
                 "io": "rchar: 50\nwchar: 70\nread_bytes: 4096\nwrite_bytes: 8192\n"}
        result = runner.proc_sample(15, read_text=lambda path: files[path.name])
        self.assertEqual(result["start_ticks"], 12345)
        self.assertEqual(result["cpu_user_ticks"], 17)
        self.assertEqual(result["cpu_system_ticks"], 5)
        self.assertEqual(result["VmHWM_bytes"], 200 * 1024)
        self.assertEqual(result["io"]["read_bytes"], 4096)
    def test_sampler_keeps_high_water_mark_and_phase_max_while_alive(self):
        rows = [{"start_ticks": 5, "VmRSS_bytes": 10, "VmHWM_bytes": 20, "cpu_user_ticks": 1, "cpu_system_ticks": 1, "io": {"read_bytes": 100}},
                {"start_ticks": 5, "VmRSS_bytes": 80, "VmHWM_bytes": 90, "cpu_user_ticks": 2, "cpu_system_ticks": 2, "io": {"read_bytes": 180}},
                {"start_ticks": 5, "VmRSS_bytes": 30, "VmHWM_bytes": 90, "cpu_user_ticks": 4, "cpu_system_ticks": 3, "io": {"read_bytes": 200}}]
        sampler = runner.ProcessSampler(12, reader=lambda _: rows.pop(0) if rows else None)
        sampler.sample()
        sampler.sample()
        result = sampler.finish()
        self.assertEqual(result["sampled_peak_rss_bytes"], 80)
        self.assertEqual(result["kernel_peak_hwm_bytes"], 90)
        self.assertEqual(result["cpu_user_ticks_delta"], 3)
        self.assertEqual(result["io_delta"]["read_bytes"], 100)
        self.assertIn("not remote", result["scope"])
    def test_sampler_does_not_mix_reused_pid(self):
        rows = [{"start_ticks": 5, "VmRSS_bytes": 10}, {"start_ticks": 8, "VmRSS_bytes": 1000}]
        sampler = runner.ProcessSampler(12, reader=lambda _: rows.pop(0) if rows else None)
        sampler.sample()
        result = sampler.finish()
        self.assertEqual(result["sampled_peak_rss_bytes"], 10)
        self.assertTrue(result["identity_changed"])
    def test_sampler_handles_exit_between_samples(self):
        with patch.object(Path, "read_text", side_effect=FileNotFoundError):
            self.assertIsNone(runner.proc_sample(12))
        result = runner.ProcessSampler(12, reader=lambda _: None).finish()
        self.assertIsNone(result["kernel_peak_hwm_bytes"])
        self.assertEqual(result["samples"], 0)


class LifecycleTests(Fixture):
    def test_one_dataset_build_and_cleanup_after_all_variants_trials(self):
        variant_a, variant_b = self.variant("A"), self.variant("B")
        scratch = self.root / "scratch"
        scratch.mkdir()
        manifest = {"scratch_parent": str(scratch), "data_dir": str(self.root), "endpoint": "http://fake:9000", "port": 1,
                    "metrics_port": 2, "variants": {"A": variant_a, "B": variant_b},
                    "schedule": {name: runner.schedule(name, ["python"], ["index"], 3, 7) for name in ("gist", "sift")}}
        calls = []
        physical = {"sha256": "same-physical-index"}
        def stage(config, stage, out, env, timeout):
            calls.append((config["dataset"], stage, env["PYTHONPATH"]))
            return {"kind": stage}
        def case(config, current, variant, index, out, timeout):
            calls.append((config["dataset"], "case", current["variant"], config["token"], index["sha256"]))
            return {"case": current}
        def cleanup(config, epoch, store, timeout):
            calls.append((config["dataset"], "cleanup"))
            runner.base.remove_owned_root(Path(config["scratch"]), scratch, config["token"], config["dataset"])
            return {"scratch_absent": True}
        with patch.object(runner, "execute_stage", side_effect=stage), \
             patch.object(runner, "physical_index_fingerprint", return_value=physical), \
             patch.object(runner, "assert_physical_index"), patch.object(runner, "run_case", side_effect=case), \
             patch.object(runner, "cleanup_epoch", side_effect=cleanup), \
             patch.object(runner, "paired_summary", return_value={"complete": True}):
            for dataset in ("gist", "sift"):
                runner.run_dataset(manifest, dataset, self.root, MemoryStore(), 1)
        for name in ("gist", "sift"):
            rows = [row for row in calls if row[0] == name]
            self.assertEqual([row[1] for row in rows], ["setup", "build"] + ["case"] * 6 + ["cleanup"])
            self.assertEqual(len({row[3] for row in rows if row[1] == "case"}), 1)
            self.assertEqual({row[4] for row in rows if row[1] == "case"}, {"same-physical-index"})
        gist_cleanup = calls.index(("gist", "cleanup"))
        sift_setup = next(index for index, row in enumerate(calls) if row[:2] == ("sift", "setup"))
        self.assertLess(gist_cleanup, sift_setup)
    def test_each_case_removes_only_its_cache_immediately_after_process_exit(self):
        variant = self.variant("A")
        config = self.config()
        case = {**runner.schedule("gist", ["python"], ["index"], 3, 7)[0], "variant": "A"}
        output = self.root / "epoch-output"
        output.mkdir()
        exited = []
        original_remove = runner.base.remove_owned_cache
        def stage(current, name, out, env, timeout):
            out.mkdir()
            (out / "process").mkdir()
            runner.base.dump_new(out / "record.json", {"complete": True})
            runner.base.dump_new(out / "process/process.json", {"returncode": 0, "process_group_exited": True})
            data = Path(env["LAKESOUL_CACHE_PATH"])
            data.mkdir()
            (data / "cached-object").write_bytes(b"cache")
            return {"complete": True}
        def cleanup(cache, root, parent, token, dataset):
            self.assertTrue(exited, "must verify exited processes before cache deletion")
            original_remove(cache, root, parent, token, dataset)
        with patch.object(runner, "execute_stage", side_effect=stage), \
             patch.object(runner, "validate_complete_record"), \
             patch.object(runner, "ensure_exited", side_effect=lambda path: exited.append(path)), \
             patch.object(runner.base, "remove_owned_cache", side_effect=cleanup):
            runner.run_case(config, case, variant, {"sha256": "shared-index"}, output, 1)
        cache = Path(config["scratch"]) / "cache/python_index_64"
        self.assertFalse(cache.exists())
        self.assertTrue(Path(config["scratch"]).exists(), "shared dataset/index root must survive between variants")
        self.assertTrue((Path(config["scratch"]) / runner.base.MARKER).exists())

    def test_interrupted_trial_preserves_owned_index_for_resume_and_never_advances_dataset(self):
        variants = {"A": self.variant("A"), "B": self.variant("B")}
        scratch = self.root / "scratch"
        scratch.mkdir()
        manifest = {"scratch_parent": str(scratch), "data_dir": str(self.root), "endpoint": "http://fake:9000", "port": 1,
                    "metrics_port": 2, "variants": variants,
                    "schedule": {"gist": runner.schedule("gist", ["python"], ["index"], 3, 7)}}
        physical = {"sha256": "same-index"}
        store = MemoryStore()
        with patch.object(runner, "execute_stage", return_value={}), \
             patch.object(runner, "physical_index_fingerprint", return_value=physical), \
             patch.object(runner, "assert_physical_index"), \
             patch.object(runner, "run_case", side_effect=KeyboardInterrupt("interrupted")), \
             patch.object(runner, "cleanup_epoch") as cleanup:
            with self.assertRaises(KeyboardInterrupt):
                runner.run_dataset(manifest, "gist", self.root, store, 1)
        cleanup.assert_not_called()
        epoch = next((self.root / "gist/epochs").iterdir())
        config = json.loads((epoch / "dataset.json").read_text())
        self.assertTrue(Path(config["scratch"]).exists())
        self.assertTrue((epoch / "physical-index.json").exists())
        self.assertFalse((self.root / "gist/dataset-complete.json").exists())
        self.assertFalse((self.root / "sift").exists())
        self.assertEqual(len(list(scratch.glob("nbatch_*"))), 1)

    def test_active_owned_process_prevents_cleanup_and_resume(self):
        marker = self.root / "owned-process.json"
        marker.write_text(json.dumps({"pid": 1234, "start_ticks": 10}))
        with patch.object(runner.base, "process_group_alive", return_value=True), \
             patch.object(runner, "proc_sample", return_value={"start_ticks": 10}):
            with self.assertRaises(runner.ProtocolError):
                runner.ensure_exited(self.root)
    def test_recovery_stops_only_the_original_recorded_process_group(self):
        marker = self.root / "owned-process.json"
        marker.write_text(json.dumps({"pid": 1234, "start_ticks": 10}))
        samples = iter([{"start_ticks": 10}, None])
        with patch.object(runner, "proc_sample", side_effect=lambda _: next(samples, None)), \
             patch.object(os, "getpgid", return_value=1234), \
             patch.object(os, "killpg") as kill, \
             patch.object(runner.base, "process_group_alive", return_value=False):
            runner.stop_recorded_processes(self.root)
        kill.assert_called_once_with(1234, runner.signal.SIGTERM)
        recovery = next(self.root.glob("recovered-*.json"))
        self.assertTrue(json.loads(recovery.read_text())["process_group_exited"])

    def test_recovery_never_signals_a_reused_pid_or_unknown_group(self):
        marker = self.root / "owned-process.json"
        marker.write_text(json.dumps({"pid": 1234, "start_ticks": 10}))
        with patch.object(runner, "proc_sample", return_value={"start_ticks": 99}), \
             patch.object(os, "killpg") as kill:
            runner.stop_recorded_processes(self.root)
            kill.assert_not_called()
        with patch.object(runner, "proc_sample", return_value={"start_ticks": 10}), \
             patch.object(os, "getpgid", return_value=4321), patch.object(os, "killpg") as kill:
            with self.assertRaises(runner.ProtocolError):
                runner.stop_recorded_processes(self.root)
            kill.assert_not_called()

    def test_pid_reuse_does_not_claim_or_kill_an_unrelated_process(self):
        marker = self.root / "owned-process.json"
        marker.write_text(json.dumps({"pid": 1234, "start_ticks": 10}))
        with patch.object(runner.base, "process_group_alive", return_value=True), \
             patch.object(runner, "proc_sample", return_value={"start_ticks": 99}), \
             patch.object(os, "killpg") as kill:
            runner.ensure_exited(self.root)
            kill.assert_not_called()


if __name__ == "__main__":
    unittest.main()
