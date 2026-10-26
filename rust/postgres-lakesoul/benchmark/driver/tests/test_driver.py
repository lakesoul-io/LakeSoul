"""Fast standard-library regression tests; real protocol coverage is smoke.py."""

from __future__ import annotations

import argparse
import asyncio
import json
import tempfile
import unittest
from pathlib import Path

from lsbench.answers import compare, ordered
from lsbench.cli import int32, parser, read_mix, substitute_sf
from lsbench.console import MemoryTimeline, Parser, check, script
from lsbench.wire import ReadPolicy, Result


class AnswerTests(unittest.TestCase):
    def check_answer(
        self, values, expected, *, sql="SELECT value", columns=None, oids=None
    ):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "answer.csv"
            path.write_text(expected, encoding="utf-8")
            result = Result(
                columns=columns or ["actual_name"],
                type_oids=oids or [1700],
                values=values,
                rows=len(values),
            )
            return compare(result, path, sql)

    def test_header_names_are_ignored(self):
        self.assertEqual(
            self.check_answer([("42",)], "different_label\n42\n"), (True, None)
        )

    def test_column_count_is_checked(self):
        correct, mismatch = self.check_answer([("42",)], "one,two\n42,43\n")
        self.assertFalse(correct)
        self.assertIn("Column count", mismatch)

    def test_row_count_is_checked(self):
        correct, mismatch = self.check_answer([("42",)], "n\n42\n43\n")
        self.assertFalse(correct)
        self.assertIn("Row count", mismatch)

    def test_numeric_absolute_and_relative_tolerance(self):
        self.assertTrue(
            self.check_answer([("1.009",), ("1000001",)], "n\n1\n1000000\n")[0]
        )
        self.assertFalse(self.check_answer([("1.011",)], "n\n1\n")[0])

    def test_ordered_rows(self):
        self.assertFalse(
            self.check_answer(
                [("1",), ("2",)], "n\n2\n1\n", sql="SELECT value ORDER BY value"
            )[0]
        )
        self.assertTrue(self.check_answer([("1",), ("2",)], "n\n2\n1\n")[0])

    def test_ordered_ties_compare_as_multisets(self):
        # TPC-H q11 orders by `value` only; rows with equal values may come in
        # any order, but every row and the order between distinct keys count.
        sql = "SELECT ps_partkey, sum(x) AS value FROM t GROUP BY ps_partkey ORDER BY value DESC"
        expected = "ps_partkey,value\n7,9.00\n1,5.00\n2,5.00\n3,4.00\n"

        def check(values, sql=sql):
            return self.check_answer(
                values,
                expected,
                sql=sql,
                columns=["ps_partkey", "value"],
                oids=[20, 1700],
            )

        self.assertEqual(
            check([("7", "9.00"), ("2", "5.00"), ("1", "5.00"), ("3", "4.00")]),
            (True, None),
        )
        correct, mismatch = check(
            [("7", "9.00"), ("2", "5.00"), ("8", "5.00"), ("3", "4.00")]
        )
        self.assertFalse(correct)
        self.assertIn("Ordered rows 2-3 with equal sort keys", mismatch)
        self.assertFalse(
            check([("7", "9.00"), ("3", "4.00"), ("1", "5.00"), ("2", "5.00")])[0]
        )
        # Keys that are not plain output columns keep exact row order.
        self.assertFalse(
            check(
                [("7", "9.00"), ("2", "5.00"), ("1", "5.00"), ("3", "4.00")],
                sql="SELECT ps_partkey, value FROM t ORDER BY value + 0 DESC",
            )[0]
        )

    def test_tie_cut_by_limit_must_keep_the_reference_rows(self):
        sql = "SELECT k, v FROM t ORDER BY v DESC LIMIT 2"
        expected = "k,v\n1,9\n2,5\n"
        self.assertTrue(
            self.check_answer(
                [("1", "9"), ("2", "5")],
                expected,
                sql=sql,
                columns=["k", "v"],
                oids=[20, 20],
            )[0]
        )
        self.assertFalse(
            self.check_answer(
                [("1", "9"), ("3", "5")],
                expected,
                sql=sql,
                columns=["k", "v"],
                oids=[20, 20],
            )[0]
        )

    def test_duplicates_preserved(self):
        self.assertFalse(self.check_answer([("1",), ("1",)], "n\n1\n2\n")[0])
        self.assertTrue(self.check_answer([("1",), ("1",)], "n\n1\n1\n")[0])

    def test_overlapping_numeric_matches_use_augmenting_paths(self):
        # First actual row can match either reference. Second can match only
        # the first, so a naive first-fit greedy algorithm incorrectly fails.
        self.assertTrue(self.check_answer([("0.009",), ("0.001",)], "n\n0\n0.018\n")[0])

    def test_text_is_not_coerced_to_number(self):
        self.assertFalse(self.check_answer([("001",)], "n\n1\n", oids=[25])[0])
        self.assertTrue(self.check_answer([("a   ",)], "n\na\n", oids=[25])[0])

    def test_dates_nulls_and_quoted_csv(self):
        self.assertTrue(
            self.check_answer(
                [("2026-10-16", None, "a,b")],
                'date,null,text\n2026-10-16,,"a,b"\n',
                columns=["d", "n", "t"],
                oids=[1082, 25, 25],
            )[0]
        )

    def test_order_detection_ignores_nested_and_quoted_sql(self):
        unordered = [
            "SELECT * FROM (SELECT 1 ORDER BY 1) t",
            "SELECT 'ORDER BY'",
            'SELECT "ORDER BY"',
            "SELECT $$ ORDER BY $$",
            "SELECT $tag$ ORDER BY $tag$",
            "SELECT 1 -- ORDER BY 1",
            "SELECT 1 /* nested /* ORDER BY */ comment */",
            "WITH t AS (SELECT 1 ORDER BY 1) SELECT * FROM t",
        ]
        for sql in unordered:
            with self.subTest(sql=sql):
                self.assertFalse(ordered(sql))
        self.assertTrue(
            ordered(
                "WITH t AS (SELECT 1 ORDER BY 1) SELECT * FROM t ORDER /*comment*/ BY 1"
            )
        )


class CLITests(unittest.TestCase):
    def test_scale_factor(self):
        self.assertEqual(substitute_sf("SELECT {{SF}}", 1), "SELECT 1.0")
        self.assertEqual(substitute_sf("SELECT {{SF}}", 0.1), "SELECT 0.1")
        self.assertEqual(substitute_sf("SELECT 1", None), "SELECT 1")
        with self.assertRaisesRegex(ValueError, "--sf"):
            substitute_sf("SELECT {{SF}}", None)

    def test_mix_relative_paths_and_placeholder_fail_fast(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "query.sql").write_text("SELECT {{SF}}")
            (root / "mix.json").write_text(
                '[{"name":"q", "sql_file":"query.sql", "weight":2}]'
            )
            self.assertEqual(
                read_mix(str(root / "mix.json"), 1)[0]["sql"], "SELECT 1.0"
            )
            with self.assertRaisesRegex(ValueError, "--sf"):
                read_mix(str(root / "mix.json"), None)

    def test_signed_execute_limits(self):
        self.assertEqual(int32("-2147483648"), -(2**31))
        self.assertEqual(int32("2147483647"), 2**31 - 1)
        with self.assertRaises(argparse.ArgumentTypeError):
            int32("2147483648")
        args = parser().parse_args(
            [
                "tpch",
                "--database",
                "d",
                "--out",
                "-",
                "--protocol",
                "extended",
                "--max-rows",
                "-2147483648",
            ]
        )
        self.assertEqual(args.max_rows, -(2**31))

    def test_protocol_defaults(self):
        args = parser().parse_args(["tpch", "--database", "d", "--out", "-"])
        self.assertEqual(args.protocol, "simple")
        self.assertEqual(args.max_rows, 0)
        self.assertTrue(args.fetch_result)


class ConsoleTests(unittest.TestCase):
    @staticmethod
    def feed(lines: list[str], exit_code: int) -> list[dict]:
        parser = Parser(keep_values=True)
        outcomes = [parser.line(line, float(i)) for i, line in enumerate(lines)]
        outcomes.append(parser.finish(float(len(lines)), exit_code))
        return [o for o in outcomes if o is not None]

    @staticmethod
    def marker(name: str, iteration: int) -> list[str]:
        return [
            '{"columns":[{"name":"lsbench_marker","type":"Utf8"}]}',
            f'["lsbench-marker:{name}:{iteration}"]',
            "1 row(s) fetched. ",
            "Elapsed 0.001 seconds.",
            "",
        ]

    def test_queries_take_their_own_elapsed_values_and_errors(self):
        lines = (
            ['{"columns":[]}', "0 row(s) fetched. ", "Elapsed 0.002 seconds."]  # USE
            + self.marker("q01", -1)
            + [
                '{"columns":[{"name":"n","type":"Utf8"},{"name":"v","type":"Decimal128(15, 2)"}]}',
                '["Elapsed 9 seconds.\\n4 row(s) fetched.","1.50"]',
                '[null,"2.00"]',
                "2 row(s) fetched. ",
                "Elapsed 1.250 seconds.",
                "",
            ]
            + self.marker("q02", -1)
            + ["● Schema error: No field named x.", "│ Valid fields are a."]
            + self.marker("q03", 0)
            + [
                '{"columns":[{"name":"n","type":"Int64"}]}',
                "0 row(s) fetched. ",
                "Elapsed 0.300 seconds.",
            ]
        )
        q01, q02, q03 = self.feed(lines, 0)
        self.assertEqual(
            (
                q01["query"],
                q01["iteration"],
                q01["status"],
                q01["latency_s"],
                q01["rows"],
            ),
            ("q01", -1, "ok", 1.25, 2),
        )
        self.assertEqual(
            (q01["columns"], q01["types"]), (["n", "v"], ["Utf8", "Decimal128(15, 2)"])
        )
        self.assertEqual(
            q01["values"],
            [("Elapsed 9 seconds.\n4 row(s) fetched.", "1.50"), (None, "2.00")],
        )
        self.assertEqual(
            (q02["query"], q02["status"], q02["latency_s"]), ("q02", "error", None)
        )
        self.assertIn("No field named x", q02["error"])
        self.assertEqual(
            (q03["query"], q03["iteration"], q03["status"], q03["rows"], q03["values"]),
            ("q03", 0, "ok", 0, []),
        )

    def test_answers_use_column_types(self):
        outcome = dict(
            columns=["name", "total"],
            types=["Utf8", "Decimal128(38, 4)"],
            values=[("001", "100.004"), ("b", None)],
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "q.csv"
            path.write_text("name,total\n001,100.00\nb,\n", encoding="utf-8")
            self.assertEqual(check(outcome, path, "SELECT 1"), (True, None))
            path.write_text("name,total\n1,100.00\nb,\n", encoding="utf-8")
            self.assertFalse(check(outcome, path, "SELECT 1")[0])
            path.write_text("name,total\n001,100.02\nb,\n", encoding="utf-8")
            self.assertFalse(check(outcome, path, "SELECT 1")[0])

    def test_killed_console_marks_the_running_query(self):
        lines = self.marker("q01", 0) + ["memory allocation failed"]
        (q01,) = self.feed(lines, -9)
        self.assertEqual(q01["status"], "disconnected")
        self.assertIn("memory allocation failed", q01["error"])
        self.assertIn("code -9", q01["error"])

    def test_script_terminates_statements(self):
        text = script(
            "tpch_sf1", [("q01", "select 1\n"), ("q02", "select 2;\n")], 1, 1, 8
        )
        self.assertEqual(
            text.splitlines()[:2],
            ["USE tpch_sf1;", "SET datafusion.execution.target_partitions = 8;"],
        )
        self.assertEqual(text.count("select 1;"), 2)
        self.assertEqual(text.count("select 2;"), 2)
        self.assertNotIn("SET", script("d", [("q01", "select 1")], 0, 1, None))
        with self.assertRaises(ValueError):
            script("tpch; drop", [("q01", "select 1")], 0, 1, None)

    def test_memory_windows_follow_a_growing_sample_file(self):
        def line(ts: float, anon: int, allocated: int | None, pool: int) -> str:
            return json.dumps(dict(ts=ts, rss_bytes=anon + 10, rss_anon_bytes=anon,
                                   jemalloc_allocated_bytes=allocated, jemalloc_active_bytes=allocated,
                                   jemalloc_resident_bytes=allocated, pool_reserved_bytes=pool)) + "\n"
        with tempfile.TemporaryDirectory() as directory:
            timeline = MemoryTimeline(str(Path(directory) / "memory.jsonl"))
            self.assertIsNone(timeline.window(0.0, 1.0))  # not created yet
            with open(timeline.path, "w", encoding="utf-8") as f:
                f.write(line(1.0, 100, 50, 0) + line(2.0, 300, 90, 40) + line(3.0, 200, None, 80))
                f.write(line(4.0, 999, 999, 999)[:20])  # the sampler is mid-write
                f.flush()
                window = timeline.window(1.5, 3.0)
                self.assertEqual(window["samples"], 2)
                self.assertEqual(window["peak"]["rss_anon_bytes"], 300)
                self.assertEqual(window["peak"]["jemalloc_allocated_bytes"], 90)
                self.assertEqual(window["peak"]["pool_reserved_bytes"], 80)
                self.assertEqual(window["at_rss_anon_peak"]["ts"], 2.0)
                self.assertEqual(window["at_rss_anon_peak"]["pool_reserved_bytes"], 40)
                f.write(line(4.0, 999, 999, 999)[20:])
                f.flush()
                self.assertEqual(timeline.window(3.5, 4.5)["peak"]["rss_anon_bytes"], 999)
                self.assertIsNone(timeline.window(5.0, 6.0))


class PolicyTests(unittest.IsolatedAsyncioTestCase):
    async def test_stall_at_exact_threshold_and_only_once(self):
        policy = ReadPolicy(stall_after=7, stall_seconds=0.001)
        self.assertEqual(await policy.allowance(100), 7)
        policy.received = 7
        self.assertEqual(await policy.allowance(100), 100)
        self.assertTrue(policy.stalled)
        self.assertEqual(await policy.allowance(100), 100)

    async def test_unthrottled_reads_remain_bounded(self):
        policy = ReadPolicy()
        self.assertEqual(await policy.allowance(1024 * 1024), 16384)


if __name__ == "__main__":
    unittest.main()
