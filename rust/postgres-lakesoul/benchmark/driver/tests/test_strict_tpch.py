"""Regression tests for the strict TPC-H gate's validation and accounting.

Only the runner's own input validation, response classification, comparison
verdict, and exit accounting are exercised here. The permissive CSV normalizer
and ORDER BY tie rules themselves stay covered by test_driver.py.
"""
from __future__ import annotations

import argparse
import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from lsbench.strict_tpch import (
    Outcome,
    QuerySpec,
    judge,
    main,
    positive_float,
    render_scale,
    select_queries,
    summarize,
)
from lsbench.wire import Result


class StrictInputTests(unittest.TestCase):
    def test_scale_rendering_is_finite_and_leaves_no_placeholder(self):
        for scale, literal in (("0.1", "0.1"), ("1", "1.0"), ("10", "10.0")):
            rendered = render_scale("SELECT {{SF}} AS sf", scale)
            self.assertNotIn("{{", rendered)
            self.assertIn(literal, rendered)
        self.assertEqual(render_scale("SELECT 1", "0.1"), "SELECT 1")
        with self.assertRaises(ValueError):
            render_scale("SELECT '{{OTHER}}'", "0.1")
        with self.assertRaises(ValueError):
            render_scale("SELECT {{SF}}", "NaN")

    def test_selection_rejects_duplicates_and_unknown_names(self):
        with self.assertRaises(ValueError):
            select_queries(["q01", "q01"])
        with self.assertRaises(ValueError):
            select_queries(["q99"])
        self.assertEqual(select_queries(["q02", "q01"]), ["q01", "q02"])



class StrictVerdictTests(unittest.TestCase):

    def test_sql_error_result_fails_even_with_a_complete_response(self):
        spec = QuerySpec(name="q01", sql="SELECT 1", answer=Path("unused.csv"))
        result = Result(columns=["n"], type_oids=[23], values=[], rows=0,
                        rowdesc_s=0.1, ready_at=0.2, command="SELECT 0",
                        sqlstate="XX000", error="boom")
        outcome = judge(spec, result)
        self.assertEqual(outcome.status, "FAIL")
        self.assertEqual(outcome.stage, "server_error")
        self.assertEqual(outcome.sqlstate, "XX000")
        self.assertTrue(outcome.complete)

    def test_partial_response_and_bad_accounting_fail(self):
        spec = QuerySpec(name="q01", sql="SELECT 1", answer=Path("unused.csv"))
        incomplete = Result(columns=["n"], type_oids=[23], values=[("1",)],
                            rows=1, rowdesc_s=0.1, ready_at=None,
                            command="SELECT 1")
        self.assertEqual(judge(spec, incomplete).stage, "incomplete")

        lost_row = Result(columns=["n"], type_oids=[23], values=[("1",)],
                          rows=2, rowdesc_s=0.1, ready_at=0.2,
                          command="SELECT 2")
        self.assertEqual(judge(spec, lost_row).stage, "accounting")

        bad_tag = Result(columns=["n"], type_oids=[23], values=[("1",)],
                         rows=1, rowdesc_s=0.1, ready_at=0.2,
                         command="SELECT 7")
        self.assertEqual(judge(spec, bad_tag).stage, "accounting")

    def test_server_statement_timeout_is_an_incomplete_execution(self):
        spec = QuerySpec(name="q01", sql="SELECT 1", answer=Path("unused.csv"))
        result = Result(columns=[], type_oids=[], values=[], rows=0,
                        rowdesc_s=None, ready_at=0.2, command=None,
                        sqlstate="57014",
                        error="canceling statement due to statement timeout")
        outcome = judge(spec, result)
        self.assertEqual(outcome.status, "FAIL")
        self.assertEqual(outcome.stage, "timeout")
        self.assertTrue(outcome.complete)
        summary = summarize("0.1", "pk", "simple", ["q01"], [outcome])
        self.assertEqual(summary["category"], "incomplete")
        self.assertEqual(summary["cause"], "timeout")



class StrictSummaryTests(unittest.TestCase):
    def test_summary_fails_when_a_query_errors_despite_other_passes(self):
        passed = Outcome(name="q01", status="PASS", attempted=True, complete=True)
        failed = Outcome(name="q02", status="FAIL", stage="server_error",
                         sqlstate="XX000", attempted=True, complete=True)
        summary = summarize("0.1", "pk", "simple", ["q01", "q02"],
                            [passed, failed])
        self.assertFalse(summary["ok"])
        self.assertEqual(summary["expected"], 2)
        self.assertEqual(summary["attempted"], 2)
        self.assertEqual(summary["completed"], 2)
        self.assertEqual(summary["matched"], 1)
        self.assertEqual(summary["category"], "query")
        self.assertEqual(summary["cause"], "server_error")
        self.assertIn("server_error", summary["causes"])
        self.assertIn("query", summary["failure_categories"])

    def test_summary_marks_setup_failure_with_a_typed_cause(self):
        summary = summarize("0.1", "pk", "simple", ["q01"], [],
                            error="connect failed", category="setup",
                            cause="connect_failed")
        self.assertFalse(summary["ok"])
        self.assertEqual(summary["category"], "setup")
        self.assertEqual(summary["cause"], "connect_failed")
        self.assertEqual(summary["causes"], ["connect_failed"])
        self.assertEqual(summary["attempted"], 0)
        self.assertEqual(summary["unattempted"], 1)

    def test_summary_fails_for_an_unattempted_selected_query(self):
        passed = Outcome(name="q01", status="PASS", attempted=True, complete=True)
        skipped = Outcome(name="q02", status="SKIPPED", stage="unattempted")
        summary = summarize("0.1", "pk", "simple", ["q01", "q02"],
                            [passed, skipped])
        self.assertFalse(summary["ok"])
        self.assertEqual(summary["attempted"], 1)
        self.assertEqual(summary["unattempted"], 1)
        self.assertEqual(summary["category"], "incomplete")

    def test_duplicate_execution_cannot_replace_a_selected_query(self):
        first = Outcome(name="q01", status="PASS", attempted=True, complete=True)
        duplicate = Outcome(name="q01", status="PASS", attempted=True, complete=True)
        summary = summarize("0.1", "pk", "simple", ["q01", "q02"],
                            [first, duplicate])
        self.assertFalse(summary["ok"])
        self.assertEqual(summary["category"], "incomplete")
        self.assertIn("selection_accounting", summary["causes"])

    def test_summary_fails_for_a_partial_response(self):
        passed = Outcome(name="q01", status="PASS", attempted=True, complete=True)
        partial = Outcome(name="q02", status="FAIL", stage="timeout",
                          attempted=True, complete=False)
        summary = summarize("0.1", "pk", "simple", ["q01", "q02"],
                            [passed, partial])
        self.assertFalse(summary["ok"])
        self.assertEqual(summary["attempted"], 2)
        self.assertEqual(summary["completed"], 1)
        self.assertEqual(summary["matched"], 1)
        self.assertEqual(summary["category"], "incomplete")
        self.assertEqual(summary["cause"], "timeout")


class StrictCLITests(unittest.TestCase):
    def test_missing_inputs_exit_nonzero_before_connecting(self):
        with tempfile.TemporaryDirectory() as directory:
            queries = Path(directory) / "queries"
            answers = Path(directory) / "answers"
            queries.mkdir()
            answers.mkdir()
            stdout, stderr = io.StringIO(), io.StringIO()
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                code = main(["--port", "1", "--database", "unused",
                             "--queries", "q01",
                             "--queries-dir", str(queries),
                             "--answers-dir", str(answers), "--json"])
            self.assertNotEqual(code, 0)
            payload = json.loads(stdout.getvalue())
            self.assertFalse(payload["ok"])
            self.assertEqual(payload["expected"], 1)
            self.assertEqual(payload["attempted"], 0)
            self.assertEqual(payload["completed"], 0)
            self.assertEqual(payload["matched"], 0)
            self.assertEqual(payload["unmatched"], 1)
            self.assertEqual(payload["category"], "preflight")
            self.assertIn("missing_answer", payload["causes"])

    def test_query_timeout_rejects_non_positive_and_non_finite_values(self):
        for value in ("0", "-1", "nan", "inf"):
            with self.assertRaises(argparse.ArgumentTypeError):
                positive_float(value)


if __name__ == "__main__":
    unittest.main()
