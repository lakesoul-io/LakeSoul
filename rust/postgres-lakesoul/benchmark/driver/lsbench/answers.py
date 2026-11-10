"""TPC-H text answer comparison, including unordered duplicate rows."""

from __future__ import annotations

import csv
import re
from collections import Counter, defaultdict
from decimal import Decimal, InvalidOperation
from pathlib import Path

from .wire import Result

NUMERIC_OIDS = {20, 21, 23, 26, 700, 701, 1700}
ABS_TOL = Decimal("0.01")
REL_TOL = Decimal("0.000001")


def order_by(sql: str) -> int | None:
    """Offset just past a top-level ORDER BY, excluding literals, identifiers
    and comments; None without one."""
    pos, depth = 0, 0
    previous = ""
    while pos < len(sql):
        if sql.startswith("--", pos):
            end = sql.find("\n", pos + 2)
            pos = len(sql) if end < 0 else end + 1
        elif sql.startswith("/*", pos):
            nesting = 1
            pos += 2
            while pos < len(sql) and nesting:
                if sql.startswith("/*", pos):
                    nesting += 1
                    pos += 2
                elif sql.startswith("*/", pos):
                    nesting -= 1
                    pos += 2
                else:
                    pos += 1
        elif sql[pos] in "'\"":
            quote = sql[pos]
            pos += 1
            while pos < len(sql):
                if sql[pos] == quote:
                    pos += 1
                    if pos < len(sql) and sql[pos] == quote:
                        pos += 1
                        continue
                    break
                if sql[pos] == "\\" and quote == "'":
                    pos += 1
                pos += 1
            previous = ""
        elif sql[pos] == "$" and (
            match := re.match(r"\$(?:[A-Za-z_][A-Za-z_0-9]*)?\$", sql[pos:])
        ):
            delimiter = match.group()
            end = sql.find(delimiter, pos + len(delimiter))
            pos = len(sql) if end < 0 else end + len(delimiter)
            previous = ""
        elif sql[pos] == "(":
            depth += 1
            pos += 1
            previous = ""
        elif sql[pos] == ")":
            depth -= 1
            pos += 1
            previous = ""
        elif sql[pos].isalpha() or sql[pos] == "_":
            match = re.match(r"[A-Za-z_][A-Za-z_0-9$]*", sql[pos:])
            assert match is not None
            word = match.group().lower()
            if depth == 0 and previous == "order" and word == "by":
                return pos + len(match.group())
            previous = word if depth == 0 else ""
            pos += len(match.group())
        else:
            if not sql[pos].isspace():
                previous = ""
            pos += 1
    return None


def ordered(sql: str) -> bool:
    return order_by(sql) is not None


SORT_KEY = re.compile(
    r"\s*([A-Za-z_][A-Za-z_0-9]*)(?:\s+(?:asc|desc))?(?:\s+nulls\s+(?:first|last))?\s*",
    re.IGNORECASE,
)


def sort_columns(sql: str, start: int, columns: list[str]) -> list[int] | None:
    """Result positions of the ORDER BY keys starting at `start`, or None
    unless every key is a bare name of exactly one output column."""
    clause = re.split(
        r"\b(?:limit|offset|fetch)\b|;", sql[start:], maxsplit=1, flags=re.IGNORECASE
    )[0]
    names = [column.lower() for column in columns]
    positions = []
    for item in clause.split(","):
        match = SORT_KEY.fullmatch(item)
        if match is None or names.count(match.group(1).lower()) != 1:
            return None
        positions.append(names.index(match.group(1).lower()))
    return positions


def normalize(row, numeric):
    normalized = []
    for index, value in enumerate(row):
        if value is None or value == "":
            normalized.append(None)
        elif index in numeric:
            try:
                normalized.append(Decimal(value.strip()))
            except InvalidOperation:
                normalized.append(value.rstrip(" "))
        else:
            normalized.append(value.rstrip(" "))
    return tuple(normalized)


def cells_equal(left, right) -> bool:
    if isinstance(left, Decimal) and isinstance(right, Decimal):
        if left.is_nan() or right.is_nan():
            return left.is_nan() and right.is_nan()
        if left == right:
            return True
        if not left.is_finite() or not right.is_finite():
            return False
        delta = abs(left - right)
        return delta <= ABS_TOL or delta <= REL_TOL * abs(right)
    return left == right


def rows_equal(left, right) -> bool:
    return len(left) == len(right) and all(
        cells_equal(a, b) for a, b in zip(left, right)
    )


def compare(result: Result, answer: Path, sql: str) -> tuple[bool, str | None]:
    with answer.open(newline="", encoding="utf-8") as stream:
        reader = csv.reader(stream)
        header = next(reader, None)
        if header is None:
            return False, "Answer CSV is empty (missing header)"
        expected = list(reader)
    if len(header) != len(result.columns):
        return (
            False,
            f"Column count: actual={len(result.columns)}, expected={len(header)}",
        )
    if len(expected) != result.rows:
        return False, f"Row count: actual={result.rows}, expected={len(expected)}"
    if any(len(row) != len(header) for row in expected):
        return False, "Answer CSV contains a row with the wrong column count"
    numeric = {
        index for index, oid in enumerate(result.type_oids) if oid in NUMERIC_OIDS
    }
    actual_rows = [normalize(row, numeric) for row in result.values]
    expected_rows = [normalize(row, numeric) for row in expected]
    if (start := order_by(sql)) is not None:
        keys = sort_columns(sql, start, result.columns)
        # SQL leaves the order of rows with equal sort keys unspecified. When
        # the keys are known output columns, each run of expected rows with
        # equal keys must match the same positions as a multiset; otherwise
        # every row must match in order.
        pos = 0
        while pos < len(expected_rows):
            end = pos + 1
            if keys is not None:
                key = [expected_rows[pos][k] for k in keys]
                while (
                    end < len(expected_rows)
                    and [expected_rows[end][k] for k in keys] == key
                ):
                    end += 1
            if end - pos == 1:
                if not rows_equal(actual_rows[pos], expected_rows[pos]):
                    return (
                        False,
                        f"Ordered row {pos + 1}: actual={actual_rows[pos]!r}, expected={expected_rows[pos]!r}"[
                            :400
                        ],
                    )
            elif (
                mismatch := unmatched(
                    actual_rows[pos:end], expected_rows[pos:end], numeric
                )
            ) is not None:
                return (
                    False,
                    f"Ordered rows {pos + 1}-{end} with equal sort keys: {mismatch}"[
                        :400
                    ],
                )
            pos = end
        return True, None
    mismatch = unmatched(actual_rows, expected_rows, numeric)
    return mismatch is None, mismatch


def unmatched(
    actual_rows: list[tuple], expected_rows: list[tuple], numeric: set[int]
) -> str | None:
    """First row of a multiset difference under numeric tolerance, or None."""
    # Remove exact matches first; most large answer sets need no fuzzy matching.
    actual_counts, expected_counts = Counter(actual_rows), Counter(expected_rows)
    common = actual_counts & expected_counts
    actual_counts.subtract(common)
    expected_counts.subtract(common)
    groups = defaultdict(lambda: ([], []))
    for side, counts in enumerate((actual_counts, expected_counts)):
        for row, count in counts.items():
            if count > 0:
                key = tuple(value for i, value in enumerate(row) if i not in numeric)
                groups[key][side].extend([row] * count)
    for actual, reference in groups.values():
        if len(actual) != len(reference):
            return f"Unmatched multiset row: {(actual or reference)[0]!r}"[:400]
        # Iterative augmenting paths ensure duplicates/overlapping tolerances do
        # not produce the false negatives of a greedy multiset comparison.
        owners: dict[int, int] = {}
        for root in range(len(actual)):
            pending = [root]
            previous = {root: None}
            found = None
            while pending and found is None:
                left = pending.pop()
                for right, expected_row in enumerate(reference):
                    if not rows_equal(actual[left], expected_row):
                        continue
                    owner = owners.get(right)
                    if owner is None:
                        found = (left, right)
                        break
                    if owner not in previous:
                        previous[owner] = (left, right)
                        pending.append(owner)
            if found is None:
                return f"Unmatched multiset row: {actual[root]!r}"[:400]
            left, right = found
            while True:
                owners[right] = left
                link = previous[left]
                if link is None:
                    break
                left, right = link
    return None
