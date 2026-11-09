from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

ERROR = "error"
WARN = "warn"


@dataclass
class CheckResult:
    table: str
    check: str
    severity: str
    passed: bool
    failed_rows: int
    details: str = ""


@dataclass
class Check:
    name: str
    severity: str
    run: Callable[[DataFrame], tuple[bool, int, str]]


def not_null(*cols: str, severity: str = ERROR) -> Check:
    def run(df):
        cond = F.lit(False)
        for c in cols:
            cond = cond | F.col(c).isNull()
        n = df.filter(cond).count()
        return n == 0, n, ""

    return Check(f"not_null({', '.join(cols)})", severity, run)


def unique(*cols: str, severity: str = ERROR) -> Check:
    def run(df):
        n = df.groupBy(*cols).count().filter("count > 1").count()
        return n == 0, n, "duplicate keys"

    return Check(f"unique({', '.join(cols)})", severity, run)


def expression(name: str, condition: Column, severity: str = ERROR) -> Check:
    """Every row has to satisfy `condition` (nulls count as failures)."""

    def run(df):
        n = df.filter(~F.coalesce(condition, F.lit(False))).count()
        return n == 0, n, ""

    return Check(name, severity, run)


def max_ratio(name: str, condition: Column, ratio: float, severity: str = WARN) -> Check:
    """At most `ratio` of rows may match `condition`."""

    def run(df):
        total = df.count()
        bad = df.filter(condition).count()
        actual = bad / total if total else 0.0
        return actual <= ratio, bad, f"{actual:.2%} (max {ratio:.2%})"

    return Check(name, severity, run)


def min_rows(n: int, severity: str = ERROR) -> Check:
    def run(df):
        count = df.count()
        return count >= n, 0, f"{count} rows"

    return Check(f"min_rows({n})", severity, run)


def fresh(col: str, max_age: timedelta, severity: str = WARN) -> Check:
    def run(df):
        latest = df.agg(F.max(col)).first()[0]
        if latest is None:
            return False, 0, "empty"
        age = datetime.now(UTC).replace(tzinfo=None) - latest
        return age <= max_age, 0, f"latest {latest:%Y-%m-%d %H:%M}, age {age}"

    return Check(f"fresh({col}, {max_age})", severity, run)


def run_checks(table: str, df: DataFrame, checks: list[Check]) -> list[CheckResult]:
    results = []
    for check in checks:
        passed, failed_rows, details = check.run(df)
        results.append(CheckResult(table, check.name, check.severity, passed, failed_rows, details))
    return results


def results_df(spark: SparkSession, run_id: str, results: list[CheckResult]) -> DataFrame:
    ts = datetime.now(UTC).replace(tzinfo=None)
    rows = [
        (run_id, ts, r.table, r.check, r.severity, r.passed, r.failed_rows, r.details)
        for r in results
    ]
    return spark.createDataFrame(
        rows,
        "run_id string, checked_at timestamp, table string, check string, severity string, "
        "passed boolean, failed_rows long, details string",
    )
