"""Part 3 §4 — Coverage check.

Verifies that every unstructured record that should have been de-identified actually was:
- record count in the de-identified table == count in source (no skipped records), and
- no NULL/empty values in expected-text columns (flagged for review).

Cross-references the Part-2 encounter row counts conceptually, but is cheap to run standalone.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field

from sqlalchemy import text

from deid.core.dbPkg.dbhandler import NDDBHandler

logger = logging.getLogger("deid.qc.coverage")


@dataclass
class CoverageConfig:
    table: str
    text_cols: list[str] = field(default_factory=list)


def _scalar(handler: NDDBHandler, sql: str) -> int:
    with handler.engine.connect() as conn:
        row = conn.execute(text(sql)).fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def check_coverage(source_conn_str: str, dest_conn_str: str, cfg: CoverageConfig) -> dict:
    source = NDDBHandler(source_conn_str, read_only=True)
    dest = NDDBHandler(dest_conn_str)
    try:
        qi = dest._qi
        src_count = source.get_exact_row_count(cfg.table)
        dst_count = dest.get_exact_row_count(cfg.table)
        null_gaps = {}
        for col in cfg.text_cols:
            sql = (
                f"SELECT COUNT(*) FROM {qi(cfg.table)} "
                f"WHERE {qi(col)} IS NULL OR {qi(col)} = ''"
            )
            null_gaps[col] = _scalar(dest, sql)
    finally:
        source.close()
        dest.close()

    total_null = sum(null_gaps.values())
    passed = (src_count == dst_count) and total_null == 0
    report = {
        "table_name": cfg.table,
        "source_rows_count": src_count,
        "dest_rows_count": dst_count,
        "count_match": src_count == dst_count,
        "null_or_empty_by_col": null_gaps,
        "coverage_gaps": abs(src_count - dst_count) + total_null,
        "passed": passed,
    }
    logger.info(
        "[Part3/coverage] %s: source=%d dest=%d null/empty=%d passed=%s",
        cfg.table, src_count, dst_count, total_null, passed,
    )
    return report
