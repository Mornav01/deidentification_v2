"""Delta-identity QC — cross-environment row-level diff (polars).

Polars port of the standalone ``cdc_id_validation.py``. Runs when delta data arrives (e.g. after a
CDC merge): for each table it compares a **source** environment (e.g. prod) against a **dest**
environment (e.g. CDC-merged staging / local) on a shared row key, and classifies every key as:

- ``missing_in_dest``  — present in source, absent from dest (rows dropped during ingest)
- ``extra_in_dest``    — present in dest, absent from source (rows that shouldn't be there)
- ``value_mismatch``   — present in both, but the business key value differs

This maps to the QC framework's Part 2 "Cross-Environment Row Count Checks", done at the row/value
level rather than just aggregate counts.

Design notes (carried from the MoM):
- The dest side is the one that carries the delta-window column (``nd_extracted_date`` by default),
  so the window is applied to dest, and source is fetched **filtered to the dest key set** — this
  avoids ever scanning the full source table.
- ``value_mismatch`` is only meaningful when dest is a faithful copy of source (prod ↔ staging).
  For prod ↔ de-identified output the business key is replaced, so leave ``biz_key_col`` unset and
  rely on the always-valid presence checks (missing / extra).
- All compared columns are normalized then cast to Utf8 so ``Decimal(14243118)`` and ``14243118``
  compare equal (the original avoided ``astype(str)`` for exactly this reason).
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Optional

import polars as pl
from sqlalchemy import text

from deid.core.dbPkg.dbhandler import NDDBHandler, _normalize_rows

logger = logging.getLogger("deid.qc.delta_identity")

DEFAULT_BIZ_KEY_PATTERNS: tuple[str, ...] = (
    "patientid", "patient_id",
    "encounterid", "encounter_id",
    "invoiceid", "invoice_id",
    "ndid", "psid",
    "claimid", "claim_id",
    "visitid", "visit_id",
    "chartid", "chart_id",
)


@dataclass
class DeltaIdentityConfig:
    tables: list[str]
    id_col: str = "nd_auto_increment_id"
    # Business key compared for value-mismatch. Either set explicitly or leave None to
    # auto-discover from dest columns using ``biz_key_patterns`` (excluding ``id_col``).
    biz_key_col: Optional[str] = None
    biz_key_patterns: tuple[str, ...] = DEFAULT_BIZ_KEY_PATTERNS
    # Delta window on the dest side: only dest rows with ``delta_col > delta_after`` are checked.
    delta_col: str = "nd_extracted_date"
    delta_after: Optional[str] = None
    chunk_size: int = 500          # IN (...) batch size, matches the original
    sample_size: int = 10          # max sample diff rows persisted per issue type

    @classmethod
    def from_dict(cls, d: dict | None) -> "DeltaIdentityConfig":
        d = dict(d or {})
        if "biz_key_patterns" in d and d["biz_key_patterns"] is not None:
            d["biz_key_patterns"] = tuple(d["biz_key_patterns"])
        return cls(**d)


# ── frame loading ──────────────────────────────────────────────────────────────


def _empty_frame(cols: list[str]) -> pl.DataFrame:
    return pl.DataFrame({c: pl.Series(c, [], dtype=pl.Utf8) for c in cols})


def _rows_to_frame(rows, cols: list[str]) -> pl.DataFrame:
    if not rows:
        return _empty_frame(cols)
    frame = pl.DataFrame(
        _normalize_rows(rows),
        schema=cols,
        orient="row",
        infer_schema_length=len(rows),
    )
    # Normalize compared columns to Utf8 so numeric/Decimal vs int/str compare equally.
    return frame.with_columns([pl.col(c).cast(pl.Utf8, strict=False) for c in cols])


def load_id_frame(
    handler: NDDBHandler,
    table: str,
    id_col: str,
    biz_col: Optional[str],
    *,
    id_filter: Optional[list] = None,
    delta_col: Optional[str] = None,
    delta_after: Optional[str] = None,
    chunk_size: int = 500,
) -> pl.DataFrame:
    """Fetch ``id_col`` (and ``biz_col``) from ``table`` into a polars DataFrame.

    - ``id_filter`` not None → fetch only ``WHERE id_col IN (...)`` (chunked); ``[]`` returns empty
      without hitting the DB.
    - ``delta_col`` + ``delta_after`` → fetch only ``WHERE delta_col > :after`` (full-table path).
    """
    cols = [id_col] + ([biz_col] if biz_col else [])
    qi = handler._qi
    col_expr = ", ".join(qi(c) for c in cols)
    nolock = " WITH (NOLOCK)" if handler.engine.dialect.name == "mssql" else ""
    table_ref = qi(table)

    if id_filter is not None:
        if not id_filter:
            return _empty_frame(cols)
        frames: list[pl.DataFrame] = []
        for i in range(0, len(id_filter), chunk_size):
            batch = id_filter[i:i + chunk_size]
            placeholders = ", ".join(f":v{j}" for j in range(len(batch)))
            params = {f"v{j}": v for j, v in enumerate(batch)}
            query = text(
                f"SELECT {col_expr} FROM {table_ref}{nolock} "
                f"WHERE {qi(id_col)} IN ({placeholders})"
            )
            with handler.engine.connect() as conn:
                rows = conn.execute(query, params).fetchall()
            frames.append(_rows_to_frame(rows, cols))
        return pl.concat(frames) if frames else _empty_frame(cols)

    where, params = "", {}
    if delta_col and delta_after:
        where = f"WHERE {qi(delta_col)} > :after"
        params = {"after": delta_after}
    query = text(f"SELECT {col_expr} FROM {table_ref}{nolock} {where}")
    with handler.engine.connect() as conn:
        rows = conn.execute(query, params).fetchall()
    return _rows_to_frame(rows, cols)


def discover_biz_key(handler: NDDBHandler, table: str, id_col: str, patterns: tuple[str, ...]) -> Optional[str]:
    """First column (other than ``id_col``) whose name contains a biz-key pattern, else None."""
    id_lower = id_col.lower()
    for col in handler.get_columns(table):
        name = col["name"]
        if name.lower() == id_lower:
            continue
        if any(p in name.lower() for p in patterns):
            return name
    return None


# ── comparison ───────────────────────────────────────────────────────────────


def compare_frames(source_df: pl.DataFrame, dest_df: pl.DataFrame, id_col: str, biz_col: Optional[str]) -> dict:
    """Full-join source↔dest on ``id_col`` and classify each key.

    Returns counts plus the actual diff frames (for sampling). ``source`` = e.g. prod,
    ``dest`` = e.g. local/staging.
    """
    src = source_df.with_columns(pl.lit(True).alias("_in_source"))
    dst = dest_df.with_columns(pl.lit(True).alias("_in_dest"))
    merged = src.join(dst, on=id_col, how="full", coalesce=True, suffix="_dest")

    missing = merged.filter(pl.col("_in_dest").is_null())     # in source, not dest
    extra = merged.filter(pl.col("_in_source").is_null())     # in dest, not source
    both = merged.filter(pl.col("_in_source").is_not_null() & pl.col("_in_dest").is_not_null())

    if biz_col and both.height:
        src_c, dst_c = biz_col, f"{biz_col}_dest"
        both_null = pl.col(src_c).is_null() & pl.col(dst_c).is_null()
        vals_equal = (pl.col(src_c) == pl.col(dst_c)) | both_null
        mismatch = both.filter(~vals_equal)
        matched = both.height - mismatch.height
    else:
        mismatch = _empty_frame([id_col])
        matched = both.height

    return {
        "missing_in_dest": missing,
        "extra_in_dest": extra,
        "value_mismatch": mismatch,
        "matched": matched,
        "source_count": source_df.height,
        "dest_count": dest_df.height,
    }


def _sample_detail(diff: dict, id_col: str, biz_col: Optional[str], cap: int) -> list[dict]:
    detail: list[dict] = []
    sc, dc = biz_col, (f"{biz_col}_dest" if biz_col else None)

    def _grab(frame: pl.DataFrame, issue: str, src_val_col, dst_val_col):
        for row in frame.head(cap).iter_rows(named=True):
            detail.append({
                "issue": issue,
                "id_value": row.get(id_col),
                "source_val": row.get(src_val_col) if src_val_col else None,
                "dest_val": row.get(dst_val_col) if dst_val_col else None,
            })

    _grab(diff["missing_in_dest"], "missing_in_dest", sc, None)
    _grab(diff["extra_in_dest"], "extra_in_dest", None, dc)
    _grab(diff["value_mismatch"], "value_mismatch", sc, dc)
    return detail


# ── per-table check ────────────────────────────────────────────────────────────


def check_table(source_handler: NDDBHandler, dest_handler: NDDBHandler, table: str, cfg: DeltaIdentityConfig) -> dict:
    """Run the delta-identity diff for one table. Returns a result dict (also the persistence shape)."""
    biz_col = cfg.biz_key_col or discover_biz_key(dest_handler, table, cfg.id_col, cfg.biz_key_patterns)

    # Dest first (delta-windowed), then source filtered to the dest key set.
    dest_df = load_id_frame(
        dest_handler, table, cfg.id_col, biz_col,
        delta_col=cfg.delta_col, delta_after=cfg.delta_after, chunk_size=cfg.chunk_size,
    )
    dest_ids = dest_df.get_column(cfg.id_col).drop_nulls().to_list()
    source_df = load_id_frame(
        source_handler, table, cfg.id_col, biz_col,
        id_filter=dest_ids, chunk_size=cfg.chunk_size,
    )

    diff = compare_frames(source_df, dest_df, cfg.id_col, biz_col)
    n_missing = diff["missing_in_dest"].height
    n_extra = diff["extra_in_dest"].height
    n_mismatch = diff["value_mismatch"].height
    passed = (n_missing == 0 and n_extra == 0 and n_mismatch == 0)

    reason = "" if passed else (
        f"missing_in_dest={n_missing}, extra_in_dest={n_extra}, value_mismatch={n_mismatch}"
    )
    logger.info(
        "[DeltaQC] [%s] %s — source=%d dest=%d matched=%d missing=%d extra=%d mismatch=%d",
        table, "PASS" if passed else "FAIL",
        diff["source_count"], diff["dest_count"], diff["matched"],
        n_missing, n_extra, n_mismatch,
    )
    return {
        "table_name": table,
        "is_qc_passed": passed,
        "id_col": cfg.id_col,
        "biz_key_col": biz_col or "",
        "delta_after": cfg.delta_after or "",
        "source_rows_count": diff["source_count"],
        "dest_rows_count": diff["dest_count"],
        "matched_count": diff["matched"],
        "missing_in_dest": n_missing,
        "extra_in_dest": n_extra,
        "value_mismatch": n_mismatch,
        "sample_detail": _sample_detail(diff, cfg.id_col, biz_col, cfg.sample_size),
        "reason": reason,
    }


# ── entrypoint (Trigger A: callable from the CDC flow) ──────────────────────────


def run_delta_identity_qc(
    source_conn_str: str,
    dest_conn_str: str,
    cfg: DeltaIdentityConfig,
    qc_results_db_url: str = "",
) -> list[dict]:
    """Run the delta-identity QC across ``cfg.tables`` and (optionally) persist results.

    Intended to be called at the end of a CDC merge. ``source`` is opened read-only.
    Returns one result dict per table.
    """
    assert cfg.tables, "cfg.tables must not be empty"
    source_handler = NDDBHandler(source_conn_str, read_only=True)
    dest_handler = NDDBHandler(dest_conn_str)
    try:
        results = []
        for table in cfg.tables:
            try:
                results.append(check_table(source_handler, dest_handler, table, cfg))
            except Exception as exc:  # one bad table must not abort the rest
                logger.exception("[DeltaQC] [%s] errored: %s", table, exc)
                results.append({
                    "table_name": table, "is_qc_passed": False, "id_col": cfg.id_col,
                    "biz_key_col": cfg.biz_key_col or "", "delta_after": cfg.delta_after or "",
                    "source_rows_count": 0, "dest_rows_count": 0, "matched_count": 0,
                    "missing_in_dest": 0, "extra_in_dest": 0, "value_mismatch": 0,
                    "sample_detail": [], "reason": f"error: {exc}",
                })
    finally:
        source_handler.close()
        dest_handler.close()

    if qc_results_db_url:
        _persist_results(qc_results_db_url, results)
    return results


def _persist_results(db_url: str, results: list[dict]) -> None:
    from sqlalchemy.orm import Session

    from deid.models.base import create_qc_results_engine, create_all_qc_results_tables
    from deid.models.qc_results import QCDeltaIdentityResult

    engine = create_qc_results_engine(db_url)
    create_all_qc_results_tables(engine)
    with Session(engine) as session:
        for r in results:
            session.add(QCDeltaIdentityResult(
                table_name=r["table_name"],
                is_qc_passed=r["is_qc_passed"],
                id_col=r["id_col"],
                biz_key_col=r["biz_key_col"],
                delta_after=r["delta_after"],
                source_rows_count=r["source_rows_count"],
                dest_rows_count=r["dest_rows_count"],
                matched_count=r["matched_count"],
                missing_in_dest=r["missing_in_dest"],
                extra_in_dest=r["extra_in_dest"],
                value_mismatch=r["value_mismatch"],
                sample_detail=json.dumps(r["sample_detail"], default=str),
                reason=r["reason"],
            ))
        session.commit()
    engine.dispose()
    logger.info("[DeltaQC] Persisted %d table result(s) to %s", len(results), db_url)
