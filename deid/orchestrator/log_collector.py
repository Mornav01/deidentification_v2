"""LogCollector — single-writer log aggregation from Redis pub/sub."""
from __future__ import annotations

import json
import logging
import resource
import sys
import time
from pathlib import Path

from deid.config.task_models import BatchFailure, LogLevel, LogRecord

logger = logging.getLogger("deid.orchestrator")


class LogCollector:
    """Receives log records and writes to log file, failures file, and summary."""

    def __init__(self, log_dir: str, run_timestamp: str):
        self._log_dir = Path(log_dir)
        self._log_dir.mkdir(parents=True, exist_ok=True)
        self._run_ts = run_timestamp
        self._prefix = f"deid_{run_timestamp}"

        # Open files for incremental writing.
        self._log_path = self._log_dir / f"{self._prefix}.log"
        self._failures_path = self._log_dir / f"{self._prefix}_failures.jsonl"
        self._summary_path = self._log_dir / f"{self._prefix}_summary.json"

        self._log_fh = open(self._log_path, "a", encoding="utf-8")
        self._failures_fh = None  # opened lazily on first failure

        # Stats accumulators.
        self._table_stats: dict[str, dict] = {}
        self._global_warnings = 0
        self._peak_worker_mb = 0
        self._worker_latest_by_pid: dict[int, int] = {}  # {pid: last reported mb}
        self._peak_concurrent_workers_mb = 0  # peak of sum(latest per pid)
        self._failure_count = 0
        self._stop = False
        self._records_since_summary = 0
        self._run_start = time.monotonic()

    def _ensure_table(self, table: str):
        if table not in self._table_stats:
            self._table_stats[table] = {
                "rows_succeeded": 0,
                "rows_failed": 0,
                "warnings": 0,
                "batches_completed": 0,
                "batches_failed": 0,
                "duration_ms": 0,
                "errors": [],
                "status": "started",
                "_first_seen_time": None,
                "_last_seen_time": None,
            }

    def handle_record(self, raw: dict):
        """Process a single log record dict (from Redis or direct call)."""
        record = LogRecord(**raw) if not isinstance(raw, LogRecord) else raw
        self._ensure_table(record.table)
        ts = self._table_stats[record.table]

        # Track wall time per table (first event → last event).
        now = time.monotonic()
        if ts["_first_seen_time"] is None:
            ts["_first_seen_time"] = now
        ts["_last_seen_time"] = now

        # Write formatted log line.
        line = self._format_line(record)
        self._log_fh.write(line + "\n")
        self._log_fh.flush()

        # Accumulate stats from batch-completion records.
        # Rows are counted only from the "process" phase to avoid 3× inflation
        # (fetch, process, and write tasks each emit rows_succeeded for the same batch).
        if record.phase == "process":
            if record.rows_succeeded is not None:
                ts["rows_succeeded"] += record.rows_succeeded
            if record.rows_failed is not None:
                ts["rows_failed"] += record.rows_failed
        if record.duration_ms is not None:
            ts["duration_ms"] += record.duration_ms

        # Track warnings.
        if record.level == LogLevel.WARNING or record.level == "WARNING":
            ts["warnings"] += 1
            self._global_warnings += 1

        # Track peak worker memory — per PID for accurate concurrent total.
        if record.peak_memory_mb is not None:
            if record.peak_memory_mb > self._peak_worker_mb:
                self._peak_worker_mb = record.peak_memory_mb
            if record.worker_pid is not None:
                self._worker_latest_by_pid[record.worker_pid] = record.peak_memory_mb
                concurrent_total = sum(self._worker_latest_by_pid.values())
                if concurrent_total > self._peak_concurrent_workers_mb:
                    self._peak_concurrent_workers_mb = concurrent_total

        # Handle errors — write to failures file and flush summary immediately.
        if record.level == LogLevel.ERROR or record.level == "ERROR":
            ts["batches_failed"] += 1
            if record.error:
                ts["errors"].append(record.error)
            self._write_failure(record)
            self.write_summary()
            self._records_since_summary = 0
        else:
            if record.batch is not None and record.rows_in_batch is not None:
                ts["batches_completed"] += 1
            # Flush summary every 50 non-error records.
            self._records_since_summary += 1
            if self._records_since_summary % 50 == 0:
                self.write_summary()

    def _export_table_stats(self, ts: dict) -> dict:
        """Return a serialisable copy of a table stats dict with wall/cpu times added."""
        first = ts["_first_seen_time"]
        last  = ts["_last_seen_time"]
        wall_s = round((last - first), 1) if (first is not None and last is not None) else 0.0
        out = {k: v for k, v in ts.items() if not k.startswith("_")}
        out["wall_time_s"] = wall_s
        out["cpu_time_s"]  = round(ts["duration_ms"] / 1000, 1)
        return out

    def _format_line(self, record: LogRecord) -> str:
        level = record.level if isinstance(record.level, str) else record.level.value
        return (
            f"{record.timestamp} | {level:<7} | {record.table:<15} | "
            f"{record.phase:<12} | {record.message}"
        )

    def _write_failure(self, record: LogRecord):
        if self._failures_fh is None:
            self._failures_fh = open(self._failures_path, "a", encoding="utf-8")

        task_type = "range" if record.start_id is not None else "full"
        failure = BatchFailure(
            table=record.table,
            start_id=record.start_id,
            end_id=record.end_id,
            batch=record.batch,
            error=record.error or record.message,
            timestamp=record.timestamp,
            task_type=task_type,
        )
        self._failures_fh.write(failure.model_dump_json() + "\n")
        self._failures_fh.flush()
        self._failure_count += 1

    def get_stats(self) -> dict:
        """Return accumulated stats dict."""
        total_succeeded = sum(t["rows_succeeded"] for t in self._table_stats.values())
        total_failed = sum(t["rows_failed"] for t in self._table_stats.values())
        total_warnings = self._global_warnings

        tables_completed = sum(
            1 for t in self._table_stats.values() if t["batches_failed"] == 0
        )
        tables_failed = sum(
            1 for t in self._table_stats.values() if t["batches_failed"] > 0
        )

        # Orchestrator memory.
        usage = resource.getrusage(resource.RUSAGE_SELF)
        ru_maxrss = usage.ru_maxrss
        if sys.platform == "darwin":
            orch_mb = ru_maxrss // (1024 * 1024)
        else:
            orch_mb = ru_maxrss // 1024

        wall_time_s = round(time.monotonic() - self._run_start, 1)

        return {
            "run_timestamp": self._run_ts,
            "wall_time_s": wall_time_s,
            "totals": {
                "tables": len(self._table_stats),
                "tables_completed": tables_completed,
                "tables_failed": tables_failed,
                "rows_succeeded": total_succeeded,
                "rows_failed": total_failed,
                "warnings": total_warnings,
            },
            "memory": {
                "peak_single_worker_mb": self._peak_worker_mb,
                "peak_concurrent_workers_mb": self._peak_concurrent_workers_mb,
                "worker_count": len(self._worker_latest_by_pid),
                "peak_orchestrator_mb": orch_mb,
                "peak_total_mb": self._peak_concurrent_workers_mb + orch_mb,
            },
            "tables": {
                name: self._export_table_stats(ts)
                for name, ts in self._table_stats.items()
            },
            "log_file": str(self._log_path),
            "failures_file": str(self._failures_path) if self._failure_count > 0 else None,
        }

    def write_summary(self) -> dict:
        """Write JSON summary file and return the stats dict."""
        stats = self.get_stats()
        with open(self._summary_path, "w", encoding="utf-8") as f:
            json.dump(stats, f, indent=2)
        return stats

    def format_text_summary(self) -> str:
        """Generate human-readable text summary."""
        stats = self.get_stats()
        t = stats["totals"]
        m = stats["memory"]

        ws = stats["wall_time_s"]
        if ws >= 3600:
            wall_str = f"{int(ws // 3600)}h {int((ws % 3600) // 60)}m {int(ws % 60)}s"
        elif ws >= 60:
            wall_str = f"{int(ws // 60)}m {int(ws % 60)}s"
        else:
            wall_str = f"{ws}s"

        lines = [
            "",
            "=" * 60,
            f"  Run Summary — {self._run_ts.replace('_', ' ')}",
            "=" * 60,
            f"  Wall time:   {wall_str}",
            f"  Peak Memory: {m['peak_total_mb']} MB total "
            f"(orchestrator: {m['peak_orchestrator_mb']} MB, "
            f"workers: {m['peak_concurrent_workers_mb']} MB across {m['worker_count']} processes)",
            f"  Tables:      {t['tables']} total | {t['tables_completed']} completed | {t['tables_failed']} failed",
            f"  Rows:        {t['rows_succeeded'] + t['rows_failed']:,} processed | "
            f"{t['rows_succeeded']:,} succeeded | {t['rows_failed']:,} failed",
            f"  Warnings:    {t['warnings']}",
            f"  Log file:    {stats['log_file']}",
        ]

        if stats["failures_file"]:
            lines.append(
                f"  Failures:    {stats['failures_file']} ({self._failure_count} entries)"
            )

        # Per-table breakdown.
        lines.append("")
        lines.append(f"  {'Table':<20} {'Status':<10} {'Rows':>10} {'Failed':>8} {'Warnings':>10} {'Wall time':>12} {'CPU time':>12}")
        lines.append(f"  {'-'*20} {'-'*10} {'-'*10} {'-'*8} {'-'*10} {'-'*12} {'-'*12}")

        def _fmt_duration(s: float) -> str:
            if s >= 3600:
                return f"{int(s // 3600)}h {int((s % 3600) // 60)}m {int(s % 60):02d}s"
            if s >= 60:
                return f"{int(s // 60)}m {int(s % 60):02d}s"
            return f"{s:.1f}s"

        for name, ts in stats["tables"].items():
            status = "FAILED" if ts["batches_failed"] > 0 else "OK"
            total_rows = ts["rows_succeeded"] + ts["rows_failed"]
            wall_str = _fmt_duration(ts.get("wall_time_s", 0))
            cpu_str  = _fmt_duration(ts.get("cpu_time_s", ts["duration_ms"] / 1000))
            lines.append(
                f"  {name:<20} {status:<10} {total_rows:>10,} {ts['rows_failed']:>8,} "
                f"{ts['warnings']:>10} {wall_str:>12} {cpu_str:>12}"
            )

        # Failed table details.
        failed_tables = [
            (name, ts) for name, ts in stats["tables"].items()
            if ts["batches_failed"] > 0
        ]
        if failed_tables:
            lines.append("")
            lines.append("  Failed tables:")
            for name, ts in failed_tables:
                for err in ts["errors"]:
                    lines.append(f"    - {name}: {err}")

            lines.append("")
            lines.append("  Retry command:")
            lines.append(
                f"    deid retry --config <config.yaml> "
                f"--failures {stats['failures_file']}"
            )

        lines.append("=" * 60)
        return "\n".join(lines)

    async def listen(self, redis_url: str):
        """Subscribe to deid:logs and process records until stopped."""
        import redis.asyncio as aioredis

        r = aioredis.from_url(redis_url)
        pubsub = r.pubsub()
        await pubsub.subscribe("deid:logs")

        try:
            async for message in pubsub.listen():
                if self._stop:
                    break
                if message["type"] == "message":
                    data = json.loads(message["data"])
                    self.handle_record(data)
        finally:
            await pubsub.unsubscribe("deid:logs")
            await r.aclose()

    def stop(self):
        """Signal the listener to stop."""
        self._stop = True

    def close(self):
        """Close open file handles."""
        if self._log_fh:
            self._log_fh.close()
        if self._failures_fh:
            self._failures_fh.close()
