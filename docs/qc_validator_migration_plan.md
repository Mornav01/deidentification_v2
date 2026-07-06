# Auto QC — QC Framework coverage & build plan

**Source of truth:** `QC_Framework_Proposition.pdf` (the MoM). Goal: the deid tool's auto QC must
cover **everything** in that document — all three parts — even where `phi-deid-validator` never did.
We are free to rewrite. The standalone Streamlit validator is retired once Parts 1–3 are covered here.
All work lives on branch `auto_qc`, implemented on polars to match the pipeline stack.

Legend: ✅ covered · 🟡 partial · ❌ missing.  "Target" = where it lands in `deidentification_v2`.

---

## Part 1 — Structured Column Checks (during/after batch insertion, per-row)

| Doc check | Target |
|---|---|
| **ID** length (`PATIENT_ID`,`ENCOUNTER_ID`,`REFERENCE_PID`,`APPOINTMENT_ID`,`CHART_ID`) | ✅ `qc/builders/structured.py` — existing PATIENT/ENCOUNTER/REFERENCE detectors + new `SAppointmentIdDetector`/`SChartIdDetector` (were missing → `KeyError`). Exact length + prefix (Decision D1). |
| **DATE** format `YYYY-MM-DD` + plausibility `[1900, today]` | ✅ format+plausibility added to `SDateOffestDetector`/`SStaticOffestDetector` (keeps offset-applied check too). |
| **TRUNCATE** `ZIP_CODE` len = 3 | ✅ `SZipCodeDetector` tightened `≤3` → `==3` (D2). |
| **TRUNCATE** `PATIENT_DOB` year-only | ✅ `SDobDetector`. |
| **REPLACE** `MASK` | ✅ `SMaskDetector` (`<<mask>>`, D1). |
| **NOTES/GENERIC_NOTES** → Part 3 | ✅ routed. |
| Failure action: log row id + column + value/length | ✅ ID detectors capture offending `nd_auto_increment_id` in `remarks.offenders`. |

## Part 2 — Mapping & Count Checks (pre-pipeline, **BLOCKING**)

| Doc check | Target |
|---|---|
| Mapping ↔ target table row-count equality | ✅ `qc/mapping_count.py::check_mapping_to_table_counts`. |
| Patient → distinct-encounter count (source vs dest) | ✅ `check_patient_encounter_counts` (identity-agnostic distribution match). |
| Encounter-based row counts across FK tables | ✅ `check_encounter_row_counts`. |
| Mapping 1:1 uniqueness (patient / encounter) | ✅ `check_mapping_uniqueness`. |
| Patient offset within `[-38, 38]` | ✅ `check_offset_range`. |
| Mapping ID format (len/prefix) | ✅ `check_mapping_id_format`. |
| Row-level cross-env identity diff (missing/extra/value-mismatch) | ✅ `qc/delta_identity.py` (polars port of `cdc_id_validation.py`). |
| **Blocking pre-run gate** (halt before pipeline) | ✅ `cli/run.py::_run_part2_gate` → `mapping_count.run_pre_pipeline_gate` (raises `Part2Blocked`); config `qc.part2` / `qc.part2_blocking`. |

## Part 3 — Unstructured Data Audit (post-pipeline, **master-referenced**)

| Doc check | Target |
|---|---|
| PHI master reference (independent ground truth) | ✅ `qc/master_phi.py::make_pii_loader` (via `PIITable`); `scanner.get_pii_info()` now loads master PHI when `qc.pii_master_conn_str` set (was a no-op `{}`). |
| PHI presence scan (case-insensitive) + partial-name | ✅ `scan_phi_presence`. |
| Masking outcomes: phone / 5-digit ZIP / URL / facility / surrogate present | ✅ `scan_masking_outcomes` + surrogate check in `run_master_phi_audit`. |
| Coverage check (count parity + NULL/empty) | ✅ `qc/coverage.py::check_coverage`. |
| Audit report (timestamp, totals, pass/fail, failure detail, coverage gaps) | ✅ `qc/report.py::build_audit_report` + `render_markdown`. |
| Failure action: quarantine failed records | ✅ `QCUnstructuredAuditResult.failure_detail` = quarantine list, persisted for release gating. |

---

## Module layout (`deid/qc/`)
`delta_identity.py`, `mapping_count.py`, `master_phi.py`, `coverage.py`, `report.py` (new);
`builders/structured.py` + `builders/__init__.py`, `scanner.py` (extended). Results models in
`deid/models/qc_results.py`: `QCDeltaIdentityResult`, `QCPart2Result`, `QCUnstructuredAuditResult`.
Commands: `deid qc` (Part 1/3 in-pipeline scan), `deid qc-delta` (`cli/qc_delta.py`, Part 2 row-level;
also opt-in CDC post-merge hook in `CDC/MySQL/cdc_merge.py` via `DEID_CDC_DELTA_QC=1`, fail-soft), and
`deid qc-audit` (`cli/qc_audit.py`, Part 3 master-referenced audit, config `qc.master_phi`). All
registered in `cli/app.py`.

## Decisions
- **D1** QC validates real pipeline output: ID exact length+prefix (not doc `<14`); MASK `<<mask>>` (not `== column_name`).
- **D2** ZIP tightened `≤3` → `==3` for non-null.
- **D3** Part 2 runs as a blocking pre-pipeline gate (config-gated via `qc.part2_blocking`).

## Post-review decisions (2026-07 discussion)
- **Part 1 timing:** post-hoc **sampled** scan is accepted (not injected at batch insertion). Deliberate deviation.
- **Alerting:** handled by the **Airflow** pipeline that runs the QC tasks — no in-code alerting. Every QC
  task is therefore an importable function in `deid/qc/api.py` (`run_part2_from_config`,
  `run_delta_identity_from_config`, `run_master_phi_from_config`, `build_audit_report`) that returns a
  structured result and never exits the process; the CLI commands are thin wrappers over these.
- **P2 done — Part 3 masking-outcome completeness:** `master_phi.py` now also flags residual full
  **addresses**, **dates-in-notes** (raw master-date leak + implausible dates), and — opt-in via
  `require_mask_token`/`expected_mask_tokens` — **mask-token-present**.
- **Presidio removed project-wide → pluggable residual-PII scanner** (`deid/qc/llm_scan.py`):
  `auto` (default) | `regex` | `mlx` (local LLM via `mlx-lm`, **Apple Silicon only**) | `none`.
  `auto` picks mlx on Apple Silicon when installed, else regex — one shared config for the mixed
  Apple/Windows fleet. Wired into the Part-1 unstructured detector and, opt-in, the Part-3 audit
  (`MasterPhiConfig.residual_pii_backend`). mlx is an optional extra (`pip install -e '.[mlx]'`),
  **not** in `requirements.txt` (no Linux/Windows wheels); the scanner fails open to regex if mlx is
  absent. Presidio was not actually imported by the de-id engine either (docs were stale), so
  `presidio-analyzer`/`presidio-anonymizer` were dropped from `requirements.txt` entirely.
- **Parked:** auto-discovery of encounter-FK tables (Part 2) and quarantine *enforcement* (currently
  recorded only); real-DB end-to-end test (owner will run).

## Known follow-ups
- In-pipeline `get_pii_info` uses a flat (cross-patient) PHI set; `master_phi.py` is the precise
  per-patient audit. Consider making the in-pipeline detector per-patient too.
- Mapping↔table / per-patient / per-encounter checks assume an already-populated dest (incremental);
  a fresh first run should enable only source+mapping checks (uniqueness / offset / id-format).

## Tests
`tests/test_qc_framework.py` — 10 tests across Parts 1–3 (all green).
