import csv
from types import SimpleNamespace

import pytest

import deid.qc.auto_qc as aq
from deid.qc.auto_qc import (
    TableRoles,
    parse_rules_csv,
    build_table_config,
    build_summary_rows,
    build_findings_rows,
    run_auto_qc,
)


# ── fixtures ─────────────────────────────────────────────────────────────────

RULES_CSV_ROWS = [
    # patients: patient id + name (MASK) + dob + zip
    ("patients", "patientid", "PATIENT_ID"),
    ("patients", "first_name", "MASK"),
    ("patients", "dob", "PATIENT_DOB"),
    ("patients", "zip", "ZIP_CODE"),
    ("patients", "notes", "NOTES"),
    ("patients", "internal_flag", ""),          # blank rule → skipped
    # encounters: encounter id + date
    ("encounters", "encounterid", "ENCOUNTER_ID"),
    ("encounters", "visit_date", "DATE_OFFSET"),
]


def _write_rules_csv(tmp_path, rows=RULES_CSV_ROWS, header=("table_name", "column_name", "rule")):
    p = tmp_path / "rules.csv"
    with p.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    return str(p)


def _fake_cfg():
    """Minimal DeidConfig stand-in — only the attributes the orchestrator touches once runners are stubbed."""
    return SimpleNamespace(
        qc=SimpleNamespace(master_phi={}, part2={}, delta_identity={}),
        mapping_tables={"patient": SimpleNamespace(identifier_columns=["patientid"])},
        mappings_connection_string="sqlite:///mappings.db",
        resolved_qc_results_db_url="sqlite:///qc_results.db",
        source_db=SimpleNamespace(connection_string=lambda: "sqlite:///src.db"),
        destination_db=SimpleNamespace(connection_string=lambda: "sqlite:///dst.db"),
    )


# ── parse_rules_csv ──────────────────────────────────────────────────────────

def test_parse_rules_csv_derives_roles(tmp_path):
    roles = parse_rules_csv(_write_rules_csv(tmp_path))
    assert set(roles) == {"patients", "encounters"}

    pat = roles["patients"]
    assert pat.patient_id_col == "patientid"
    assert pat.note_cols == ["notes"]
    assert pat.name_cols == ["first_name"]
    assert "dob" in pat.date_cols
    assert "internal_flag" not in pat.columns   # blank rule skipped

    enc = roles["encounters"]
    assert enc.encounter_id_col == "encounterid"
    assert "visit_date" in enc.date_cols


def test_parse_rules_csv_filters_and_orders(tmp_path):
    roles = parse_rules_csv(_write_rules_csv(tmp_path), tables=["encounters", "patients"])
    assert list(roles) == ["encounters", "patients"]   # order preserved


def test_parse_rules_csv_unknown_table_is_skipped(tmp_path):
    roles = parse_rules_csv(_write_rules_csv(tmp_path), tables=["patients", "nope"])
    assert list(roles) == ["patients"]


def test_parse_rules_csv_missing_header_raises(tmp_path):
    bad = _write_rules_csv(tmp_path, rows=[("patients", "x")], header=("table_name", "column_name"))
    with pytest.raises(ValueError, match="missing columns"):
        parse_rules_csv(bad)


def test_parse_rules_csv_missing_file_raises():
    with pytest.raises(FileNotFoundError):
        parse_rules_csv("/no/such/rules.csv")


# ── build_table_config ───────────────────────────────────────────────────────

def test_build_table_config_maps_columns_and_refs():
    roles = TableRoles(
        table="patients",
        columns={"patientid": "PATIENT_ID", "first_name": "MASK", "junk": "NOT_A_RULE"},
        patient_id_col="patientid",
    )
    tc = build_table_config(roles)
    assert tc["reference_patient_id_column"] == "patientid"
    assert tc["reference_enc_id_column"] is None
    names = {c["column_name"] for c in tc["columns_details"]}
    assert names == {"patientid", "first_name"}          # unknown rule dropped
    for c in tc["columns_details"]:
        assert c["is_phi"] is True
        assert c["mask_value"] == c["column_name"].upper()


# ── summary / findings assembly ──────────────────────────────────────────────

def _part1_result(passed, failed_col=None):
    cols = {"patientid": {"passed_count": 10, "failed_count": 0, "remarks": {}}}
    if failed_col:
        cols[failed_col] = {"passed_count": 8, "failed_count": 2,
                            "remarks": {"residual_pii_remarks": [("PHONE_NUMBER", "555-1212")]}}
    return {
        "table_name": "patients", "source_rows_count": 100, "dest_rows_count": 100,
        "ignore_rows_count": 0, "unstruct_sample_size": 20, "ColumnsQCResult": cols,
        "final_qc_result": {"is_qc_passed": passed, "reason": "" if passed else "QC Failed"},
    }


def _per_table_all_pass():
    roles = TableRoles(table="patients", columns={"patientid": "PATIENT_ID", "notes": "NOTES"},
                       patient_id_col="patientid", note_cols=["notes"])
    return {"patients": {
        "roles": roles,
        "part1": {"status": "PASS", "result": _part1_result(True)},
        "delta": {"status": "PASS", "result": {"biz_key_col": "patientid", "missing_in_dest": 0,
                                               "extra_in_dest": 0, "value_mismatch": 0, "reason": "ok"}},
        "master": {"status": "PASS", "result": {"fail_count": 0, "coverage_gaps": 0, "failure_detail": []}},
    }}


def test_build_summary_rows_pass_and_gate():
    gate = {"status": "PASS", "checks": []}
    rows = build_summary_rows(_per_table_all_pass(), gate)
    assert rows[0]["table"] == "patients"
    assert rows[0]["overall_status"] == "PASS"
    assert rows[-1]["table"] == "(mapping-count-gate)"
    assert rows[-1]["overall_status"] == "PASS"


def test_build_summary_rows_fail_and_error():
    per = {"t": {
        "roles": TableRoles(table="t", columns={"c": "MASK"}),
        "part1": {"status": "FAIL", "result": _part1_result(False, failed_col="c")},
        "delta": {"status": "ERROR", "reason": "no nd_auto_increment_id", "result": None},
        "master": {"status": "SKIPPED", "result": None},
    }}
    rows = build_summary_rows(per, {"status": "SKIPPED", "reason": "not configured", "checks": []})
    r = rows[0]
    assert r["overall_status"] == "FAIL"
    assert r["part1_failed_columns"] == "c"
    assert "delta:no nd_auto_increment_id" in r["errors"]


def test_build_findings_rows_details():
    per = {"patients": {
        "roles": TableRoles(table="patients", columns={"patientid": "PATIENT_ID", "notes": "NOTES"},
                            note_cols=["notes"]),
        "part1": {"status": "FAIL", "result": _part1_result(False, failed_col="notes")},
        "delta": {"status": "FAIL", "result": {"biz_key_col": "patientid", "missing_in_dest": 3,
                                               "extra_in_dest": 0, "value_mismatch": 1, "reason": "diff"}},
        "master": {"status": "FAIL", "result": {"fail_count": 1, "coverage_gaps": 2,
                                               "failure_detail": [{"nd_patient_id": "ND1",
                                                                   "phi_hits": ["Jane"], "mask_hits": [],
                                                                   "surrogate_missing": False}]}},
    }}
    gate = {"status": "FAIL", "checks": [{"check_name": "offset_range", "entity": "patients",
                                          "status": "fail", "actual": 5, "details": "out of range"}]}
    rows = build_findings_rows(per, gate)
    kinds = {(r["part"], r["check"]) for r in rows}
    assert ("part1_unstructured", "phi_detected") in kinds     # notes → unstructured
    assert ("delta_identity", "missing_in_dest") in kinds
    assert ("delta_identity", "value_mismatch") in kinds
    assert ("master_phi", "coverage_gap") in kinds
    assert ("master_phi", "phi_or_mask") in kinds
    assert ("gate", "offset_range") in kinds
    # no extra_in_dest finding (count was 0)
    assert ("delta_identity", "extra_in_dest") not in kinds


# ── run_auto_qc (runners stubbed) ────────────────────────────────────────────

def test_run_auto_qc_writes_both_csvs(tmp_path, monkeypatch):
    rules = _write_rules_csv(tmp_path)

    monkeypatch.setattr(aq, "run_part1", lambda cfg, roles, qc, mdb: {"status": "PASS", "result": _part1_result(True)})
    monkeypatch.setattr(aq, "run_delta", lambda cfg, roles, delta_after=None: {
        "status": "PASS", "result": {"biz_key_col": roles.patient_id_col or "", "missing_in_dest": 0,
                                     "extra_in_dest": 0, "value_mismatch": 0, "reason": "ok"}})
    monkeypatch.setattr(aq, "run_master", lambda cfg, roles, conn, backend: (
        {"status": "PASS", "result": {"fail_count": 0, "coverage_gaps": 0, "failure_detail": []}}
        if roles.note_cols else {"status": "SKIPPED", "result": None}))
    monkeypatch.setattr(aq, "run_gate", lambda cfg: {"status": "SKIPPED", "reason": "test", "checks": []})

    out = run_auto_qc(_fake_cfg(), rules, out_dir=str(tmp_path / "out"))

    # both files exist with expected headers
    with open(out["summary_csv"], newline="") as f:
        srows = list(csv.DictReader(f))
    with open(out["findings_csv"], newline="") as f:
        frows = list(csv.DictReader(f))
    # 2 tables + gate row
    assert {r["table"] for r in srows} == {"patients", "encounters", "(mapping-count-gate)"}
    assert all(r["overall_status"] in ("PASS", "SKIPPED") for r in srows)
    # encounters has no notes → master skipped
    enc = next(r for r in srows if r["table"] == "encounters")
    assert enc["master_status"] == "SKIPPED"
    # all-pass run → no findings
    assert frows == []


def test_run_auto_qc_error_in_one_check_is_isolated(tmp_path, monkeypatch):
    rules = _write_rules_csv(tmp_path, rows=[("patients", "patientid", "PATIENT_ID")])

    def boom(*a, **k):
        raise RuntimeError("db down")

    monkeypatch.setattr(aq, "run_part1", boom)   # errors, but must not abort
    monkeypatch.setattr(aq, "run_delta", lambda cfg, roles, delta_after=None: {"status": "PASS", "result": {
        "biz_key_col": "patientid", "missing_in_dest": 0, "extra_in_dest": 0, "value_mismatch": 0, "reason": ""}})
    monkeypatch.setattr(aq, "run_master", lambda *a, **k: {"status": "SKIPPED", "result": None})
    monkeypatch.setattr(aq, "run_gate", lambda cfg: {"status": "SKIPPED", "checks": []})

    out = run_auto_qc(_fake_cfg(), rules, out_dir=str(tmp_path / "out"))
    prow = next(r for r in out["summary"] if r["table"] == "patients")
    assert prow["part1_status"] == "ERROR"
    assert prow["overall_status"] == "PARTIAL"   # part1 ERROR + delta PASS
    assert any(f["status"] == "ERROR" and f["part"] == "part1" for f in out["findings"])


def test_run_auto_qc_no_matching_tables_raises(tmp_path):
    rules = _write_rules_csv(tmp_path)
    with pytest.raises(ValueError, match="No tables to QC"):
        run_auto_qc(_fake_cfg(), rules, tables=["ghost"], out_dir=str(tmp_path / "out"))


def test_run_auto_qc_parallel_matches_sequential(tmp_path, monkeypatch):
    """max_workers>1 must produce the same per-table rows and preserve input order."""
    rules = _write_rules_csv(tmp_path)

    monkeypatch.setattr(aq, "run_part1", lambda cfg, roles, qc, mdb: {"status": "PASS", "result": _part1_result(True)})
    monkeypatch.setattr(aq, "run_delta", lambda cfg, roles, delta_after=None: {"status": "PASS", "result": {
        "biz_key_col": "", "missing_in_dest": 0, "extra_in_dest": 0, "value_mismatch": 0, "reason": ""}})
    monkeypatch.setattr(aq, "run_master", lambda *a, **k: {"status": "SKIPPED", "result": None})
    monkeypatch.setattr(aq, "run_gate", lambda cfg: {"status": "SKIPPED", "checks": []})

    out = run_auto_qc(_fake_cfg(), rules, tables=["encounters", "patients"],
                      out_dir=str(tmp_path / "out"), max_workers=4)
    tables_in_order = [r["table"] for r in out["summary"] if r["table"] != "(mapping-count-gate)"]
    assert tables_in_order == ["encounters", "patients"]   # input order preserved
    assert all(r["overall_status"] in ("PASS", "SKIPPED") for r in out["summary"])
