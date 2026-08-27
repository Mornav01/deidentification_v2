"""QC Framework coverage tests — Parts 1, 2, 3 (auto_qc)."""
import tempfile

import pytest
from sqlalchemy import create_engine, text


# ── Part 1 — structured detectors ────────────────────────────────────────────


def test_new_detectors_registered():
    from deid.qc.builders import DectorMapping
    assert "APPOINTMENT_ID" in DectorMapping
    assert "CHART_ID" in DectorMapping


def test_appointment_id_length_prefix():
    from deid.qc.builders.structured import SAppointmentIdDetector
    d = SAppointmentIdDetector(
        patient_mapping_dict={}, enc_mapping_dict={},
        qc_config={"APPOINTMENT_ID": {"length_of_value": 15, "prefix_value": "1001"}},
        column_config={"column_name": "apptid"}, patient_id_column=None, enc_id_column=None,
    )
    rows = [
        {"nd_auto_increment_id": 1, "apptid": "100100000000001"},  # ok
        {"nd_auto_increment_id": 2, "apptid": "999"},              # bad
        {"nd_auto_increment_id": 3, "apptid": None},               # null → pass
    ]
    r = d.is_deidentified(before_rows=[], after_rows=rows, ignore_condition={})
    assert r["passed_count"] == 2 and r["failed_count"] == 1
    assert r["remarks"]["offenders"][0]["nd_auto_increment_id"] == 2


def test_zip_exactly_three():
    from deid.qc.builders.structured import SZipCodeDetector
    d = SZipCodeDetector(patient_mapping_dict={}, enc_mapping_dict={}, qc_config={},
                         column_config={"column_name": "zip"}, patient_id_column=None, enc_id_column=None)
    rows = [{"zip": "021"}, {"zip": "02139"}, {"zip": None}, {"zip": "02"}]
    r = d.is_deidentified(before_rows=[], after_rows=rows, ignore_condition={})
    assert r["passed_count"] == 2  # "021" and None; "02139" and "02" fail


def test_date_plausibility():
    from deid.qc.builders.structured import _parse_date, _is_plausible_date
    from datetime import datetime
    today = datetime(2026, 7, 1)
    assert _is_plausible_date(_parse_date("2020-05-01"), today) is True
    assert _is_plausible_date(_parse_date("2099-01-01"), today) is False
    assert _is_plausible_date(_parse_date("1800-01-01"), today) is False


# ── Part 2 — mapping & count + blocking gate ──────────────────────────────────


def _sqlite(tmp, name, stmts):
    url = f"sqlite:///{tmp}/{name}.db"
    e = create_engine(url)
    with e.begin() as c:
        for s in stmts:
            c.execute(text(s))
    e.dispose()
    return url


def test_part2_gate_blocks_on_offset(tmp_path):
    from deid.qc.mapping_count import Part2Config, run_pre_pipeline_gate, Part2Blocked
    tmp = str(tmp_path)
    map_url = _sqlite(tmp, "map", [
        "CREATE TABLE patient_mapping_table (patient_id TEXT, nd_patient_id INTEGER, offset INTEGER)",
        "INSERT INTO patient_mapping_table VALUES ('p1',1,10),('p2',2,99)",
        "CREATE TABLE encounter_mapping_table (encounter_id TEXT, nd_encounter_id INTEGER)",
        "INSERT INTO encounter_mapping_table VALUES ('e1',1)",
    ])
    src_url = _sqlite(tmp, "src", ["CREATE TABLE t (x INTEGER)"])
    dst_url = _sqlite(tmp, "dst", ["CREATE TABLE t (x INTEGER)"])
    cfg = Part2Config(check_offset_range=True, offset_min=-38, offset_max=38)
    with pytest.raises(Part2Blocked):
        run_pre_pipeline_gate(src_url, dst_url, map_url, cfg, blocking=True)


def test_part2_mapping_uniqueness_and_counts(tmp_path):
    from deid.qc.mapping_count import Part2Config, run_part2_checks
    tmp = str(tmp_path)
    map_url = _sqlite(tmp, "map", [
        "CREATE TABLE patient_mapping_table (patient_id TEXT, nd_patient_id INTEGER, offset INTEGER)",
        "INSERT INTO patient_mapping_table VALUES ('p1',1,0),('p1',2,0)",  # p1 → 2 nd ids (violation)
        "CREATE TABLE encounter_mapping_table (encounter_id TEXT, nd_encounter_id INTEGER)",
        "INSERT INTO encounter_mapping_table VALUES ('e1',1)",
    ])
    src_url = _sqlite(tmp, "src", ["CREATE TABLE t (x INTEGER)"])
    dst_url = _sqlite(tmp, "dst", ["CREATE TABLE t (x INTEGER)"])
    cfg = Part2Config(check_offset_range=False)
    res = {(r["check_name"], r["entity"]): r for r in run_part2_checks(src_url, dst_url, map_url, cfg)}
    assert res[("mapping_uniqueness", "patient:patient_mapping_table")]["status"] == "fail"


# ── Part 2 (row-level) — delta identity ───────────────────────────────────────


def test_delta_identity_classifies(tmp_path):
    from deid.qc.delta_identity import DeltaIdentityConfig, run_delta_identity_qc
    tmp = str(tmp_path)
    src_url = _sqlite(tmp, "src", [
        "CREATE TABLE t (nd_auto_increment_id INTEGER, patientid TEXT)",
        "INSERT INTO t VALUES (2,'200'),(3,'300'),(4,'400')",
    ])
    dst_url = _sqlite(tmp, "dst", [
        "CREATE TABLE t (nd_auto_increment_id INTEGER, patientid TEXT, nd_extracted_date TEXT)",
        "INSERT INTO t VALUES (2,'200','2026-06-20'),(3,'999','2026-06-20'),(6,'600','2026-06-20'),(1,'100','2026-05-01')",
    ])
    cfg = DeltaIdentityConfig(tables=["t"], delta_after="2026-06-01")
    r = run_delta_identity_qc(src_url, dst_url, cfg)[0]
    # dest in-window ids = {2,3,6}; source filtered to those = {2,3}. id2 matches, id3 mismatches,
    # id6 is extra-in-dest, none missing. → matched=1, mismatch=1, extra=1.
    assert r["extra_in_dest"] == 1 and r["value_mismatch"] == 1 and r["matched_count"] == 1
    assert r["is_qc_passed"] is False


# ── Part 3 — master-referenced audit ──────────────────────────────────────────


def test_phi_presence_and_masking_scans():
    from deid.qc.master_phi import scan_phi_presence, scan_masking_outcomes, MasterPhiConfig
    assert scan_phi_presence("mr JOHN here", {"ufname": "John"})[0]["kind"] == "exact"
    np = scan_phi_presence("seen by John", {"full_name": "John Smith"}, name_columns=["full_name"])
    assert np and np[0]["kind"] == "name_part"
    cfg = MasterPhiConfig(dest_table="n", content_cols=["c"], facility_names=["Northwest"])
    kinds = {h["kind"] for h in scan_masking_outcomes("call 555-123-4567 www.x.com Northwest", cfg)}
    assert {"phone", "url", "facility"} <= kinds


def test_p2_masking_address_and_dates():
    from deid.qc.master_phi import scan_masking_outcomes, scan_dates_in_notes, scan_mask_token, MasterPhiConfig
    from datetime import datetime
    cfg = MasterPhiConfig(dest_table="n", content_cols=["c"])
    # address detection
    kinds = {h["kind"] for h in scan_masking_outcomes("lives at 123 Main Street apt 4", cfg)}
    assert "address" in kinds
    # dates-in-notes: leak of a raw master date + implausible future date
    today = datetime(2026, 7, 1)
    hits = {h["kind"] for h in scan_dates_in_notes("dob 1985-06-15 and 2099-01-01", ["1985-06-15"], today)}
    assert "date_leak" in hits and "date_implausible" in hits
    # mask-token presence
    assert scan_mask_token("name is <<PATIENT_NAME>> here", ["<<PATIENT_NAME>>"]) is True
    assert scan_mask_token("no token", ["<<PATIENT_NAME>>"]) is False


def test_p2_audit_record_flags_address_and_date_leak():
    from deid.qc.master_phi import audit_record, MasterPhiConfig
    from datetime import datetime
    cfg = MasterPhiConfig(dest_table="n", content_cols=["c"], date_columns=["dob"])
    phi = {"dob": "1985-06-15", "ufname": "Zephyr"}
    res = audit_record("seen 1985-06-15 at 500 Oak Avenue", phi, cfg, today=datetime(2026, 7, 1))
    kinds = {h["kind"] for h in res["mask_hits"]}
    assert "date_leak" in kinds and "address" in kinds
    assert res["passed"] is False


def test_mask_token_required_opt_in():
    from deid.qc.master_phi import audit_record, MasterPhiConfig
    cfg = MasterPhiConfig(dest_table="n", content_cols=["c"], name_columns=["ufname"],
                          require_mask_token=True, expected_mask_tokens=["<<PATIENT_NAME>>"],
                          check_dates_in_notes=False)
    # patient has a name, note is non-empty, no token → flagged
    res = audit_record("routine visit note", {"ufname": "Zephyr"}, cfg)
    assert any(h["kind"] == "mask_token_missing" for h in res["mask_hits"])
    # token present → not flagged
    res2 = audit_record("visit for <<PATIENT_NAME>>", {"ufname": "Zephyr"}, cfg)
    assert not any(h["kind"] == "mask_token_missing" for h in res2["mask_hits"])


def test_residual_scanner_regex_backend():
    from deid.qc.llm_scan import ResidualPIIScanner, regex_scan
    s = ResidualPIIScanner(backend="regex")
    hits = {h["type"] for h in s.scan("call 555-123-4567 or a@b.com ssn 123-45-6789 www.x.com")}
    assert {"PHONE_NUMBER", "EMAIL_ADDRESS", "US_SSN", "URL"} <= hits
    assert ResidualPIIScanner(backend="none").scan("555-123-4567") == []
    assert regex_scan("nothing here") == []


def test_unknown_backend_falls_back_to_regex():
    # An unsupported backend name must degrade to regex, not crash.
    from deid.qc.llm_scan import ResidualPIIScanner
    s = ResidualPIIScanner(backend="something-else")
    assert s.backend == "regex"
    hits = {h["type"] for h in s.scan("call 555-123-4567")}
    assert "PHONE_NUMBER" in hits


def test_unstructured_detector_uses_scanner_not_presidio():
    from deid.qc.builders.unstructured import UnstructuredDetector
    import deid.qc.builders.unstructured as u
    assert not hasattr(u, "_get_analyzer") and "presidio" not in (u.__doc__ or "").lower()
    d = UnstructuredDetector(patient_mapping_dict={}, enc_mapping_dict={},
                             qc_config={"residual_pii_backend": "regex"},
                             column_config={"column_name": "note"},
                             patient_id_column=None, enc_id_column=None)
    rows = [
        {"nd_auto_increment_id": 1, "note": "clean text"},
        {"nd_auto_increment_id": 2, "note": "call 555-123-4567"},
        {"nd_auto_increment_id": 3, "note": None},
    ]
    r = d.is_deidentified(before_rows=[], after_rows=rows, ignore_condition={}, pii_info={})
    # Residual-regex hits are advisory only — pass/fail is gated on the master exact-match.
    assert r["passed_count"] == 3 and r["failed_count"] == 0
    assert "residual_advisory" in r["remarks"]
    assert [x["nd_auto_increment_id"] for x in r["remarks"]["residual_advisory"]] == [2]


def test_unstructured_detector_fails_only_on_master_exact_match():
    from deid.qc.builders.unstructured import UnstructuredDetector
    d = UnstructuredDetector(patient_mapping_dict={}, enc_mapping_dict={},
                             qc_config={"residual_pii_backend": "none"},
                             column_config={"column_name": "note"},
                             patient_id_column=None, enc_id_column=None)
    rows = [
        {"nd_auto_increment_id": 10, "note": "patient John Doe seen today"},
        {"nd_auto_increment_id": 11, "note": "clean"},
    ]
    r = d.is_deidentified(before_rows=[], after_rows=rows, ignore_condition={},
                          pii_info={"John Doe": "John Doe"})
    assert r["passed_count"] == 1 and r["failed_count"] == 1
    # The failing row carries its nd_auto_increment_id for manual review.
    assert [x["nd_auto_increment_id"] for x in r["remarks"]["exact_match_failures"]] == [10]


def test_master_phi_audit_end_to_end(tmp_path):
    from deid.qc.master_phi import MasterPhiConfig, run_master_phi_audit
    tmp = str(tmp_path)
    dst = _sqlite(tmp, "dst", [
        "CREATE TABLE notes (nd_patient_id INTEGER, content TEXT)",
        "INSERT INTO notes VALUES (1,'clean'),(2,'contains John Smith'),(3,'')",
    ])
    cfg = MasterPhiConfig(dest_table="notes", content_cols=["content"])
    loader = lambda ids: {1: {"ufname": "Alice"}, 2: {"ufname": "John"}, 3: {"ufname": "Bob"}}
    rep = run_master_phi_audit(dst, cfg, loader)
    assert rep["total_records_audited"] == 3
    assert rep["fail_count"] == 1
    assert rep["coverage_gaps"] == 1


def test_audit_report_builds(tmp_path):
    from deid.qc.delta_identity import DeltaIdentityConfig, run_delta_identity_qc
    from deid.qc.report import build_audit_report, render_markdown
    tmp = str(tmp_path)
    qc_db = f"{tmp}/qc.db"
    src_url = _sqlite(tmp, "src", ["CREATE TABLE t (nd_auto_increment_id INTEGER, patientid TEXT)",
                                   "INSERT INTO t VALUES (1,'a')"])
    dst_url = _sqlite(tmp, "dst", ["CREATE TABLE t (nd_auto_increment_id INTEGER, patientid TEXT)",
                                   "INSERT INTO t VALUES (1,'a')"])
    run_delta_identity_qc(src_url, dst_url, DeltaIdentityConfig(tables=["t"]), qc_results_db_url=qc_db)
    report = build_audit_report(qc_db)
    assert report["delta_identity"]["tables_checked"] == 1
    assert isinstance(render_markdown(report), str)


def test_prune_config_remaps_to_actual_dest_casing():
    """Case-insensitive match + remap to the real dest column casing; unmatched dropped."""
    from deid.qc.scanner import DbScanner
    tc = {
        "columns_details": [
            {"column_name": "encounterid", "is_phi": True, "de_identification_rule": "ENCOUNTER_ID"},
            {"column_name": "modifydate", "is_phi": True, "de_identification_rule": "DATE_OFFSET"},
            {"column_name": "ghost", "is_phi": True, "de_identification_rule": "MASK"},
        ],
        "reference_patient_id_column": "patientid",
        "reference_enc_id_column": "encounterid",
    }
    available = {"encounterID", "ModifyDate", "PatientID", "nd_auto_increment_id"}
    DbScanner._prune_config_to_available(tc, available, "t")
    assert {c["column_name"] for c in tc["columns_details"]} == {"encounterID", "ModifyDate"}
    assert tc["reference_patient_id_column"] == "PatientID"   # remapped to real casing
    assert tc["reference_enc_id_column"] == "encounterID"


def test_date_offset_reads_source_column_case_insensitively():
    """Source and dest may differ in column casing; the offset check must still align them."""
    from deid.qc.builders.structured import SDateOffestDetector
    d = SDateOffestDetector(
        patient_mapping_dict={100: {"offset": 10}}, enc_mapping_dict={},
        qc_config={}, column_config={"column_name": "ModifyDate"},   # dest casing
        patient_id_column="patientid", enc_id_column=None,
    )
    before = [{"nd_auto_increment_id": 1, "modifydate": "2020-01-01"}]   # source lowercase
    after = [{"nd_auto_increment_id": 1, "ModifyDate": "2020-01-11", "patientid": 100}]
    r = d.is_deidentified(before_rows=before, after_rows=after, ignore_condition={})
    assert r["passed_count"] == 1 and r["failed_count"] == 0   # 10-day shift == offset


def test_date_offset_future_shifted_date_passes_not_implausible():
    """A positive offset can push a de-identified date past 'today'; if dest == source+offset it
    must PASS (regression: was wrongly flagged 'implausible' by the [1900, today] window)."""
    from deid.qc.builders.structured import SDateOffestDetector
    d = SDateOffestDetector(
        patient_mapping_dict={500: {"offset": 34}}, enc_mapping_dict={},
        qc_config={}, column_config={"column_name": "ModifyDate"},
        patient_id_column="patientid", enc_id_column=None,
    )
    # source 2026-07-07 + 34d == dest 2026-08-10 (in the future relative to a 'today' of ~2026-07-27)
    before = [{"nd_auto_increment_id": 7921387, "modifydate": "2026-07-07 16:53:45"}]
    after = [{"nd_auto_increment_id": 7921387, "ModifyDate": "2026-08-10 00:00:00", "patientid": 500}]
    r = d.is_deidentified(before_rows=before, after_rows=after, ignore_condition={})
    assert r["passed_count"] == 1 and r["failed_count"] == 0


def test_encounter_id_must_be_prefixed_by_patient_id():
    """nd_encounter_id must start with its row's nd_patient_id (left(enc,len(pat))==pat)."""
    from deid.qc.builders.structured import SEncounterIDDetector
    d = SEncounterIDDetector(
        patient_mapping_dict={}, enc_mapping_dict={}, qc_config={},
        column_config={"column_name": "encounterid"},
        patient_id_column="patientid", enc_id_column="encounterid",
    )
    rows = [
        {"nd_auto_increment_id": 1, "patientid": 10001, "encounterid": 10001001},  # ok: starts with 10001
        {"nd_auto_increment_id": 2, "patientid": 10001, "encounterid": 20002001},  # bad: wrong patient prefix
        {"nd_auto_increment_id": 3, "patientid": None, "encounterid": 99},          # no patient → skip seq check
    ]
    r = d.is_deidentified(before_rows=[], after_rows=rows, ignore_condition={})
    assert r["passed_count"] == 2 and r["failed_count"] == 1
    assert r["remarks"]["patient_prefix_verification_failed"] == 1
    assert r["remarks"]["offenders"][0]["nd_auto_increment_id"] == 2


def test_offset_band_validation_rejects_zero_and_small(tmp_path):
    """Offsets must be non-zero and banded [-38,-30] U [30,38]; 0, |o|<30, |o|>38 are violations."""
    from deid.qc.mapping_count import Part2Config, check_offset_range
    from deid.core.dbPkg.dbhandler import NDDBHandler
    tmp = str(tmp_path)
    map_url = _sqlite(tmp, "map", [
        "CREATE TABLE patient_mapping_table (nd_patient_id INTEGER, offset INTEGER)",
        # valid: 34, -30, 38, -38 ;  invalid: 0, 10, 40
        "INSERT INTO patient_mapping_table VALUES (1,34),(2,-30),(3,38),(4,-38),(5,0),(6,10),(7,40)",
    ])
    h = NDDBHandler(map_url, read_only=True)
    try:
        r = check_offset_range(h, Part2Config())[0]   # defaults: abs_min=30, [-38,38]
    finally:
        h.close()
    assert r["status"] == "fail"
    assert r["actual"] == "3"          # 0, 10, 40
    assert "nd_patient_id:offset" in r["details"]


def test_offset_band_validation_passes_when_all_in_band(tmp_path):
    from deid.qc.mapping_count import Part2Config, check_offset_range
    from deid.core.dbPkg.dbhandler import NDDBHandler
    tmp = str(tmp_path)
    map_url = _sqlite(tmp, "map2", [
        "CREATE TABLE patient_mapping_table (nd_patient_id INTEGER, offset INTEGER)",
        "INSERT INTO patient_mapping_table VALUES (1,30),(2,-38),(3,35),(4,-31)",
    ])
    h = NDDBHandler(map_url, read_only=True)
    try:
        r = check_offset_range(h, Part2Config())[0]
    finally:
        h.close()
    assert r["status"] == "pass"


def test_mapping_uniqueness_one_patient_one_ndid(tmp_path):
    """Each source patientid must map to exactly one nd_patient_id (renamed src col honored)."""
    from deid.qc.mapping_count import Part2Config, check_mapping_uniqueness
    from deid.core.dbPkg.dbhandler import NDDBHandler
    tmp = str(tmp_path)
    map_url = _sqlite(tmp, "uniq", [
        "CREATE TABLE patient_mapping_table (patientid TEXT, nd_patient_id INTEGER)",
        "INSERT INTO patient_mapping_table VALUES ('P1',1),('P1',2),('P2',3)",   # P1 -> 2 nd ids
        "CREATE TABLE encounter_mapping_table (encounter_id TEXT, nd_encounter_id INTEGER)",
        "INSERT INTO encounter_mapping_table VALUES ('E1',10),('E2',20)",
    ])
    h = NDDBHandler(map_url, read_only=True)
    try:
        res = {r["entity"]: r for r in check_mapping_uniqueness(h, Part2Config(patient_map_src_col="patientid"))}
    finally:
        h.close()
    assert res["patient:patient_mapping_table"]["status"] == "fail"
    assert res["patient:patient_mapping_table"]["actual"] == "1"     # one offending patientid (P1)
    assert res["encounter:encounter_mapping_table"]["status"] == "pass"


def test_fill_rate_parity_check(tmp_path, monkeypatch):
    """Identifier fill-rate must match source vs dest; a drop (id nulled out) FAILs."""
    from types import SimpleNamespace
    import deid.qc.auto_qc as aq
    from deid.qc.auto_qc import run_fill_rate, TableRoles
    tmp = str(tmp_path)
    # source: encounterid 100% filled (3/3); dest: 66% (2/3) -> one id lost -> FAIL
    src = _sqlite(tmp, "s", [
        "CREATE TABLE enc (nd_auto_increment_id INTEGER, encounterid INTEGER)",
        "INSERT INTO enc VALUES (1,100010001),(2,100010002),(3,100020001)",
    ])
    dst = _sqlite(tmp, "d", [
        "CREATE TABLE enc (nd_auto_increment_id INTEGER, encounterid INTEGER)",
        "INSERT INTO enc VALUES (1,100010001),(2,100010002),(3,NULL)",
    ])
    cfg = SimpleNamespace(
        qc=SimpleNamespace(fillrate_tolerance_pct=1.0),
        source_db=SimpleNamespace(connection_string=lambda: src),
        destination_db=SimpleNamespace(connection_string=lambda: dst),
    )
    roles = TableRoles(table="enc", columns={"encounterid": "ENCOUNTER_ID"}, encounter_id_col="encounterid")
    r = run_fill_rate(cfg, roles)
    assert r["status"] == "FAIL"
    col = r["result"]["columns"][0]
    assert col["source_fill_pct"] == 100.0 and col["dest_fill_pct"] == round(2/3*100, 2)
    assert col["passed"] is False


def test_patient_id_mapping_correctness():
    """Dest patient id must equal the mapping table's nd id for the row's source patient id."""
    from deid.qc.builders.structured import SPatientIdDetector
    # reverse map: nd_patient_id -> source patient_id
    d = SPatientIdDetector(
        patient_mapping_dict={1001: {"patient_id": "1", "offset": 34}, 1002: {"patient_id": "2", "offset": 31}},
        enc_mapping_dict={}, qc_config={}, column_config={"column_name": "patientid"},
        patient_id_column="patientid", enc_id_column=None,
    )
    before = [
        {"nd_auto_increment_id": 1, "patientid": "1"},   # source pid 1
        {"nd_auto_increment_id": 2, "patientid": "2"},   # source pid 2
    ]
    after = [
        {"nd_auto_increment_id": 1, "patientid": 1001},  # correct: 1 -> 1001
        {"nd_auto_increment_id": 2, "patientid": 1001},  # WRONG: pid 2 should be 1002, got 1001
    ]
    r = d.is_deidentified(before_rows=before, after_rows=after, ignore_condition={})
    assert r["passed_count"] == 1 and r["failed_count"] == 1
    assert r["remarks"]["mapping_mismatch"] == 1
    off = r["remarks"]["mapping_offenders"][0]
    assert off["nd_auto_increment_id"] == 2 and off["mapped_source_id"] == "1"


def test_load_mapping_covers_all_patient_columns(tmp_path):
    """A table with two patient columns (e.g. From/To) reverse-maps BOTH, not just the primary."""
    from deid.qc.scanner import LoadMappingData
    tmp = str(tmp_path)
    map_url = _sqlite(tmp, "mm", [
        "CREATE TABLE patient_mapping_table (patient_id TEXT, nd_patient_id INTEGER, offset INTEGER)",
        "INSERT INTO patient_mapping_table VALUES ('A',1001,34),('B',1002,31)",
    ])
    sample = [{"nd_auto_increment_id": 1, "fromid": 1001, "toid": 1002}]
    table_config = {
        "columns_details": [
            {"column_name": "fromid", "de_identification_rule": "PATIENT_ID"},
            {"column_name": "toid", "de_identification_rule": "PATIENT_ID"},
        ],
        "reference_patient_id_column": "fromid",
        "reference_enc_id_column": None,
    }
    pdict, _ = LoadMappingData.load(sample, table_config, {"connection_str": map_url})
    assert set(pdict) == {1001, 1002}   # both columns' nd ids present


def test_offender_ids_collects_uniform_list():
    from deid.qc.auto_qc import _offender_ids
    remarks = {
        "mapping_offenders": [{"nd_auto_increment_id": 5}, {"nd_auto_increment_id": 6}],
        "offenders": [{"nd_auto_increment_id": 6}, {"nd_auto_increment_id": 7}],  # 6 deduped
        "length_verification_failed": 3,  # non-list counter — ignored
    }
    assert _offender_ids(remarks) == "5, 6, 7"
