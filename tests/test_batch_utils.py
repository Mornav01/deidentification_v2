"""Tests for deid.tasks.batch_utils — _is_lock_error and reset_or_fail_batch."""
import pytest
from sqlalchemy.orm import Session


@pytest.fixture
def state_engine(tmp_path):
    from deid.models.base import create_state_engine, create_all_state_tables
    engine = create_state_engine(str(tmp_path / "state.db"))
    create_all_state_tables(engine)
    return engine


@pytest.fixture
def batch_row(state_engine):
    """Pre-insert a single BatchState row at status='pending', retry_count=0."""
    from deid.models.state import BatchState
    with Session(state_engine) as s:
        s.add(BatchState(
            table_name="tbl1", start_id=0, end_id=999,
            status="pending", config_key="default",
        ))
        s.commit()
    return state_engine


# ---------------------------------------------------------------------------
# _is_lock_error
# ---------------------------------------------------------------------------

def test_is_lock_error_mysql_timeout():
    from deid.tasks.batch_utils import _is_lock_error
    assert _is_lock_error(Exception("Lock wait timeout exceeded")) is True


def test_is_lock_error_mysql_deadlock():
    from deid.tasks.batch_utils import _is_lock_error
    assert _is_lock_error(Exception("Deadlock found when trying to get lock")) is True


def test_is_lock_error_mysql_code_1205():
    from deid.tasks.batch_utils import _is_lock_error
    assert _is_lock_error(Exception("(1205, 'Lock wait timeout exceeded')")) is True


def test_is_lock_error_mysql_code_1213():
    from deid.tasks.batch_utils import _is_lock_error
    assert _is_lock_error(Exception("(1213, 'Deadlock found')")) is True


def test_is_lock_error_sqlite():
    from deid.tasks.batch_utils import _is_lock_error
    assert _is_lock_error(Exception("database is locked")) is True


def test_is_lock_error_case_insensitive():
    from deid.tasks.batch_utils import _is_lock_error
    assert _is_lock_error(Exception("LOCK WAIT TIMEOUT exceeded")) is True


def test_is_lock_error_false_connection_refused():
    from deid.tasks.batch_utils import _is_lock_error
    assert _is_lock_error(Exception("connection refused")) is False


def test_is_lock_error_false_syntax_error():
    from deid.tasks.batch_utils import _is_lock_error
    assert _is_lock_error(Exception("syntax error near SELECT")) is False


# ---------------------------------------------------------------------------
# reset_or_fail_batch
# ---------------------------------------------------------------------------

def test_reset_or_fail_batch_not_found(state_engine):
    from deid.tasks.batch_utils import reset_or_fail_batch
    result = reset_or_fail_batch(state_engine, "no_such_table", 0, 999, "default", max_retries=3)
    assert result == "pending"


def test_reset_or_fail_batch_first_failure(batch_row):
    from deid.tasks.batch_utils import reset_or_fail_batch
    from deid.models.state import BatchState

    result = reset_or_fail_batch(batch_row, "tbl1", 0, 999, "default", max_retries=3, reason="fetch error")
    assert result == "pending"

    with Session(batch_row) as s:
        row = s.query(BatchState).filter_by(table_name="tbl1").first()
        assert row.status == "pending"
        assert row.retry_count == 1
        assert row.last_failed_reason == "fetch error"


def test_reset_or_fail_batch_second_failure_still_pending(batch_row):
    from deid.tasks.batch_utils import reset_or_fail_batch
    from deid.models.state import BatchState

    reset_or_fail_batch(batch_row, "tbl1", 0, 999, "default", max_retries=3)
    result = reset_or_fail_batch(batch_row, "tbl1", 0, 999, "default", max_retries=3)
    assert result == "pending"

    with Session(batch_row) as s:
        row = s.query(BatchState).filter_by(table_name="tbl1").first()
        assert row.retry_count == 2
        assert row.status == "pending"


def test_reset_or_fail_batch_at_threshold_marks_failed(batch_row):
    from deid.tasks.batch_utils import reset_or_fail_batch
    from deid.models.state import BatchState

    reset_or_fail_batch(batch_row, "tbl1", 0, 999, "default", max_retries=3)
    reset_or_fail_batch(batch_row, "tbl1", 0, 999, "default", max_retries=3)
    result = reset_or_fail_batch(batch_row, "tbl1", 0, 999, "default", max_retries=3)
    assert result == "failed"

    with Session(batch_row) as s:
        row = s.query(BatchState).filter_by(table_name="tbl1").first()
        assert row.status == "failed"
        assert row.retry_count == 3


def test_reset_or_fail_batch_reason_truncated(batch_row):
    from deid.tasks.batch_utils import reset_or_fail_batch
    from deid.models.state import BatchState

    long_reason = "x" * 600
    reset_or_fail_batch(batch_row, "tbl1", 0, 999, "default", max_retries=3, reason=long_reason)

    with Session(batch_row) as s:
        row = s.query(BatchState).filter_by(table_name="tbl1").first()
        assert row.last_failed_reason == "x" * 500


def test_reset_or_fail_batch_empty_reason(batch_row):
    from deid.tasks.batch_utils import reset_or_fail_batch
    from deid.models.state import BatchState

    reset_or_fail_batch(batch_row, "tbl1", 0, 999, "default", max_retries=3)

    with Session(batch_row) as s:
        row = s.query(BatchState).filter_by(table_name="tbl1").first()
        assert row.last_failed_reason == ""


def test_reset_or_fail_batch_max_retries_one(state_engine):
    from deid.tasks.batch_utils import reset_or_fail_batch
    from deid.models.state import BatchState

    with Session(state_engine) as s:
        from deid.models.state import BatchState
        s.add(BatchState(table_name="fast_fail", start_id=0, end_id=99, status="pending"))
        s.commit()

    result = reset_or_fail_batch(state_engine, "fast_fail", 0, 99, "default", max_retries=1)
    assert result == "failed"

    with Session(state_engine) as s:
        row = s.query(BatchState).filter_by(table_name="fast_fail").first()
        assert row.status == "failed"
        assert row.retry_count == 1


# ---------------------------------------------------------------------------
# _is_connection_error
# ---------------------------------------------------------------------------

def test_is_connection_error_pymssql_20017():
    from deid.tasks.batch_utils import _is_connection_error
    assert _is_connection_error(Exception(
        "(20017, b'DB-Lib error message 20017, severity 9:\\nUnexpected EOF from the server\\n')"
    )) is True


def test_is_connection_error_connection_reset():
    from deid.tasks.batch_utils import _is_connection_error
    assert _is_connection_error(Exception("connection reset by peer")) is True


def test_is_connection_error_false_syntax_error():
    from deid.tasks.batch_utils import _is_connection_error
    assert _is_connection_error(Exception("syntax error near SELECT")) is False
