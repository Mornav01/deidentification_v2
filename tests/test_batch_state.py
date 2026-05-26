"""Tests for BatchState model."""
import pytest
from sqlalchemy.orm import Session


@pytest.fixture
def state_engine(tmp_path):
    from deid.models.base import create_state_engine, create_all_state_tables
    engine = create_state_engine(str(tmp_path / "state.db"))
    create_all_state_tables(engine)
    return engine


def test_batch_state_create(state_engine):
    from deid.models.state import BatchState

    with Session(state_engine) as session:
        batch = BatchState(
            table_name="patients",
            start_id=1,
            end_id=1000,
            status="pending",
        )
        session.add(batch)
        session.commit()
        assert batch.id is not None

    with Session(state_engine) as session:
        row = session.query(BatchState).first()
        assert row.table_name == "patients"
        assert row.start_id == 1
        assert row.end_id == 1000
        assert row.status == "pending"


def test_batch_state_unique_constraint(state_engine):
    from deid.models.state import BatchState
    from sqlalchemy.exc import IntegrityError

    with Session(state_engine) as session:
        session.add(BatchState(table_name="t1", start_id=1, end_id=100, status="pending"))
        session.commit()

    with Session(state_engine) as session:
        session.add(BatchState(table_name="t1", start_id=1, end_id=100, status="fetched"))
        with pytest.raises(IntegrityError):
            session.commit()


def test_batch_state_status_transitions(state_engine):
    from deid.models.state import BatchState

    with Session(state_engine) as session:
        session.add(BatchState(table_name="t1", start_id=1, end_id=100, status="pending"))
        session.commit()

    for new_status in ("fetched", "processed", "done"):
        with Session(state_engine) as session:
            batch = session.query(BatchState).filter_by(table_name="t1").first()
            batch.status = new_status
            session.commit()

        with Session(state_engine) as session:
            batch = session.query(BatchState).filter_by(table_name="t1").first()
            assert batch.status == new_status


def test_batch_state_sentinel_for_no_id_tables(state_engine):
    from deid.models.state import BatchState

    with Session(state_engine) as session:
        session.add(BatchState(table_name="no_id_table", start_id=-1, end_id=-1, status="pending"))
        session.commit()

    with Session(state_engine) as session:
        row = session.query(BatchState).filter_by(table_name="no_id_table").first()
        assert row.start_id == -1
        assert row.end_id == -1


def test_batch_state_new_columns_defaults(state_engine):
    from deid.models.state import BatchState

    with Session(state_engine) as session:
        session.add(BatchState(table_name="t_defaults", start_id=1, end_id=100, status="pending"))
        session.commit()

    with Session(state_engine) as session:
        row = session.query(BatchState).filter_by(table_name="t_defaults").first()
        assert row.retry_count == 0
        assert row.last_failed_reason is None


def test_batch_state_retry_fields_roundtrip(state_engine):
    from deid.models.state import BatchState

    with Session(state_engine) as session:
        session.add(BatchState(
            table_name="t_retry", start_id=1, end_id=100,
            status="failed", retry_count=2, last_failed_reason="oops",
        ))
        session.commit()

    with Session(state_engine) as session:
        row = session.query(BatchState).filter_by(table_name="t_retry").first()
        assert row.retry_count == 2
        assert row.last_failed_reason == "oops"
        assert row.status == "failed"
