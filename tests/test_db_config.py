"""Tests for DbConfig connection-string escaping and state/failed-rows DB resolution."""
import pytest
from sqlalchemy.engine import make_url


def _db_config(**overrides):
    from deid.config.schema import DbConfig

    defaults = dict(
        type="mysql",
        host="localhost",
        port=3306,
        database="mydb",
        username="myuser",
        password="secret",
    )
    defaults.update(overrides)
    return DbConfig(**defaults)


@pytest.mark.parametrize(
    "password",
    ["p@ss", "p%ss", "p$ss", "p:ss", "p/ss", "p#ss", "p ss"],
)
def test_connection_string_round_trips_special_characters(password):
    """A password containing URL-special characters must survive intact, not get
    misparsed (e.g. '@' splitting into a bogus host) or silently decoded (e.g. '%xx')."""
    db = _db_config(password=password)
    conn_str = db.connection_string()
    parsed = make_url(conn_str)
    assert parsed.password == password, f"Password corrupted in {conn_str!r}"
    assert parsed.host == "localhost"
    assert parsed.database == "mydb"


def test_connection_string_normal_password_unaffected():
    db = _db_config(password="plainpassword")
    parsed = make_url(db.connection_string())
    assert parsed.username == "myuser"
    assert parsed.password == "plainpassword"
    assert parsed.host == "localhost"
    assert parsed.port == 3306
    assert parsed.database == "mydb"


def _deid_config(**overrides):
    from deid.config.schema import DeidConfig, TableConfig

    defaults = dict(
        source_db=_db_config(database="source"),
        destination_db=_db_config(database="dest", password="p@ss"),
        tables=[TableConfig(name="t1", rules={})],
    )
    defaults.update(overrides)
    return DeidConfig(**defaults)


def test_resolved_state_db_url_defaults_to_sqlite():
    cfg = _deid_config()
    assert cfg.resolved_state_db_url == f"sqlite:///{cfg.state_db_path}"


def test_resolved_failed_rows_db_url_defaults_to_sqlite():
    cfg = _deid_config()
    assert cfg.resolved_failed_rows_db_url == f"sqlite:///{cfg.failed_rows_db_path}"


def test_resolved_state_db_url_reuses_destination_db_credentials():
    cfg = _deid_config(state_db_name="deid_state")
    parsed = make_url(cfg.resolved_state_db_url)
    assert parsed.host == "localhost"
    assert parsed.username == "myuser"
    assert parsed.password == "p@ss"
    assert parsed.database == "deid_state"


def test_resolved_failed_rows_db_url_reuses_destination_db_credentials():
    cfg = _deid_config(failed_rows_db_name="deid_failed")
    parsed = make_url(cfg.resolved_failed_rows_db_url)
    assert parsed.host == "localhost"
    assert parsed.username == "myuser"
    assert parsed.password == "p@ss"
    assert parsed.database == "deid_failed"
