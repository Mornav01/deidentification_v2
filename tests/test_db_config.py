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


def test_pii_db_connection_strings_built_from_destination_db_with_at_password():
    """pii_db uses destination_db credentials; a '@' in the password must be
    encoded (not misparsed into a bogus host like '2025@localhost')."""
    cfg = _deid_config(
        destination_db=_db_config(database="dest", password="Nd@2025"),
        pii_db={"master_db_name": "master_sep", "secondary_pii_db_name": "master_sep"},
    )
    for key, db_name in (
        ("master_connection_str", "master_sep"),
        ("secondary_pii_connection_str", "master_sep"),
    ):
        parsed = make_url(cfg.pii_db[key])
        assert parsed.host == "localhost", f"{key} host misparsed: {cfg.pii_db[key]!r}"
        assert parsed.username == "myuser"
        assert parsed.password == "Nd@2025"
        assert parsed.database == db_name


def test_pii_db_none_is_left_untouched():
    cfg = _deid_config()
    assert cfg.pii_db is None


def test_pii_db_raw_connection_str_without_db_name_is_preserved():
    """Legacy configs supplying a raw *_connection_str and no *_db_name are not
    a regression: the value is passed through unchanged."""
    raw = "mysql+pymysql://u:p@localhost:3306/master_sep"
    cfg = _deid_config(pii_db={"master_connection_str": raw})
    assert cfg.pii_db["master_connection_str"] == raw
