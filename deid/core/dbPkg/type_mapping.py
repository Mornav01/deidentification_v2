"""MSSQL → MySQL type mapping for de-identification pipeline.

When the source database is MSSQL and the destination is MySQL, reflected
column types must be converted to MySQL-compatible types. MSSQL types like
UNIQUEIDENTIFIER, VARCHAR(MAX), DATETIME2, etc. have no direct MySQL equivalent.

Connection: Use mssql+pymssql (no ODBC driver required) or mssql+pyodbc.
  mssql+pymssql://user:password@host:port/database
"""
from sqlalchemy import types as sa_types
from sqlalchemy.dialects.mysql import (
    BIGINT,
    CHAR,
    DATETIME,
    DATE,
    DECIMAL,
    DOUBLE,
    FLOAT,
    INTEGER,
    LONGBLOB,
    LONGTEXT,
    SMALLINT,
    TEXT as MYSQL_TEXT,
    TIME,
    TINYINT,
    VARBINARY,
    VARCHAR,
)
from sqlalchemy.dialects.mssql import (
    BIT,
    DATETIME2,
    DATETIMEOFFSET,
    IMAGE,
    MONEY,
    NCHAR,
    NTEXT,
    NVARCHAR,
    REAL,
    SMALLDATETIME,
    SMALLMONEY,
    TEXT as MSSQL_TEXT,
    TINYINT as MSSQL_TINYINT,
    UNIQUEIDENTIFIER,
    VARCHAR as MSSQL_VARCHAR,
)


def _is_unbounded_length(length) -> bool:
    """True if `length` is one of the sentinel values MSSQL reflection uses for
    VARCHAR(MAX)/NVARCHAR(MAX), rather than a real declared column width."""
    if length is None or length == "max":
        return True
    if isinstance(length, int) and (length < 0 or length >= 65535):
        return True
    return False


def _buffered_varchar_length(length: int) -> int:
    """Add a safety margin to a reflected source VARCHAR/NVARCHAR length.

    Destination tables are sized once from the source's declared column width,
    with no later ALTER if source data grows or de-identification placeholders
    (e.g. ``<<COLUMN>>`` masks) push a value past that width — the next insert
    then fails with MySQL error 1406 ("Data too long"). A fixed margin absorbs
    that drift without discarding the source's real declared width the way
    forcing everything to LONGTEXT would.
    """
    return length + max(20, length // 4)


def mssql_type_to_mysql(source_type) -> "sa_types.TypeEngine":
    """Map an MSSQL-reflected column type to a MySQL-compatible type.

    Used when creating the destination table in MySQL from a source table
    reflected from MSSQL. Returns a MySQL dialect type.
    """
    if source_type is None:
        return VARCHAR(255)

    # Exact type matches (MSSQL dialect types)
    type_cls = type(source_type)

    if type_cls is UNIQUEIDENTIFIER:
        return CHAR(36)
    if type_cls is BIT:
        return TINYINT(1)
    if type_cls is MSSQL_TINYINT:
        # MSSQL tinyint is always 0-255 (there is no signed tinyint in T-SQL) —
        # map to an unsigned MySQL TINYINT, not the generic signed Integer
        # fallback below, which would let values >127 overflow on insert.
        return TINYINT(unsigned=True)
    if type_cls is DATETIME2:
        return DATETIME(fsp=getattr(source_type, "precision", 6) or 6)
    if type_cls is SMALLDATETIME:
        return DATETIME()
    if type_cls is DATETIMEOFFSET:
        return DATETIME(fsp=6)  # lose timezone
    if type_cls is MONEY:
        return DECIMAL(19, 4)
    if type_cls is SMALLMONEY:
        return DECIMAL(10, 4)
    if type_cls is REAL:
        return FLOAT()
    if type_cls is NTEXT or type_cls is IMAGE:
        return LONGTEXT()
    if type_cls is MSSQL_TEXT:
        # MSSQL's deprecated TEXT type is inherently unbounded (~2GB), but SQL
        # Server's catalog (sys.columns.max_length) always reports a bogus
        # length of 16 for it — the size of the legacy internal text-pointer
        # structure, not real capacity — so `.length` can't be trusted here.
        # MySQL has a native TEXT type; use it instead of forcing LONGTEXT.
        return MYSQL_TEXT()
    if type_cls is NCHAR:
        length = getattr(source_type, "length", None) or 255
        return CHAR(min(length, 255))
    if type_cls is NVARCHAR:
        length = getattr(source_type, "length", None)
        # NVARCHAR(MAX) → LONGTEXT; any real declared width is preserved (+ buffer)
        if _is_unbounded_length(length):
            return LONGTEXT()
        return VARCHAR(min(_buffered_varchar_length(length), 16383))  # MySQL VARCHAR max ~16383 utf8mb4
    if type_cls is MSSQL_VARCHAR:
        length = getattr(source_type, "length", None)
        # VARCHAR(MAX) → LONGTEXT; any real declared width is preserved (+ buffer)
        if _is_unbounded_length(length):
            return LONGTEXT()
        return VARCHAR(min(_buffered_varchar_length(length), 16383))

    # Generic SQLAlchemy types (may come from reflection). Note: pyodbc reflects
    # MSSQL VARCHAR(MAX)/NVARCHAR(MAX) columns as a *generic* sqlalchemy.sql
    # VARCHAR (length=None), not the mssql-dialect VARCHAR matched above — so
    # this branch, not the ones above, is what most MAX-width free-text columns
    # (e.g. Messages.Body) actually hit.
    if isinstance(source_type, (sa_types.String, sa_types.Text)):
        length = getattr(source_type, "length", None)
        if _is_unbounded_length(length):
            return LONGTEXT()
        return VARCHAR(min(_buffered_varchar_length(length), 16383))
    if isinstance(source_type, sa_types.Integer):
        if isinstance(source_type, sa_types.BigInteger):
            return BIGINT()
        if isinstance(source_type, sa_types.SmallInteger):
            return SMALLINT()
        return INTEGER()
    if isinstance(source_type, sa_types.Float):
        return DOUBLE()
    if isinstance(source_type, sa_types.Numeric):
        # `or` would silently discard a real precision/scale of 0 (falsy) and
        # substitute the fallback default — e.g. NUMERIC(18,0), a whole-number
        # ID column, would wrongly become DECIMAL(18,2). Check `is None` instead
        # so an explicit 0 (no fractional digits) is preserved.
        p = source_type.precision if source_type.precision is not None else 18
        s = source_type.scale if source_type.scale is not None else 2
        # DECIMAL(p,s) allows only (p-s) digits before decimal. Values like 635264369268131824
        # (18 digits) overflow DECIMAL(18,2). Use min precision 20 when s>=2 so 18-digit
        # integers fit.
        if s >= 2 and p <= 18:
            p = max(p, 20)
        return DECIMAL(p, s)
    if isinstance(source_type, sa_types.DateTime):
        return DATETIME()
    if isinstance(source_type, sa_types.Date):
        return DATE()
    if isinstance(source_type, sa_types.Time):
        return TIME()
    if isinstance(source_type, sa_types.Boolean):
        return TINYINT(1)
    if isinstance(source_type, sa_types.LargeBinary):
        return LONGBLOB()
    if isinstance(source_type, sa_types.Binary):
        length = getattr(source_type, "length", None)
        return VARBINARY(length or 255)

    # Fallback: try to preserve length for string-like (e.g. VARCHAR(2147483647) from MSSQL)
    try:
        length = getattr(source_type, "length", None)
        if length and isinstance(length, int):
            if _is_unbounded_length(length):
                return LONGTEXT()  # VARCHAR(MAX) sentinel → LONGTEXT
            return VARCHAR(min(_buffered_varchar_length(length), 16383))
    except Exception:
        pass
    return VARCHAR(255)
