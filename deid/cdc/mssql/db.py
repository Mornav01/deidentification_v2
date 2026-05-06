from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from pydantic import validate_call

DB_URL = "mssql+pymssql://sa:ndADMIN2025@localhost:1433/master"

engine = create_engine(DB_URL, pool_pre_ping=True, pool_timeout=300)
SessionLocal = sessionmaker(bind=engine)

@validate_call(config=dict(arbitrary_types_allowed=True))
def get_session():
    return SessionLocal()
