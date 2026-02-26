from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

DB_URL = "mssql+pymssql://sa:ndADMIN2025@localhost:1433/master"

engine = create_engine(DB_URL, pool_pre_ping=True, pool_timeout=300)
SessionLocal = sessionmaker(bind=engine)

def get_session():
    return SessionLocal()
