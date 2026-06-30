import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

DB_URL = f"mssql+pymssql://{os.environ.get('SA_USER','sa')}:{os.environ.get('SA_PASS','')}@localhost:1433/master"

engine = create_engine(DB_URL, pool_pre_ping=True, pool_timeout=300)
SessionLocal = sessionmaker(bind=engine)

def get_session():
    return SessionLocal()
