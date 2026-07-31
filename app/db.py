from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
import os

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set")

engine_options = {"pool_pre_ping": True}
if DATABASE_URL.startswith("sqlite") and ":memory:" in DATABASE_URL:
    engine_options.update({"connect_args": {"check_same_thread": False}, "poolclass": StaticPool})

engine = create_engine(DATABASE_URL, **engine_options)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
