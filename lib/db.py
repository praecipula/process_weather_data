from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session

engine = create_engine("sqlite:///weather.testing.db")

# Enforce read-only at the connection level
@event.listens_for(engine, "connect")
def set_readonly(dbapi_conn, connection_record):
    dbapi_conn.execute("PRAGMA query_only = ON")

session = Session(engine)