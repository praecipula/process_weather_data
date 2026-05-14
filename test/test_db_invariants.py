import pytest
import sqlite3
import re
from pathlib import Path

DB_PATH = Path("weather.db")

def check_timestamp_invariant(dt_str, label="timestamp"):
    """
    Invariant:
    1. ISO 8601 format (must have 'T' separator)
    2. Contains a timezone offset
    3. Offset must be +00:00 (UTC)
    Example: 2026-05-12T04:19:00+00:00
    """
    # Regex explains: 
    # ^\d{4}-\d{2}-\d{2}  : Date
    # T                   : Strict ISO separator
    # \d{2}:\d{2}:\d{2}   : Time
    # (\.\d+)?            : Optional microseconds
    # \+00:00$            : Explicit UTC offset (Z is also technically valid but our scripts use +00:00)
    iso_utc_pattern = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?\+00:00$")
    
    assert dt_str is not None, f"Found NULL {label}"
    assert iso_utc_pattern.match(dt_str), (
        f"Invalid {label} format: '{dt_str}'. "
        f"Must be strict ISO 8601 with 'T' separator and +00:00 offset."
    )

@pytest.mark.skipif(not DB_PATH.exists(), reason="weather.db not found")
def test_weather_table_datetime_invariants():
    """Sample 1000 rows from 'weather' and verify UTC invariants."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # We check if the table exists first to avoid confusing errors
    table_check = cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='weather';"
    ).fetchone()
    if not table_check:
        pytest.skip("weather table does not exist yet.")

    rows = cursor.execute(
        "SELECT datetime_dt, station_code FROM weather ORDER BY RANDOM() LIMIT 1000"
    ).fetchall()
    
    assert len(rows) > 0, "Weather table is empty. Run ingestion first."
    
    for dt_str, stn in rows:
        check_timestamp_invariant(dt_str, f"Weather timestamp (Station: {stn})")

@pytest.mark.skipif(not DB_PATH.exists(), reason="weather.db not found")
def test_summary_fcst_table_date_invariants():
    """Sample 1000 rows from 'summary_fcst' and verify UTC invariants."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    table_check = cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='summary_fcst';"
    ).fetchone()
    if not table_check:
        pytest.skip("summary_fcst table does not exist yet.")

    rows = cursor.execute(
        "SELECT date_d, station_code FROM summary_fcst ORDER BY RANDOM() LIMIT 1000"
    ).fetchall()
    
    # It's okay if this is empty if ingest_labels hasn't run yet, 
    # but if there is data, it must be correct.
    for dt_str, stn in rows:
        check_timestamp_invariant(dt_str, f"Summary date (Station: {stn})")

if __name__ == "__main__":
    # Allow running directly as well
    pytest.main([__file__])
