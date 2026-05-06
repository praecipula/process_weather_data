
import pytest
import datetime
from lib.db import session
from lib.area_config import get_area
from lib.input_vector import SequenceBuilder

@pytest.fixture
def builder():
    area = get_area("sfbay")
    return SequenceBuilder(area, session)

def test_filters_extreme_anomaly(builder):
    """
    Verify that 2024-09-28 (15F delta) is filtered out.
    """
    target_date = datetime.date(2024, 9, 28)
    found = False
    
    # Use limit_dates to target ONLY the problematic date
    for _, _, _, meta in builder.iter_training_days(consistency_threshold=5.0, limit_dates=[target_date]):
        if meta["station_code"] == "KSFO" and meta["date"] == target_date.isoformat():
            found = True
            break
            
    assert not found, f"Anomalous date {target_date} was NOT filtered out!"

def test_filters_moderate_anomaly(builder):
    """
    Verify that 2024-01-01 (9F delta) is filtered out.
    """
    target_date = datetime.date(2024, 1, 1)
    found = False
    
    for _, _, _, meta in builder.iter_training_days(consistency_threshold=5.0, limit_dates=[target_date]):
        if meta["station_code"] == "KSFO" and meta["date"] == target_date.isoformat():
            found = True
            break
            
    assert not found, f"Anomalous date {target_date} was NOT filtered out!"

def test_permits_valid_data(builder):
    """
    Verify that a known good date is NOT filtered out.
    We'll pick 2024-09-27 (the day before the anomaly).
    """
    target_date = datetime.date(2024, 9, 27)
    found = False
    
    for _, _, _, meta in builder.iter_training_days(consistency_threshold=5.0, limit_dates=[target_date]):
        if meta["station_code"] == "KSFO" and meta["date"] == target_date.isoformat():
            found = True
            break
            
    assert found, f"Valid date {target_date} was incorrectly filtered out!"
