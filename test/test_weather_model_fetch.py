import pytest
from datetime import datetime
from sqlalchemy import create_engine, insert, select
from sqlalchemy.orm import Session

from lib.weather_model import Base, WeatherModel

@pytest.fixture
def basic_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)  # creates tables from your model definitions

    with Session(engine) as session:
        session.execute(insert(WeatherModel).values(
            station_code="KOAK",
            datetime_dt=datetime.fromisoformat("2024-01-15T08:00:00"),
            temp_f=58.0,
            pressure_inhg=10,
            twentyfourhr_max_f=None,
        ))
        session.execute(insert(WeatherModel).values(
            station_code="KOAK",
            datetime_dt=datetime.fromisoformat("2024-01-15T12:00:00"),
            temp_f=58.0,
            pressure_inhg=None,
            twentyfourhr_max_f=100.0,
        ))
        session.execute(insert(WeatherModel).values(
            station_code="KOAK",
            datetime_dt=datetime.fromisoformat("2024-01-15T16:00:00"),
            temp_f=58.0,
            pressure_inhg=20,
            twentyfourhr_max_f=None,
        ))
        session.commit()
        yield session

def test_fetch_from_real_database():
    engine = create_engine("sqlite:///weather.testing.db")
    with Session(engine) as ses:
        stmt = select(WeatherModel).where(WeatherModel.temp_f > 90)
        results = ses.scalars(stmt).all()
        obj = results[0]
        assert obj is not None
        assert isinstance(obj.datetime_dt, datetime)


def test_fetch_one_observation(basic_session):
    stmt = select(WeatherModel).where(WeatherModel.temp_f < 90)
    results = basic_session.scalars(stmt).all()
    obj = results[0]
    assert obj is not None
    assert obj.station_code == "KOAK"
    assert isinstance(obj.datetime_dt, datetime)  # confirms datetime converter from string/to string is working
    assert obj.temp_f == 58.0

#=== Fixtures that vary ===

@pytest.fixture(params=[
    # Missing middle value means we should interpolate between the two known values
    {"pressure_inhg": (10, None, 20), "test_index": 1, "expected_interp": 15},
    # Missing last value means we should extrapolate forwards
    {"pressure_inhg": (10, 20, None), "test_index": 2, "expected_interp": 30},
    # Missing first value means we should extrapolate backwards
    {"pressure_inhg": (None, 20, 30), "test_index": 0, "expected_interp": 10},
])
def interp_session(request):
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    p_tup = request.param["pressure_inhg"]

    with Session(engine) as session:
        session.execute(insert(WeatherModel).values(
            station_code="KOAK",
            datetime_dt=datetime.fromisoformat("2024-01-15T08:00:00"),
            temp_f=58.0,
            pressure_inhg=p_tup[0],
            twentyfourhr_max_f=None,
        ))
        session.execute(insert(WeatherModel).values(
            station_code="KOAK",
            datetime_dt=datetime.fromisoformat("2024-01-15T12:00:00"),
            temp_f=58.0,
            pressure_inhg=p_tup[1],
            twentyfourhr_max_f=None,
        ))
        session.execute(insert(WeatherModel).values(
            station_code="KOAK",
            datetime_dt=datetime.fromisoformat("2024-01-15T16:00:00"),
            temp_f=58.0,
            pressure_inhg=p_tup[2],
            twentyfourhr_max_f=None,
        ))
        session.commit()
        yield session, request.param  # yield both so the test can access expected values


def test_linear_interp_value_between_returns_interpolated(interp_session):
    session, params = interp_session
    stmt = select(WeatherModel)
    results = session.scalars(stmt).all()
    obj = results[params["test_index"]]
    assert obj.interp_pressure_inhg() == params["expected_interp"]