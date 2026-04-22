import pytest
from datetime import datetime, timedelta
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

#=== Linear interpolation ===

@pytest.fixture(params=[
    # Missing middle value means we should interpolate between the two known values
    {"pressure_inhg": (10, None, 20), "test_index": 1, "expected_interp": 15},
    # Missing last value means we should extrapolate forwards
    {"pressure_inhg": (10, 20, None), "test_index": 2, "expected_interp": 30},
    # Missing first value means we should extrapolate backwards
    {"pressure_inhg": (None, 20, 30), "test_index": 0, "expected_interp": 10},
])
def linear_interp_session(request):
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    p_tup = request.param["pressure_inhg"]

    with Session(engine) as session:
        for i, val in enumerate(p_tup):
            session.execute(insert(WeatherModel).values(
                station_code="KOAK",
                datetime_dt=datetime.fromisoformat("2024-01-15T08:00:00") + timedelta(hours=i),
                temp_f=58.0,
                pressure_inhg=val,
            ))
            session.commit()
        yield session, request.param  # yield both so the test can access expected values


def test_linear_interp_value_between_returns_interpolated(linear_interp_session):
    session, params = linear_interp_session
    stmt = select(WeatherModel)
    results = session.scalars(stmt).all()
    obj = results[params["test_index"]]
    assert obj.interp_pressure_inhg() == params["expected_interp"]

# === Previous value interpolation ===

@pytest.fixture(params=[
    # Present value means just use that value
    {"twentyfourhr_max_f": (10, 20, 30), "test_index": 1, "expected_interp": 20},
    # Missing value means we should extrapolate backwards to the last time there was a value
    {"twentyfourhr_max_f": (10, None, 20), "test_index": 1, "expected_interp": 10},
    # No previous value yields None
    {"twentyfourhr_max_f": (None, None, 20), "test_index": 1, "expected_interp": None},
])
def prev_interp_session(request):
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    p_tup = request.param["twentyfourhr_max_f"]

    with Session(engine) as session:
        for i, val in enumerate(p_tup):
            session.execute(insert(WeatherModel).values(
                station_code="KOAK",
                datetime_dt=datetime.fromisoformat("2024-01-15T08:00:00") + timedelta(hours=i),
                temp_f=58.0,
                twentyfourhr_max_f=val,
            ))
            session.commit()
        yield session, request.param  # yield both so the test can access expected values


def test_previous_interp_returns_interpolated(prev_interp_session):
    session, params = prev_interp_session
    stmt = select(WeatherModel)
    results = session.scalars(stmt).all()
    obj = results[params["test_index"]]
    print(obj)
    assert obj.interp_twentyfourhr_max_f() == params["expected_interp"]

# === Default value interpolation ===

@pytest.fixture(params=[
    # Present value means just use that value
    {"visibility_m": (2.0, 5.0, 10.0), "test_index": 1, "expected_interp": 5.0},
    # Missing value means we should extrapolate backwards to the last time there was a value
    {"visibility_m": (2.0, None, 10.0), "test_index": 1, "expected_interp": 10.0},
])
def default_val_interp_session(request):
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    p_tup = request.param["visibility_m"]

    with Session(engine) as session:
        for i, val in enumerate(p_tup):
            session.execute(insert(WeatherModel).values(
                station_code="KOAK",
                datetime_dt=datetime.fromisoformat("2024-01-15T08:00:00") + timedelta(hours=i),
                temp_f=58.0,
                visibility_m=val,
            ))
            session.commit()
        yield session, request.param  # yield both so the test can access expected values


def test_default_val_interp_returns_interpolated(default_val_interp_session):
    session, params = default_val_interp_session
    stmt = select(WeatherModel)
    results = session.scalars(stmt).all()
    obj = results[params["test_index"]]
    print(obj)
    assert obj.interp_visibility_m() == params["expected_interp"]
