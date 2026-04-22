import datetime
from typing import List
from typing import Optional
from sqlalchemy import ForeignKey
from sqlalchemy import String, Text, Integer, Float
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from sqlalchemy.types import TypeDecorator
from sqlalchemy import select, inspect

from lib.interpolators.linear import LinearInterpolated
from lib.interpolators.previous import PreviousValueInterpolated
from lib.interpolators.default import DefaultValueInterpolated

class Base(DeclarativeBase):
    pass

class ISO8601DateTime(TypeDecorator):
    impl = Text  # stored as TEXT in SQLite
    cache_ok = True

    def process_result_value(self, value, dialect):
        """DB -> Python: called when reading from the database."""
        if value is None:
            return None
        return datetime.datetime.fromisoformat(value)

    def process_bind_param(self, value, dialect):
        """Python -> DB: called when writing to the database."""
        if value is None:
            return None
        return value.isoformat()




class WeatherModel(Base):
    """
    A SqlAlchemy model for working with our database.
    For now, we're using our legacy database - the definition of which is created with raw SQL in `compile_to_sqlite.py`. Eventually it might be worth migrating this to have the entire thing mapped to sqlalchemy, but I wanted to start gathering the data and putting it in *some* db asap in order to have data to migrate from...
    """
    __tablename__ = "weather"

    id: Mapped[int] = mapped_column(primary_key=True)
    station_code: Mapped[str] = mapped_column(String(30))
    datetime_dt: Mapped[datetime] = mapped_column(ISO8601DateTime)
    temp_f: Mapped[float | None]
    dewpoint_f: Mapped[float | None]
    rel_humidity_pct: Mapped[int | None]
    heat_index_f: Mapped[int | None]
    wind_chill_f: Mapped[int | None]
    wind_direction_t: Mapped[str | None]
    wind_speed_mph: Mapped[int | None]
    wind_gust_mph: Mapped[int | None]
    visibility_m: Mapped[float | None]
    weather_t: Mapped[str | None]
    clouds_t: Mapped[str | None]
    pressure_sea_mb: Mapped[float | None]
    pressure_inhg: Mapped[float | None]
    altimiter_setting_inhg: Mapped[float | None]
    accumulated_precip_in: Mapped[float | None]
    onehr_precip_in: Mapped[float | None]
    threehr_precip_in: Mapped[float | None]
    sixhr_precip_in: Mapped[float | None]
    twentyfourhr_precip_in: Mapped[float | None]
    sixhr_max_f: Mapped[int | None]
    sixhr_min_f: Mapped[int | None]
    twentyfourhr_max_f: Mapped[int | None]
    twentyfourhr_min_f: Mapped[int | None]

    def __repr__(self) -> str:
        return f"WeatherModel(id={self.id!r}, station_code={self.station_code!r}, datetime={self.datetime_dt!r}, temp_f={self.temp_f!r} ...)"

    @LinearInterpolated("heat_index_f")
    def interp_heat_index_f(self):
        # By convention, the interpolation functions should return their underlying value for use in the interpoator. As this value for interpolatable fields might be null, we trust that the interpolator will do its best to make that null a not-null value.
        return self.heat_index_f
    
    @LinearInterpolated("pressure_inhg")
    def interp_pressure_inhg(self):
        return self.pressure_inhg

    @PreviousValueInterpolated("twentyfourhr_max_f")
    def interp_twentyfourhr_max_f(self):
        return self.twentyfourhr_max_f  

    @DefaultValueInterpolated("visibility_m", default_value=10.0)
    def interp_visibility_m(self):
        return self.visibility_m



"""

The Embedding layer can be understood as a lookup table that maps from integer indices (which stand for specific words) to dense vectors (their embeddings).

https://www.tensorflow.org/text/guide/word_embeddings#using_the_embedding_layer

So I should take the categorical fields and convert them to integer indices, then use an embedding layer to convert those to dense vectors. The following link gives some information:

https://mmuratarat.github.io/2019-06-12/embeddings-with-numeric-variables-Keras

I need to construct a full vector (as above) for each item, then split the vector into the categorical and continuous parts, run the categorical part through the embedding layer, and then concatenate the embedded categorical part with the continuous part to get the full vector for that item.

At what point should I / would it be good to do principal component analysis? I think it would be after embedding, when they are concatenated, and then there might be a PCA layer in Keras?

"""