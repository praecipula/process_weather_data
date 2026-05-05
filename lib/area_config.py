"""
area_config.py
==============
Geographic area and station configuration for the weather prediction model.
"""

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class Station:
    """
    Geographic and physical metadata for a weather station.
    """
    station_code: str
    distance_from_primary_km: float
    is_coastal: bool
    elevation_m: float
    is_primary: bool = False
    station_type: str = "metar"  # "metar" | "wunderground"


@dataclass(frozen=True)
class AreaConfig:
    """
    Configuration for one geographic prediction area.

    name:               human-readable area name
    area_key:           short slug used as dict key and in file paths
    primary_station:    ICAO code of the station we're predicting
    stations:           list of Station (must include primary)
    kalshi_ticker:      Kalshi series ticker for this area's high-temp market
    temp_clim_std_f:    typical std dev of daily high temp anomaly
    anomaly_bucket_min_f: lowest temperature anomaly bucket for softmax output
    anomaly_bucket_max_f: highest temperature anomaly bucket for softmax output

    Note: anomaly_bucket range should cover all plausible deviations from normal.
    """
    name: str
    area_key: str
    primary_station: str
    stations: list[Station]
    kalshi_ticker: str
    temp_clim_std_f: float = 10.0
    anomaly_bucket_min_f: int = -25
    anomaly_bucket_max_f: int = 25

    @property
    def temp_buckets(self) -> int:
        """Total number of 1-degree anomaly buckets."""
        return self.anomaly_bucket_max_f - self.anomaly_bucket_min_f + 1

    @property
    def station_index_map(self) -> dict:
        """Returns {station_code: int_index} for embedding lookup."""
        return {s.station_code: i for i, s in enumerate(self.stations)}

    @property
    def num_stations(self) -> int:
        return len(self.stations)

    def get_station(self, code: str) -> Optional[Station]:
        for s in self.stations:
            if s.station_code == code:
                return s
        return None


# =============================================================================
# AREA DEFINITIONS
# =============================================================================

SAN_FRANCISCO = AreaConfig(
    name="San Francisco Bay Area",
    area_key="sfbay",
    primary_station="KSFO",
    kalshi_ticker="KXHIGHSFO",   # TODO: verify actual Kalshi ticker for SFO
    temp_clim_std_f=10.0,
    anomaly_bucket_min_f=-20,    # SFO rarely deviates more than 20F from normal
    anomaly_bucket_max_f=20,
    stations=[
        Station("KSFO",       0.0,  True,  4.0,  is_primary=True),
        Station("KCASANBR16", 12.0, False, 30.0, station_type="wunderground"),
        Station("KCABRISB41", 10.0, False, 20.0, station_type="wunderground"),
        Station("KCABURLI72", 25.0, False, 15.0, station_type="wunderground"),
    ],
)

# Registry: add area configs here to enable them
AREAS: dict = {
    "sfbay": SAN_FRANCISCO,
}


def get_area(area_key: str) -> AreaConfig:
    """Retrieve an AreaConfig by key. Raises KeyError if not found."""
    if area_key not in AREAS:
        raise KeyError(
            f"Unknown area '{area_key}'. "
            f"Available: {list(AREAS.keys())}"
        )
    return AREAS[area_key]
