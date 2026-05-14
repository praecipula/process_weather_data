#!/usr/bin/env python

from pathlib import Path
import json
from lxml import etree, html
import sqlite3
from datetime import datetime, timezone
import re
import pdb
import argparse
import inspect
import logging

LOG_FILE = Path("ingestion.log")
LOG = logging.getLogger("clean")

def setup_logging(verbosity):
    handlers = [logging.FileHandler(LOG_FILE)]
    if verbosity > 0:
        handlers.append(logging.StreamHandler())
    
    level = logging.INFO
    if verbosity >= 2:
        level = logging.DEBUG
        
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=handlers
    )

here = Path(".")
jsonfiles = here.glob("./input_scrapes/**/*.json")

db = sqlite3.connect("./weather.db")
cursor = db.cursor()

cursor.execute("""CREATE TABLE IF NOT EXISTS weather (
    id INTEGER PRIMARY KEY,
    station_code TEXT,
    datetime_dt TEXT,
    temp_f REAL,
    dewpoint_f REAL,
    rel_humidity_pct INTEGER,
    heat_index_f INTEGER,
    wind_chill_f INTEGER,
    wind_direction_t TEXT,
    wind_speed_mph INTEGER,
    wind_gust_mph INTEGER,
    visibility_m REAL,
    weather_t TEXT,
    clouds_t TEXT,
    pressure_sea_mb REAL,
    pressure_inhg REAL,
    altimiter_setting_inhg REAL,
    accumulated_precip_in REAL,
    onehr_precip_in REAL,
    threehr_precip_in REAL,
    sixhr_precip_in REAL,
    twentyfourhr_precip_in REAL,
    sixhr_max_f INTEGER,
    sixhr_min_f INTEGER,
    twentyfourhr_max_f INTEGER, 
    twentyfourhr_min_f INTEGER
  )""")

cursor.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS idx_stn_time ON weather (station_code, datetime_dt)
  """)

ERROR_FILES = []
EMPTY_FILES = []

class TableDialectBase:
    def __init__(self, station_code):
        self._station_code = station_code

    def _none_like_element(self, text):
        if text == "--" or not text.strip():
            return True
        return False

    def _parse_float(self, text):
        if self._none_like_element(text): return None
        match = re.match(r"^[\d\.]+", text)
        if match: return float(match.group())
        return 0.01 if text == 'T' else None

    def _parse_int(self, text):
        if self._none_like_element(text): return None
        match = re.match(r"^-*[\d]+", text)
        return int(match.group()) if match else None

    def _parse_str(self, text):
        t = text.strip()
        return t if t else None

class NWSTableDialect(TableDialectBase):
    def __init__(self, json_object, file_dt, station_code):
        super().__init__(station_code)
        self._json_object = json_object
        self._file_dt = file_dt
        self._dbFieldToHeaderStringAndConverterMapping = {
            'datetime_dt': (['Date/Time\xa0(L)', 'Date/Time\xa0'], self._parse_datetime),
            'temp_f': (['Temp.\xa0(°F)', 'Temp.\xa0'], self._parse_int),
            'dewpoint_f': (['DewPoint(°F)', 'DewPoint'], self._parse_int),
            'rel_humidity_pct': (['RelativeHumidity(%)', 'RelativeHumidity'], self._parse_int),
            'heat_index_f': (['HeatIndex(°F)', 'HeatIndex'], self._parse_int),
            'wind_chill_f': (['WindChill(°F)', 'WindChill'], self._parse_int),
            'wind_direction_t': (['WindDirection\xa0', 'WindDirection'], self._parse_str),
            'wind_speed_mph': (['WindSpeed(mph)', 'WindSpeed'], self._parse_wind_speed),
            'wind_gust_mph': ([], None),
            'visibility_m': (['Visibility\xa0(miles)', 'Visibility\xa0'], self._parse_str),
            'weather_t': (['Weather\xa0\xa0', 'Weather\xa0'], self._parse_str),   
            'clouds_t': (['Clouds\xa0(x100 ft)', 'Clouds\xa0'], self._parse_str),
            'pressure_sea_mb': (['Sea LevelPressure(mb)', 'Sea LevelPressure'], self._parse_float),
            'pressure_inhg': (['StationPressure(in Hg)'], self._parse_float),
            'altimiter_setting_inhg': (['AltimeterSetting(in Hg)', 'AltimeterSetting'], self._parse_float),
            'accumulated_precip_in': (['Accumulated Precip'], self._parse_float),
            'onehr_precip_in': (['1 HourPrecip', '1 HourPrecip(in)'], self._parse_float),
            'threehr_precip_in': (['3 HourPrecip', '3 HourPrecip(in)'], self._parse_float),
            'sixhr_precip_in': (['6 HourPrecip', '6 HourPrecip(in)'], self._parse_float),
            'twentyfourhr_precip_in': (['24 HourPrecip', '24 HourPrecip(in)'], self._parse_float),
            'sixhr_max_f': (['6 HrMax(°F)', '6 HrMax'], self._parse_int),
            'sixhr_min_f': (['6 HrMin(°F)', '6 HrMin'], self._parse_int),
            'twentyfourhr_max_f': (['24 HrMax(°F)', '24 HrMax'], self._parse_int),
            'twentyfourhr_min_f': (['24 HrMin(°F)', '24 HrMin'], self._parse_int)
        }
        self._fieldNamesArray = []
        self._convertersArray = []
        self._mappingToListsAccordingToHeaders()
        
    def _parse_datetime(self, text):
        from zoneinfo import ZoneInfo
        tz = ZoneInfo("America/Los_Angeles")
        dt_no_year = datetime.strptime(text, "%b %d, %I:%M %p")
        inferred_year = self._file_dt.year
        if self._file_dt.month <= 6 and dt_no_year.month > 6:
            inferred_year -= 1
        dt_naive = datetime.strptime(f"{inferred_year} {text}", "%Y %b %d, %I:%M %p")
        # Return aware UTC datetime
        return dt_naive.replace(tzinfo=tz).astimezone(timezone.utc)

    def _parse_wind_speed(self, text):
        if self._none_like_element(text): return None
        match = re.match(r"(.*)[gG](.*)", text)
        if not match: return (int(text), None)
        g = match.groups()
        return (int(g[0]), int(g[1]))

    def _mappingToListsAccordingToHeaders(self):
        element = html.fromstring(self._json_object[0]['rows'][0])
        for header_as_str in [e.xpath("string()") for e in element.xpath(".//th|td")]:
            found = False
            for db_field, (headers, conv) in self._dbFieldToHeaderStringAndConverterMapping.items():
                if header_as_str in headers:
                    self._fieldNamesArray.append(db_field)
                    self._convertersArray.append(conv)
                    found = True
                    break
            if not found: raise Exception(f"Unrecognized header: {header_as_str}")

    def query_keys_and_values(self):
        for row in self._json_object[0]['rows'][1:]:
            cleaned = row.replace("&nbsp;", "&#160;")
            root = html.fromstring(f"<root>{cleaned}</root>")
            elements = root.xpath("//td")
            if not elements or elements[0].xpath("string()").strip() == "(L)": continue
            keys, values = [], []
            try:
                for i, el in enumerate(elements):
                    if i >= len(self._fieldNamesArray) or self._fieldNamesArray[i] is None: continue
                    val_str = el.xpath("string()").strip()
                    conv = self._convertersArray[i]
                    converted = conv(val_str) if conv else None
                    if isinstance(converted, tuple):
                        keys.extend([self._fieldNamesArray[i], 'wind_gust_mph'])
                        values.extend([converted[0], converted[1]])
                    else:
                        keys.append(self._fieldNamesArray[i])
                        values.append(converted)
                if 'temp_f' in keys and values[keys.index('temp_f')] is not None:
                    yield (keys, values)
            except Exception as e:
                LOG.warning(f"Row parse error: {e}")

class WundergroundTableDialect(TableDialectBase):
    def __init__(self, root_element, station_code):
        super().__init__(station_code)
        tables = root_element.xpath(".//*[contains(@class, 'desktop-table')]")
        if not tables:
            raise Exception("Required 'desktop-table' element not found in Wunderground scrape.")
        self._root_element = tables[0]
        
        self._dbFieldToHeaderStringAndConverterMapping = {
            'datetime_dt': (['Time'], self._parse_datetime),
            'temp_f': (['Temperature'], self._parse_float),
            'dewpoint_f': (['Dew Point'], self._parse_float),
            'rel_humidity_pct': (['Humidity'], self._parse_int),
            'wind_direction_t': (['Wind'], self._parse_str),
            'wind_speed_mph': (['Speed'], self._parse_float),
            'wind_gust_mph': (['Gust'], self._parse_float),
            'pressure_inhg': (['Pressure'], self._parse_float),
            None: (['Precip. Rate.', 'Precip. Accum.', 'UV', 'Solar'], None)
        }
        self._fieldNamesArray, self._convertersArray = [], []
        self._mappingToListsAccordingToHeaders()
        h3_elements = self._root_element.xpath("//h3") or self._root_element.xpath("preceding::h3[1]")
        if not h3_elements:
            raise Exception("Required date header (h3) not found in Wunderground scrape.")
        
        h3 = h3_elements[0].xpath("string()")
        self._date = datetime.strptime(h3, "%B %d, %Y")

    def _parse_datetime(self, text):
        from zoneinfo import ZoneInfo
        time_parsed = datetime.strptime(text, "%I:%M %p")
        dt_naive = datetime.combine(self._date, time_parsed.time())
        return dt_naive.replace(tzinfo=ZoneInfo("America/Los_Angeles")).astimezone(timezone.utc)

    def _mappingToListsAccordingToHeaders(self):
        for header_as_str in [e.xpath("string()") for e in self._root_element.xpath(".//th")]:
            found = False
            for db_field, (headers, conv) in self._dbFieldToHeaderStringAndConverterMapping.items():
                if header_as_str in headers:
                    self._fieldNamesArray.append(db_field)
                    self._convertersArray.append(conv)
                    found = True
                    break
            if not found: raise Exception(f"Unrecognized header: {header_as_str}")

    def query_keys_and_values(self):
        for row in self._root_element.xpath(".//tr")[1:]:
            if not row.xpath("string()").strip(): continue
            keys, values = [], []
            try:
                for i, el in enumerate(row.xpath(".//td")):
                    if i >= len(self._fieldNamesArray) or self._fieldNamesArray[i] is None: continue
                    keys.append(self._fieldNamesArray[i])
                    conv = self._convertersArray[i]
                    values.append(conv(el.xpath("string()").strip()) if conv else None)
                yield (keys, values)
            except Exception as e:
                LOG.warning(f"Row parse error: {e}")

def createTableParser(json_object, file, station_code):
    try:
        stem = file.stem.replace("_", ":")
        file_dt = datetime.fromisoformat(stem)
    except:
        file_dt = datetime.now()

    if 'body' in json_object[0]:
        root = html.fromstring(f"<root>{json_object[0]['body']['message']}</root>")
        if len(root.xpath("//h3")) >= 1:
            return WundergroundTableDialect(root, station_code)
    elif 'rows' in json_object[0]:
        if not json_object[0]['rows']:
            EMPTY_FILES.append(file)
            return None
        return NWSTableDialect(json_object, file_dt, station_code)
    
    ERROR_FILES.append(file)
    return None

def process():
    inserted = 0
    LOG.info("Starting ingestion processing...")
    for file in jsonfiles:
        station_code = file.parent.name
        try:
            with open(file, 'r') as f: obj = json.load(f)
            parser = createTableParser(obj, file, station_code)
            if not parser: continue
            
            rows_in_file = 0
            for keys, values in parser.query_keys_and_values():
                # Strictly format datetimes as ISO 8601 with 'T' separator and UTC offset
                for i, val in enumerate(values):
                    if isinstance(val, datetime):
                        values[i] = val.isoformat()

                keys.append("station_code")
                values.append(station_code)
                placeholders = ", ".join(["?"] * len(values))
                stmt = f"INSERT INTO weather ({', '.join(keys)}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"
                cursor.execute(stmt, values)
                if cursor.rowcount > 0: 
                    inserted += 1
                    rows_in_file += 1
            
            if rows_in_file > 0:
                LOG.debug(f"File {file.name}: Inserted {rows_in_file} rows.")
            
            db.commit()
        except Exception as e:
            LOG.error(f"File {file} failed: {e}")
            ERROR_FILES.append(file)

    LOG.info(f"Ingestion complete. Inserted {inserted} new records.")
    LOG.info(f"Skipped {len(EMPTY_FILES)} empty scrape files.")
    if ERROR_FILES:
        LOG.warning(f"Failed to parse {len(ERROR_FILES)} files. See ingestion.log for details.")
        with open(LOG_FILE, "a") as f:
            f.write("\n--- Failed Files ---\n")
            f.write("\n".join([str(p.resolve()) for p in ERROR_FILES]) + "\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-v', '--verbose', action='count', default=0, help="Print to stdout (-v for INFO, -vv for DEBUG)")
    args = parser.parse_args()
    setup_logging(args.verbose)
    process()
