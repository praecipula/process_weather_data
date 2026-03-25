#!/usr/bin/env python

from pathlib import Path
import json
from lxml import etree, html
import sqlite3
from datetime import datetime
import re
import pdb
import argparse
import inspect

import logging
import python_logging_base
from python_logging_base import ASSERT

LOG = logging.getLogger("clean")


here = Path(".")
jsonfiles = here.glob("./input_scrapes/**/*.json")

# One unified weather database. Each data is in the same table but split by station_code
# This means we have to have a unique index, hmm. Better to shard to separate dbs by station?

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

# Keep track of all errored out files
ERROR_FILES=[]

class TableDialectBase:
    def __init__(self, station_code):
        self._station_code = station_code

    def _none_like_element(self, text):
        if text == "--":
            return True
        if len(text.strip()) == 0:
            return True
        return False

    def _parse_float(self, text):
        if self._none_like_element(text):
            return None
        float_parser = r"^[\d\.]+"
        match = re.match(float_parser, text)
        if match:           
            return float(match.group())
        else:      
            # Special case: if it's just "T" that means "Trace", i.e., for precipitation.
            if text == 'T':
                return 0.01
            else:
                breakpoint()
                return None

    def _parse_int(self, text):
        if self._none_like_element(text):
            return None
        int_parser = r"^-*[\d]+"
        match = re.match(int_parser, text)
        if match:
            return int(match.group())
        else:
            breakpoint()
            return None

    def _parse_str(self, text):
        if self._none_like_element(text):
            return None
        t = text.strip()
        LOG.trace(f"Parsing string value: '{t}'")
        if len(t) > 0:
            return t
        else:
            return None


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
            'wind_gust_mph': ([], None), # This is inferred from the value of the wind speed column, so we don't have a header parsed for it.
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

        self._fieldNamesArray = [] # The array, in order, of the DB-named fields that match the order of the headers in the table.
        self._convertersArray = [] # The appropriate converters, in order, of the value that matches the order of the headers in the table.
        self._mappingToListsAccordingToHeaders()
        
    def _parse_datetime(self, text):
        # Jank, but works to set the year.
        dt_no_year = datetime.strptime(text, "%b %d, %I:%M %p")
        inferred_year = self._file_dt.year
        # If the file was scraped in the first half of the year, but the month of the entry is in the second half,
        # then we have a wrap situation and the recorded year is last year.
        # For instance, scraped in Feb, but parsed date in Nov, we back-scraped past the year boundary.
        # This assumes that we can't set just any window for the data, which is consitent with the website
        # (which asks basically "how many hours ago from now" for query)
        if self._file_dt.month <= 6:
            if dt_no_year.month > 6:
                inferred_year = self._file_dt.year - 1
        # This seems awkward, but the date above doesn't handle leap day correctly, so we reparse from scratch with the inferred year rather than just accepting the date as parsed.
        # https://github.com/python/cpython/issues/70647.
        return datetime.strptime(str(inferred_year) + " " + text, "%Y %b %d, %I:%M %p").isoformat()

    def _parse_wind_speed(self, text):
        # Windspeed optionally has the format \d\dG\d\d, where
        # G in the middle of the string means "gusts up to".
        # So we record those as separate cols.
        if self._none_like_element(text):
            return None
        if text == None:
            return (None, None)
        match = re.match(r"(.*)[gG](.*)", text)
        if not match:
            return (int(text), None)
        else:
            g = match.groups()
            return (int(g[0]), int(g[1]))


    def _mappingToListsAccordingToHeaders(self):
        element = html.fromstring(self._json_object[0]['rows'][0])
        elements = element.xpath(".//th|td")
        self._fieldNamesArray = []
        self._convertersArray = []
        for element in elements:
            found = False
            header_as_str = element.xpath("string()")
            for db_field, mapping_tuple in self._dbFieldToHeaderStringAndConverterMapping.items():
                header_strings = mapping_tuple[0]
                converter = mapping_tuple[1] # The first element of the tuple is the list of possible header strings, the second is the converter function (if any) to apply to the value in that column.
                if header_as_str in header_strings:
                    found = True
                    self._fieldNamesArray.append(db_field)
                    self._convertersArray.append(converter)
                    break
            if not found:
                import pdb; pdb.set_trace()
                raise Exception(f"Unrecognized header string: {header_as_str}")

    
    def query_keys_and_values(self):

        # Skip the first row (header row)
        for row in self._json_object[0]['rows'][1:]:
            # Replace html with their character codes (simplest way to avoid loading a dtd from the network)
            cleaned = row.replace("&nbsp;", "&#160;")
            root = html.fromstring("<root>" + cleaned + "</root>")
            LOG.trace(root.xpath("string()"))
            elements = root.xpath("//td")
            keys_stanza = []
            values_stanza = []
            try:
                # Peek and skip if the row is fishy. Doing this outside of loop so continue does the right skip.
                value_string = elements[0].xpath("string()").strip()
                if value_string == "(L)":
                    LOG.debug(f"Skipping--this is the second row of a dual-row header.")
                    continue
                for i, element in enumerate(elements):
                    value_string = element.xpath("string()").strip()
                    LOG.trace(f"Parsing value: {value_string} for header: {self._fieldNamesArray}")
                    if self._fieldNamesArray[i] is None:
                        LOG.debug(f"Skipping {value_string} as db field is None")
                        continue
                    keys_stanza.append(self._fieldNamesArray[i])
                    converter = self._convertersArray[i]
                    if converter != None: # Keep in mind "None" means "drop this data"
                        converted = converter(value_string)
                    else:
                        converted = None
                    # Magic here because the one parser spits out both wind speed and gust, because there's only one parsed header to extract both.
                    # So, assume that a tuple means "also set the next field" in the mapping.
                    if isinstance(converted, tuple):
                        wind_speed = converted[0]
                        wind_gust = converted[1]
                        values_stanza.append(wind_speed)
                        keys_stanza.append('wind_gust_mph')
                        values_stanza.append(wind_gust)
                    else:
                        values_stanza.append(converted)
                yield(keys_stanza, values_stanza)
            except ValueError as e:
                LOG.warning(f"Could not parse, got {e}: this row will be skipped!")


            

class WundergroundTableDialect(TableDialectBase):
    def __init__(self, root_element, station_code):
        super().__init__(station_code)
        # Go ahead and only take the table we care about.
        # There's a mobile table first that isn't useful to us.
        self._root_element = root_element.xpath(".//*[contains(@class, 'desktop-table')]")[0]

        self._dbFieldToHeaderStringAndConverterMapping = {
            'datetime_dt': (['Time'], self._parse_datetime),
            'temp_f': (['Temperature'], self._parse_float),
            'dewpoint_f': (['Dew Point'], self._parse_float),
            'rel_humidity_pct': (['Humidity'], self._parse_int),
            'wind_direction_t': (['Wind'], self._parse_str),
            'wind_speed_mph': (['Speed'], self._parse_float),
            'wind_gust_mph': (['Gust'], self._parse_float),
            'pressure_inhg': (['Pressure'], self._parse_float),
            None: (['Precip. Rate.', 'Precip. Accum.', 'UV', 'Solar'], None) # These are fields we are currently ignoring, so we map them to None
        }

        self._fieldNamesArray = [] # The array, in order, of the DB-named fields that match the order of the headers in the table.
        self._convertersArray = [] # The appropriate converters, in order, of the value that matches the order of the headers in the table.
        self._mappingToListsAccordingToHeaders()
        
        self._date = None
        self._extract_date()

    def _parse_datetime(self, text):
        # The pages loaded by wunderground *always* are limited to a single day.
        # This day is a header in the table data - convenient!
        time = datetime.strptime(text, "%I:%M %p")
        # We combine the time with the date from the header to get a full datetime.
        return datetime.combine(self._date, time.time()).isoformat()


    def _mappingToListsAccordingToHeaders(self):
        # This is a bit jank, but we can use the header string to infer which field it maps to in our database. We have a mapping of possible header strings for each field, and we just loop through them until we find a match.
        # The result is an array, in order, of the headers of the table, so we can index into the rows in the correct order.
        elements = self._root_element.xpath(".//th")
        self._fieldNamesArray = []
        self._convertersArray = []
        for element in elements:
            found = False
            header_as_str = element.xpath("string()")
            for db_field, mapping_tuple in self._dbFieldToHeaderStringAndConverterMapping.items():
                header_strings = mapping_tuple[0]
                converter = mapping_tuple[1] # The first element of the tuple is the list of possible header strings, the second is the converter function (if any) to apply to the value in that column.
                if header_as_str in header_strings:
                    found = True
                    self._fieldNamesArray.append(db_field)
                    self._convertersArray.append(converter)
                    break
            if not found:
                import pdb; pdb.set_trace()
                raise Exception(f"Unrecognized header string: {header_as_str}")

    def _extract_date(self):
        # Wunderground conveniently includes the date as a HTML header, and all data is constrained to that one date.
        h3 = self._root_element.xpath("//h3")
        if len(h3) != 1:
            breakpoint()
            raise Exception("We expect exactly one h3 element in the table")
        self._date = datetime.strptime(h3[0].xpath("string()"), "%B %d, %Y")

    def query_keys_and_values(self):
        # Parse each row, map to headers, and return a tuple of (headers, values) for sql for each row.
        rows = self._root_element.xpath(".//tr")[1:] # Skip header row
        for row in rows:
            LOG.trace(row.xpath("string()"))
            # It seems like sometimes/often the first row is blank in wunderground?
            if len(row.xpath("string()")) == 0:
                continue
            keys_stanza = []
            values_stanza = []
            try:
                for i, element in enumerate(row.xpath(".//td")):
                    value_string = element.xpath("string()").strip()
                    LOG.trace(f"Parsing value: {value_string} for header: {self._fieldNamesArray[i]}")
                    if self._fieldNamesArray[i] is None:
                        LOG.debug(f"Skipping {value_string} as db field is None")
                        continue
                    keys_stanza.append(self._fieldNamesArray[i])
                    converter = self._convertersArray[i]
                    if converter != None: # Keep in mind "None" means "drop this data"
                        converted = converter(value_string)
                    else:
                        converted = None
                    values_stanza.append(converted)
                yield(keys_stanza, values_stanza)
            except ValueError as e:
                LOG.warning(f"Error in parsing: {e}; this row is skipped / not put in DB.")


def createTableParser(json_object, file, station_code):
    # For now, the technique I'll use is to look at the attributes on the header row elements. If they match a pattern, we can use that to map to a parser.

    file_dt = datetime.fromisoformat(file.stem.replace("_", ":"))
    if 'body' in json_object[0]:
        LOG.trace("New style data direct passthrough of webhook data...")
        root =  html.fromstring("<root>" + json_object[0]['body']['message'] + "</root>")
        h3 = root.xpath("//h3")
        if len(h3) == 1:
            # Wunderground table has a h3 element, where NWS does not.
            # This supplies the date, so we don't need to pass it in.
            LOG.debug("Detected wunderground style data")
            return WundergroundTableDialect(root, station_code)
    else:
        LOG.trace("Old style data with preparsed rows.")
        if len(json_object[0]['rows']) == 0:
            LOG.warning(f"Found zero-entry file at {file}; skipping")
            ERROR_FILES.append(file)
            return
        return NWSTableDialect(json_object, file_dt, station_code)
    LOG.warning(f"No parser to handle file {file}")
    ERROR_FILES.append(file)
    return None

def process():

    rows_skipped = 0
    rows_inserted = 0

    for file in jsonfiles:
        # Go ahead and infer the year from the filename.
        # Note that on a mac, downloading the file with a colon in the name causes it to be replaced with an underscore, so we should replace this back in the string. Since there's natively no underscore in the date, this should effectively no-op on Linux.
        station_code = file.parent.name # Important: the parent - the dir - should be named as the station code.
        with open(file, 'r') as f:
            obj = json.load(f)
        parser = createTableParser(obj, file, station_code)
        if not parser:
            # No parser can be found; skip this file.
            continue
        for (keys_stanza, values_stanza) in parser.query_keys_and_values():
            keys_stanza.append("station_code")
            values_stanza.append(station_code)
            stmt = f"""
            INSERT INTO weather ({", ".join(keys_stanza)}) VALUES ({("?, " * len(values_stanza))[:-2]})
            ON CONFLICT (station_code, datetime_dt) DO NOTHING
            """
            cursor.execute(stmt, values_stanza)
            db.commit()
            if cursor.rowcount == 0:
                LOG.debug(f"Skipping existing dt in db: {values_stanza[0]}")
            else:
                LOG.debug(f"Inserted {values_stanza[0]}")
                rows_inserted += 1


    LOG.info(f"Done! Inserted {rows_inserted}; skipped {rows_skipped}")

class DbTester:
    def __init__(self):
        pass

    def test_not_empty(self):
        result = cursor.execute("SELECT COUNT(*) FROM weather").fetchall()[0][0]
        LOG.trace(f"{result} count of rows")
        ASSERT(result > 0, f"Expected a non-zero number of rows in the DB")
        LOG.info(f"{inspect.currentframe().f_code.co_name} passes")

    def test_multiple_stations(self):
        result = cursor.execute("SELECT DISTINCT (station_code) FROM weather LIMIT 1000").fetchall()
        LOG.trace(f"{result} stations")
        ASSERT(len(result) > 0, f"Expected multiple stations in the DB")
        LOG.info(f"{inspect.currentframe().f_code.co_name} passes")

    def test_recency(self):
        result = cursor.execute("SELECT station_code, MAX (datetime_dt) FROM weather LIMIT 10").fetchall()
        dt = datetime.fromisoformat(result[0][1])
        LOG.trace(f"Latest entry: {result[0][0]} at {result[0][1]}")
        ASSERT( abs(datetime.now() - dt).days <= 1, f"More than one day has elapsed from greatest entry to now")
        LOG.info(f"{inspect.currentframe().f_code.co_name} passes")

    def test_continuity(self):
        result_stations = cursor.execute("SELECT DISTINCT (station_code) FROM weather LIMIT 1000").fetchall()
        station_map = {}
        for station_tup in result_stations:
            stn = station_tup[0]
            station_map[stn] = []
            result = cursor.execute("SELECT datetime_dt FROM weather WHERE station_code = ? ORDER BY datetime_dt DESC LIMIT 10000", (stn,)).fetchall() 
            for time_tup in result:
                dt = datetime.fromisoformat(time_tup[0])
                station_map[stn].append(dt)
        gap_map = {}
        for station, times in station_map.items():
            gap_map[station] = []
            for i, value in enumerate(times[0:-1]):
                current = value
                previous = times[i+1]
                diff = current - previous
                if (diff.seconds / 60) > 90:
                    gap_map[station].append((current, previous))
        for k, v in gap_map.items():
            if len(v) > 0:
                ASSERT(False, f"{k} has some time gaps! {v}")
        LOG.info(f"{inspect.currentframe().f_code.co_name} passes")
    
    def run_tests(self, testlevel):
        if testlevel == 1:
            self.test_not_empty()
            self.test_multiple_stations()
            self.test_recency()
            self.test_continuity()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-v', '--verbose',
        action='count',
        default=0,
        help='Verbosity level. Use -v, -vv, -vvv, etc.'
        )
    parser.add_argument(
        '-t', '--test',
        action='count',
        default=0,
        help='Sanity test the db. Use -t, -tt, -ttt etc.'
        )
    args = parser.parse_args()

    # args.verbose will be 0, 1, 2, 3, etc.
    LOG.setLevel(logging.INFO)
    if args.verbose == 0:
        LOG.setLevel(logging.INFO)
        LOG.info("Begin...")
    elif args.verbose == 1:
        LOG.setLevel(logging.DEBUG)
        LOG.info("Begin (debug statements on)...")
    elif args.verbose == 2:
        LOG.setLevel(logging.TRACE)
        LOG.info("Begin (trace statements on)...")


    # Non-testing run.
    if args.test == 0:
        process()
        if len(ERROR_FILES) > 0:
            LOG.info(f"Error files to investigate:\n{' '.join([str(p.resolve()) for p in ERROR_FILES])}")
    # Various levels of testing
    elif args.test == 1:
        LOG.info("Running DB sanity tests...")
        DbTester().run_tests(args.test)
