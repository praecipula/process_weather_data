#!/usr/bin/env python

from pathlib import Path
import json
from lxml import etree, html
import sqlite3
from datetime import datetime
import re
import pdb
import argparse


here = Path(".")
jsonfiles = here.glob("./input_scrapes/KSFO/*.json")

# One unified weather database. Each data is in the same table but split by station_code
# This means we have to have a unique index, hmm. Better to shard to separate dbs by station?

db = sqlite3.connect("./weather.db")

cursor = db.cursor()

cursor.execute("""CREATE TABLE IF NOT EXISTS weather (
    id INTEGER PRIMARY KEY,
    station_code TEXT,
    datetime_dt TEXT,
    temp_f INTEGER,
    dewpoint_f INTEGER,
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
    sixhr_max_f INTEGER,
    sixhr_min_f INTEGER,
    twentyfourhr_max_f INTEGER, 
    twentyfourhr_min_f INTEGER
  )""")

cursor.execute("""
    CREATE UNIQUE INDEX IF NOT EXISTS idx_stn_time ON weather (station_code, datetime_dt)
  """)


dbFieldToHeaderStringMapping = {
    'datetime_dt': ['Date/Time\xa0(L)'],
    'temp_f': ['Temp.\xa0(°F)'],
    'dewpoint_f': ['DewPoint(°F)'],
    'rel_humidity_pct': ['RelativeHumidity(%)'],
    'heat_index_f': ['HeatIndex(°F)'],
    'wind_chill_f': ['WindChill(°F)'],
    'wind_direction_t': ['WindDirection\xa0'],
    'wind_speed_mph': ['WindSpeed(mph)'],
    'wind_gust_mph': [], # This is inferred from the value of the wind speed column, so we don't have a header parsed for it.
    'visibility_m': ['Visibility\xa0(miles)'],
    'weather_t': ['Weather\xa0\xa0'],   
    'clouds_t': ['Clouds\xa0(x100 ft)'],
    'pressure_sea_mb': ['Sea LevelPressure(mb)'],
    'pressure_inhg': ['StationPressure(in Hg)'],
    'altimiter_setting_inhg': ['AltimeterSetting(in Hg)'],
    'sixhr_max_f': ['6 HrMax(°F)'],
    'sixhr_min_f': ['6 HrMin(°F)'],
    'twentyfourhr_max_f': ['24 HrMax(°F)'],
    'twentyfourhr_min_f': ['24 HrMin(°F)']

}

def parse_to_isodatetime(file_dt, date):
    # Jank, but works to set the year.
    dt_no_year = datetime.strptime(date, "%b %d, %I:%M %p")
    inferred_year = file_dt.year
    # If the file was scraped in the first half of the year, but the month of the entry is in the second half,
    # then we have a wrap situation and the recorded year is last year.
    # For instance, scraped in Feb, but parsed date in Nov, we back-scraped past the year boundary.
    # This assumes that we can't set just any window for the data, which is consitent with the website
    # (which asks basically "how many hours ago from now" for query)
    if file_dt.month <= 6:
        if dt_no_year.month > 6:
            inferred_year = file_dt.year - 1
    # This seems awkward, but the date above doesn't handle leap day correctly, so we reparse from scratch with the inferred year rather than just accepting the date as parsed.
    # https://github.com/python/cpython/issues/70647.
    return datetime.strptime(str(inferred_year) + " " + date, "%Y %b %d, %I:%M %p").isoformat()


def str_or_none(element):
    # Needed because we need all descending text, including colorized / sub-element text.
    inner_txt = element.xpath('string()').strip()
    return inner_txt if len(inner_txt) > 0 else None

def int_or_none(element):
    string = str_or_none(element)
    if string == None:
        return None
    return int(string)

def float_or_none(element):
    string = str_or_none(element)
    if string == None:
        return None
    # If less than or greater than, just strip off the less than or greater than as closest approx.
    match = re.match("[<>]\s+([\d\.]+)", string)
    if match:
        return float(match.groups()[0])
    return float(string)

def windspeed_parse(element):
    # Windspeed optionally has the format \d\dG\d\d, where
    # G in the middle of the string means "gusts up to".
    # So we record those as separate cols.
    string = str_or_none(element)
    if string == None:
        return (None, None)
    match = re.match(r"(.*)[gG](.*)", string)
    if not match:
        return (int(string), None)
    else:
        g = match.groups()
        return (int(g[0]), int(g[1]))


def create_entry_mapping(json_obj):
    # Process header row to know what fields to map
    header_row = json_obj[0]['rows'][0]
    #cleaned = header_row.replace("&nbsp;", "&#160;")
    root = html.fromstring("<root>" + header_row + "</root>")
    elements = root.xpath("//th")
    mapping = []
    for i, element in enumerate(elements):
        found = False
        for db_field, header_strings in dbFieldToHeaderStringMapping.items():
            if element.xpath("string()") in header_strings:
                found = True
                mapping.append((db_field))
                break
        if not found:
            import pdb; pdb.set_trace()
    return mapping

def process(verbosity):

    rows_skipped = 0
    rows_inserted = 0

    for file in jsonfiles:
        # Go ahead and infer the year from the filename.
        # Note that on a mac, downloading the file with a colon in the name causes it to be replaced with an underscore, so we should replace this back in the string. Since there's natively no underscore in the date, this should effectively no-op on Linux.
        file_dt = datetime.fromisoformat(file.stem.replace("_", ":"))
        with open(file, 'r') as f:
            obj = json.load(f)
        field_mapping = create_entry_mapping(obj)
        station_code = file.parent.name
        for row in obj[0]['rows'][1:]:
            # Replace html with their character codes (simplest way to avoid loading a dtd from the network)
            cleaned = row.replace("&nbsp;", "&#160;")
            root = etree.fromstring("<root>" + cleaned + "</root>")
            elements = root.xpath("//td")
            if len(elements) == 22:
                # When doing a more advanced / historical view than the default view, we get 4 extra columns:
                # 1,3,6,24h precipitation, before the temperature summary.
                # For now we strip these out to match the standard size
                trimmed_elements = elements[:14] + elements[18:]
                elements = trimmed_elements
            if len(elements) == 17:
                # Annoyingly, I noticed "wind chill" dropped as a header at one point (3/16/26).
                # This means that we might be suited for something more roubust like parsing headers.
                new_e = etree.Element("td")
                new_e.text = "\xa0"
                padded_elements = elements[:4] + [new_e] + elements[4:]
                elements = padded_elements
            if len(elements) != 18:
                if verbosity > 0:
                    pdb.set_trace()
                raise Exception("We expect a consistent number of elements, even if blank")
            try:
                print(field_mapping)
                idx = lambda field: field_mapping.index(field)
                normalized_values = {
                    'station_code': station_code,
                    'datetime_dt': parse_to_isodatetime(file_dt, elements[idx('datetime_dt')].text),
                    'temp_f': int_or_none(elements[idx('temp_f')]),
                    'dewpoint_f': int_or_none(elements[idx('dewpoint_f')]),
                    'rel_humidity_pct': int_or_none(elements[idx('rel_humidity_pct')]),
                    'heat_index_f': int_or_none(elements[idx('heat_index_f')]),
                    'wind_chill_f': int_or_none(elements[idx('wind_chill_f')]),
                    'wind_direction_t': str_or_none(elements[idx('wind_direction_t')]),
                    'wind_speed_mph': windspeed_parse(elements[idx('wind_speed_mph')])[0],
                    'wind_gust_mph': windspeed_parse(elements[idx('wind_speed_mph')])[1], # Note this special case!
                    'visibility_m': float_or_none(elements[idx('visibility_m')]),
                    'weather_t': str_or_none(elements[idx('weather_t')]),
                    'clouds_t': str_or_none(elements[idx('clouds_t')]),
                    'pressure_sea_mb': float_or_none(elements[idx('pressure_sea_mb')]),
                    'pressure_inhg': float_or_none(elements[idx('pressure_inhg')]),
                    'altimiter_setting_inhg': float_or_none(elements[idx('altimiter_setting_inhg')]),
                    'sixhr_max_f': int_or_none(elements[idx('sixhr_max_f')]),
                    'sixhr_min_f': int_or_none(elements[idx('sixhr_min_f')]),
                    'twentyfourhr_max_f': int_or_none(elements[idx('twentyfourhr_max_f')]),
                    'twentyfourhr_min_f': int_or_none(elements[idx('twentyfourhr_min_f')])
                }
            except Exception as e:
                if verbosity > 0:
                    pdb.set_trace()
                raise e
            
            print(f"keys_stanza: {keys_stanza}")
            print(f"values_stanza: {values_stanza}")
            cursor.execute(f"""
            INSERT INTO weather {keys_stanza} VALUES {values_stanza}
            ON CONFLICT (station_code, datetime_dt) DO NOTHING
            """, tuple(normalized_values.values()))
            db.commit()
            if cursor.rowcount == 0:
                if verbosity > 1:
                    print(f"Skipping existing dt in db: {datetime_dt}")
                rows_skipped += 1
            else:
                if verbosity > 1:
                    print(f"Inserted {datetime_dt}")
                rows_inserted += 1

    print(f"Done! Inserted {rows_inserted}; skipped {rows_skipped}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
            '-v', '--verbose',
            action='count',
            default=0,
            help='Verbosity level. Use -v, -vv, -vvv, etc.'
            )
    args = parser.parse_args()

    # args.verbose will be 0, 1, 2, 3, etc.
    if args.verbose == 0:
        print("Normal output")
    elif args.verbose == 1:
        print("Verbose output (-v)")
    elif args.verbose == 2:
        print("More verbose output (-vv)")
    else:
        print(f"Very verbose output (-v x{args.verbose})")

    process(args.verbose)
