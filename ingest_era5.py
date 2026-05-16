"""
ingest_era5.py
==============
Fetches the required atmospheric and surface snapshots from ERA5 
via the Copernicus CDS API and uploads them to GCS for GenCast.

Requirements:
- pip install cdsapi google-cloud-storage xarray netcdf4 h5netcdf
- ~/.cdsapirc file with valid credentials
"""

import os
import argparse
import datetime
import cdsapi
import numpy as np
import xarray as xr
from google.cloud import storage

# GenCast Requirements
PRESSURE_LEVELS = [
    '50', '100', '150', '200', '250', '300', 
    '400', '500', '600', '700', '850', '925', '1000'
]

ATMOS_VARS = [
    'temperature', 'specific_humidity', 'geopotential',
    'u_component_of_wind', 'v_component_of_wind'
]

SURFACE_VARS = [
    '2m_temperature', 'surface_pressure', 
    '10m_u_component_of_wind', '10m_v_component_of_wind'
]

def download_era5(client, target_datetime, output_dir):
    """Downloads snapshots for T-6, T, and T+6 hours. Skips if files exist."""
    atmos_file = os.path.join(output_dir, "atmos_raw.nc")
    surface_file = os.path.join(output_dir, "surface_raw.nc")
    
    if os.path.exists(atmos_file) and os.path.exists(surface_file):
        print(f"[CACHE] Found existing raw files in {output_dir}. Skipping download.")
        return atmos_file, surface_file

    t0 = target_datetime
    t_minus_6 = t0 - datetime.timedelta(hours=6)
    t_plus_6 = t0 + datetime.timedelta(hours=6)
    
    times = [t_minus_6, t0, t_plus_6]
    
    # CDS API requires strings
    unique_years = sorted(list(set([str(t.year) for t in times])))
    unique_months = sorted(list(set([str(t.month) for t in times])))
    unique_days = sorted(list(set([str(t.day) for t in times])))
    time_strs = sorted(list(set([t.strftime('%H:%M') for t in times])))
    
    print(f"Requesting snapshots for: Years {unique_years}, Months {unique_months}, Days {unique_days} at {time_strs}")
    
    # 1. Download Pressure Level Data
    client.retrieve(
        'reanalysis-era5-pressure-levels',
        {
            'product_type': 'reanalysis',
            'format': 'netcdf',
            'variable': ATMOS_VARS,
            'pressure_level': PRESSURE_LEVELS,
            'year': unique_years,
            'month': unique_months,
            'day': unique_days,
            'time': time_strs,
        },
        atmos_file
    )
    
    # 2. Download Surface Data
    client.retrieve(
        'reanalysis-era5-single-levels',
        {
            'product_type': 'reanalysis',
            'format': 'netcdf',
            'variable': SURFACE_VARS,
            'year': unique_years,
            'month': unique_months,
            'day': unique_days,
            'time': time_strs,
        },
        surface_file
    )
    
    return atmos_file, surface_file

def package_data(atmos_path, surface_path, output_path, target_datetime):
    """Merges and renames dimensions/variables to match DeepMind requirements."""
    print("Packaging and normalizing datasets...")
    ds_atmos = xr.open_dataset(atmos_path)
    ds_surface = xr.open_dataset(surface_path)
    
    # Merge them into one dataset
    ds_merged = xr.merge([ds_atmos, ds_surface])
    
    # --- CRITICAL FIX 1: Rename dimensions and variables ---
    rename_map = {
        'latitude': 'lat',
        'longitude': 'lon',
        'pressure_level': 'level',
        'z': 'geopotential',
        't': 'temperature',
        'q': 'specific_humidity',
        'u': 'u_component_of_wind',
        'v': 'v_component_of_wind',
        't2m': '2m_temperature',
        'sp': 'surface_pressure',
        'u10': '10m_u_component_of_wind',
        'v10': '10m_v_component_of_wind'
    }
    actual_rename = {k: v for k, v in rename_map.items() if k in ds_merged.coords or k in ds_merged.data_vars}
    ds_merged = ds_merged.rename(actual_rename)

    # --- CRITICAL FIX 2: Correct Time/Datetime logic ---
    raw_time_dim = 'valid_time' if 'valid_time' in ds_merged.dims else 'time'
    
    # 1. Extract raw timestamps and calculate relative offsets
    abs_datetimes = ds_merged[raw_time_dim].values
    offsets = abs_datetimes - np.datetime64(target_datetime)
    
    # 2. Rename the main dimension to 'time' (if it wasn't already)
    if raw_time_dim != 'time':
        ds_merged = ds_merged.rename({raw_time_dim: 'time'})
    
    # 3. Overwrite the 'time' dimension with the relative timedeltas
    ds_merged['time'] = offsets
    
    # --- CRITICAL FIX 3: Add the 'batch' dimension ---
    ds_merged = ds_merged.expand_dims('batch')
    ds_merged = ds_merged.assign_coords(batch=[0])

    # --- CRITICAL FIX 4: Create 2D 'datetime' coordinate (batch, time) ---
    # DeepMind requires datetime to be 2D for progress featurization
    ds_merged = ds_merged.assign_coords(datetime=(('batch', 'time'), abs_datetimes[None, :]))

    # --- CRITICAL FIX 5: Strip conflicting metadata ---
    # This prevents the 'failed to prevent overwriting existing key dtype' error
    print(f"Sanitizing NetCDF metadata for variables: {list(ds_merged.variables)}")
    for var in ds_merged.variables:
        # Clear ALL encoding and attributes for this variable
        ds_merged[var].encoding = {}
        ds_merged[var].attrs = {}

    # Double-check 'time' specifically
    ds_merged['time'].encoding = {}
    ds_merged['time'].attrs = {}

    # Drop ERA5T 'expver' if it exists
    if 'expver' in ds_merged.coords:
        print("Dropping 'expver' coordinate...")
        ds_merged = ds_merged.drop_vars('expver')
        
    # Ensure local file is overwritten
    if os.path.exists(output_path):
        os.remove(output_path)
        
    # Write using the most basic NetCDF4 settings
    ds_merged.to_netcdf(output_path)
    print(f"Packaged file created and sanitized: {output_path}")

def upload_to_gcs(local_path, bucket_name, gcs_path):
    """Uploads the file to Google Cloud Storage."""
    print(f"Uploading to gs://{bucket_name}/{gcs_path}...")
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(local_path)
    print("Upload complete.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest ERA5 data for GenCast.")
    parser.add_argument("--date", required=True, help="Target date (YYYY-MM-DD)")
    parser.add_argument("--time", default="12:00", help="Target time (HH:MM)")
    parser.add_argument("--bucket", default="overengineeredweather-run-data", help="GCS bucket name")
    
    args = parser.parse_args()
    
    target_dt = datetime.datetime.strptime(f"{args.date} {args.time}", "%Y-%m-%d %H:%M")
    
    # Setup working directory
    tmp_dir = "tmp_ingest"
    os.makedirs(tmp_dir, exist_ok=True)
    
    # Manually load credentials from local .cdsapirc if present
    cds_url, cds_key = None, None
    if os.path.exists(".cdsapirc"):
        with open(".cdsapirc", "r") as f:
            for line in f:
                if line.startswith("url:"): cds_url = line.split(":", 1)[1].strip()
                if line.startswith("key:"): cds_key = line.split(":", 1)[1].strip()
    
    c = cdsapi.Client(url=cds_url, key=cds_key)
    
    try:
        atmos_raw, surface_raw = download_era5(c, target_dt, tmp_dir)
        
        # Follow DeepMind Filename Convention
        filename = f"source-era5_date-{args.date}_res-0.25_levels-13.nc"
        final_nc = os.path.join(tmp_dir, filename)
        
        package_data(atmos_raw, surface_raw, final_nc, target_dt)
        
        # Upload using the descriptive name
        gcs_dest = f"era5_input/{filename}"
        upload_to_gcs(final_nc, args.bucket, gcs_dest)
        
        # Also upload to the 'input_batch.nc' alias for backward compatibility
        upload_to_gcs(final_nc, args.bucket, "era5_input/input_batch.nc")
        
    except Exception as e:
        print(f"Error during ingestion: {e}")
    finally:
        print("Cleaning up temporary files...")
