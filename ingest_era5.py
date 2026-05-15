"""
ingest_era5.py
==============
Fetches the required atmospheric and surface snapshots from ERA5 
via the Copernicus CDS API and uploads them to GCS for GenCast.

Requirements:
- pip install cdsapi google-cloud-storage xarray netcdf4
- ~/.cdsapirc file with valid credentials
"""

import os
import argparse
import datetime
import cdsapi
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
    """Downloads snapshots for T-6, T, and T+6 hours."""
    t0 = target_datetime
    t_minus_6 = t0 - datetime.timedelta(hours=6)
    t_plus_6 = t0 + datetime.timedelta(hours=6)
    
    times = [t_minus_6, t0, t_plus_6]
    
    # CDS API requires strings
    # We take the unique dates and times
    unique_dates = sorted(list(set([t.strftime('%Y-%m-%d') for t in times])))
    time_strs = sorted(list(set([t.strftime('%H:%M') for t in times])))
    
    print(f"Requesting snapshots for: {unique_dates} at {time_strs}")
    
    # 1. Download Pressure Level Data
    atmos_file = os.path.join(output_dir, "atmos_raw.nc")
    client.retrieve(
        'reanalysis-era5-pressure-levels',
        {
            'product_type': 'reanalysis',
            'format': 'netcdf',
            'variable': ATMOS_VARS,
            'pressure_level': PRESSURE_LEVELS,
            'year': [t.year for t in times],
            'month': [t.month for t in times],
            'day': [t.day for t in times],
            'time': time_strs,
        },
        atmos_file
    )
    
    # 2. Download Surface Data
    surface_file = os.path.join(output_dir, "surface_raw.nc")
    client.retrieve(
        'reanalysis-era5-single-levels',
        {
            'product_type': 'reanalysis',
            'format': 'netcdf',
            'variable': SURFACE_VARS,
            'year': [t.year for t in times],
            'month': [t.month for t in times],
            'day': [t.day for t in times],
            'time': time_strs,
        },
        surface_file
    )
    
    return atmos_file, surface_file

def package_data(atmos_path, surface_path, output_path):
    """Merges and renames dimensions to match DeepMind requirements."""
    print("Packaging and normalizing datasets...")
    ds_atmos = xr.open_dataset(atmos_path)
    ds_surface = xr.open_dataset(surface_path)
    
    # Merge them into one dataset
    ds_merged = xr.merge([ds_atmos, ds_surface])
    
    # --- CRITICAL FIX: Rename dimensions to match old CDS/DeepMind standards ---
    rename_map = {}
    if 'valid_time' in ds_merged.dims: rename_map['valid_time'] = 'time'
    if 'pressure_level' in ds_merged.dims: rename_map['pressure_level'] = 'level'
    
    if rename_map:
        print(f"Renaming dimensions: {rename_map}")
        ds_merged = ds_merged.rename(rename_map)

    # Drop ERA5T 'expver' if it exists (causes merge conflicts later)
    if 'expver' in ds_merged.coords:
        print("Dropping 'expver' coordinate...")
        ds_merged = ds_merged.drop_vars('expver')
        
    ds_merged.to_netcdf(output_path)
    print(f"Packaged file created: {output_path}")

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
        
        # Follow DeepMind Filename Convention: source-era5_date-YYYY-MM-DD_res-0.25_levels-13.nc
        filename = f"source-era5_date-{args.date}_res-0.25_levels-13.nc"
        final_nc = os.path.join(tmp_dir, filename)
        
        package_data(atmos_raw, surface_raw, final_nc)
        
        # Upload using the descriptive name
        gcs_dest = f"era5_input/{filename}"
        upload_to_gcs(final_nc, args.bucket, gcs_dest)
        
        # Also upload to the 'input_batch.nc' alias for backward compatibility
        upload_to_gcs(final_nc, args.bucket, "era5_input/input_batch.nc")
        
    except Exception as e:
        print(f"Error during ingestion: {e}")
    finally:
        print("Cleaning up temporary files...")
