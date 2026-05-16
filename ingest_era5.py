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
    'u_component_of_wind', 'v_component_of_wind', 'vertical_velocity'
]

SURFACE_INST_VARS = [
    '2m_temperature', 'surface_pressure', '10m_u_component_of_wind', 
    '10m_v_component_of_wind', 'sea_surface_temperature'
]

# These often need their own requests in the new CDS-Beta
SURFACE_MSL_VAR = ['mean_sea_level_pressure']
SURFACE_ACC_VAR = ['total_precipitation']

# Invariant/Static fields (Orography)
STATIC_VARS = ['geopotential', 'land_sea_mask']

def download_era5(client, target_datetime, output_dir):
    """Downloads snapshots in separate streams for maximum reliability."""
    files = {
        "atmos": os.path.join(output_dir, "atmos_raw.nc"),
        "surf_inst": os.path.join(output_dir, "surf_inst_raw.nc"),
        "surf_msl": os.path.join(output_dir, "surf_msl_raw.nc"),
        "surf_acc": os.path.join(output_dir, "surf_acc_raw.nc"),
        "static": os.path.join(output_dir, "static_raw.nc")
    }
    
    if all(os.path.exists(f) for f in files.values()):
        print(f"[CACHE] Found all {len(files)} raw files. Skipping download.")
        return files

    t0 = target_datetime
    t_minus_6 = t0 - datetime.timedelta(hours=6)
    t_plus_6 = t0 + datetime.timedelta(hours=6)
    times = [t_minus_6, t0, t_plus_6]
    
    unique_years = sorted(list(set([str(t.year) for t in times])))
    unique_months = sorted(list(set([str(t.month) for t in times])))
    unique_days = sorted(list(set([str(t.day) for t in times])))
    time_strs = sorted(list(set([t.strftime('%H:%M') for t in times])))
    
    # 1. Atmospheric Pressure Levels
    print("Requesting Atmospheric Levels...")
    client.retrieve('reanalysis-era5-pressure-levels', {
        'product_type': 'reanalysis', 'format': 'netcdf',
        'variable': ATMOS_VARS, 'pressure_level': PRESSURE_LEVELS,
        'year': unique_years, 'month': unique_months, 'day': unique_days, 'time': time_strs,
    }, files["atmos"])
    
    # 2. Surface Instantaneous
    print("Requesting Surface Instantaneous...")
    client.retrieve('reanalysis-era5-single-levels', {
        'product_type': 'reanalysis', 'format': 'netcdf',
        'variable': SURFACE_INST_VARS,
        'year': unique_years, 'month': unique_months, 'day': unique_days, 'time': time_strs,
    }, files["surf_inst"])

    # 3. Mean Sea Level Pressure
    print("Requesting Mean Sea Level Pressure...")
    client.retrieve('reanalysis-era5-single-levels', {
        'product_type': 'reanalysis', 'format': 'netcdf',
        'variable': SURFACE_MSL_VAR,
        'year': unique_years, 'month': unique_months, 'day': unique_days, 'time': time_strs,
    }, files["surf_msl"])

    # 4. Total Precipitation
    print("Requesting Total Precipitation...")
    client.retrieve('reanalysis-era5-single-levels', {
        'product_type': 'reanalysis', 'format': 'netcdf',
        'variable': SURFACE_ACC_VAR,
        'year': unique_years, 'month': unique_months, 'day': unique_days, 'time': time_strs,
    }, files["surf_acc"])

    # 5. Static Invariants
    print("Requesting Static Invariants (Orography)...")
    client.retrieve('reanalysis-era5-single-levels', {
        'product_type': 'reanalysis', 'format': 'netcdf',
        'variable': STATIC_VARS,
        'year': unique_years[0], 'month': unique_months[0], 'day': unique_days[0], 'time': time_strs[0],
    }, files["static"])
    
    return files

def package_data(files, output_path, target_datetime):
    """Merges all raw streams and renames to match DeepMind requirements."""
    print("Packaging and normalizing datasets...")
    
    # Load and clean each stream
    ds_atmos = xr.open_dataset(files["atmos"])
    ds_surf_inst = xr.open_dataset(files["surf_inst"])
    ds_surf_msl = xr.open_dataset(files["surf_msl"])
    ds_surf_acc = xr.open_dataset(files["surf_acc"])
    ds_static = xr.open_dataset(files["static"])

    # --- CRITICAL FIX: Handle 'geopotential' naming collision ---
    # Static geopotential must be renamed to 'geopotential_at_surface' BEFORE merge
    # so it doesn't collide with the 13 levels of atmospheric 'geopotential'.
    static_rename = {}
    if 'z' in ds_static.data_vars: static_rename['z'] = 'geopotential_at_surface'
    if 'lsm' in ds_static.data_vars: static_rename['lsm'] = 'land_sea_mask'
    ds_static = ds_static.rename(static_rename)
    
    # Broadcast static invariants across the time dimension
    # (DeepMind expects these to be present at every time step)
    ds_static = ds_static.broadcast_like(ds_atmos[['time']] if 'time' in ds_atmos.dims else ds_atmos[['valid_time']])

    datasets = [ds_atmos, ds_surf_inst, ds_surf_msl, ds_surf_acc, ds_static]
    cleaned_datasets = []
    for ds in datasets:
        if 'expver' in ds.coords: ds = ds.drop_vars('expver')
        cleaned_datasets.append(ds)

    # Merge all streams
    ds_merged = xr.merge(cleaned_datasets, compat='override')
    
    # --- CRITICAL FIX 1: Rename dimensions and variables ---
    rename_map = {
        'latitude': 'lat', 'longitude': 'lon', 'pressure_level': 'level',
        'z': 'geopotential', 't': 'temperature', 'q': 'specific_humidity',
        'u': 'u_component_of_wind', 'v': 'v_component_of_wind', 'w': 'vertical_velocity',
        't2m': '2m_temperature', 'sp': 'surface_pressure',
        'msl': 'mean_sea_level_pressure', 'tp': 'total_precipitation_12hr',
        'u10': '10m_u_component_of_wind', 'v10': '10m_v_component_of_wind',
        'sst': 'sea_surface_temperature'
    }
    actual_rename = {k: v for k, v in rename_map.items() if k in ds_merged.coords or k in ds_merged.data_vars}
    ds_merged = ds_merged.rename(actual_rename)

    # --- CRITICAL FIX 2: Correct Time/Datetime logic ---
    raw_time_dim = 'valid_time' if 'valid_time' in ds_merged.dims else 'time'
    abs_datetimes = ds_merged[raw_time_dim].values
    offsets = abs_datetimes - np.datetime64(target_datetime)
    
    if raw_time_dim != 'time':
        ds_merged = ds_merged.rename({raw_time_dim: 'time'})
    
    print("  -> Casting time coordinates to raw int64 nanoseconds...")
    ds_merged['time'] = offsets.astype('int64')
    
    ds_merged = ds_merged.expand_dims('batch')
    ds_merged = ds_merged.assign_coords(batch=[0])
    ds_merged = ds_merged.assign_coords(datetime=(('batch', 'time'), abs_datetimes.astype('datetime64[ns]').astype('int64')[None, :]))

    # --- CRITICAL FIX 4: Strip ALL metadata ---
    print(f"Sanitizing NetCDF metadata for variables: {list(ds_merged.variables)}")
    for var in ds_merged.variables:
        ds_merged[var].encoding = {}
        ds_merged[var].attrs = {}

    if os.path.exists(output_path):
        os.remove(output_path)
        
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
    
    tmp_dir = "tmp_ingest"
    os.makedirs(tmp_dir, exist_ok=True)
    
    cds_url, cds_key = None, None
    if os.path.exists(".cdsapirc"):
        with open(".cdsapirc", "r") as f:
            for line in f:
                if line.startswith("url:"): cds_url = line.split(":", 1)[1].strip()
                if line.startswith("key:"): cds_key = line.split(":", 1)[1].strip()
    
    c = cdsapi.Client(url=cds_url, key=cds_key)
    
    try:
        files = download_era5(c, target_dt, tmp_dir)
        filename = f"source-era5_date-{args.date}_res-0.25_levels-13.nc"
        final_nc = os.path.join(tmp_dir, filename)
        package_data(files, final_nc, target_dt)
        upload_to_gcs(final_nc, args.bucket, f"era5_input/{filename}")
        upload_to_gcs(final_nc, args.bucket, "era5_input/input_batch.nc")
        
    except Exception as e:
        print(f"Error during ingestion: {e}")
    finally:
        print("Cleaning up temporary files...")
