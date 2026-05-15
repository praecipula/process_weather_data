"""
run_gencast_inference.py
========================
Custom wrapper script to run DeepMind GenCast inference.
Based on the logic in DeepMind's official Colab demos.
"""

import os
import argparse
import datetime
import jax
import numpy as np
import xarray as xr
from graphcast import checkpoint
from graphcast import data_utils
from graphcast import gencast
from graphcast import autoregressive
from graphcast import normalization

def run_inference(model_path, stats_path, input_data_path, output_data_path):
    print(f"Loading model weights from: {model_path}")
    with open(model_path, "rb") as f:
        ckpt = checkpoint.load(f, gencast.CheckPoint)
        params = ckpt.params
        task_config = ckpt.task_config
        # GenCast-specific architecture components
        denoiser_arch = ckpt.denoiser_architecture_config
        sampler_conf = ckpt.sampler_config
        noise_conf = ckpt.noise_config
        noise_enc_conf = ckpt.noise_encoder_config

    print(f"Loading normalization stats from: {stats_path}")
    diffs_stddev_by_level = xr.open_dataset(os.path.join(stats_path, "diffs_stddev_by_level.nc"))
    mean_by_level = xr.open_dataset(os.path.join(stats_path, "mean_by_level.nc"))
    stddev_by_level = xr.open_dataset(os.path.join(stats_path, "stddev_by_level.nc"))

    # Build the GenCast model with all diffusion components
    def construct_fn(inputs, targets_template):
        model = gencast.GenCast(
            task_config=task_config,
            denoiser_architecture_config=denoiser_arch,
            sampler_config=sampler_conf,
            noise_config=noise_conf,
            noise_encoder_config=noise_enc_conf
        )
        return model(inputs, targets_template)

    # Wrap for autoregressive prediction
    predictor = autoregressive.Predictor(
        construct_fn,
        task_config,
        normalization.InputsAndTargets(
            mean_by_level, stddev_by_level, diffs_stddev_by_level
        )
    )

    print(f"Loading ERA5 input data from: {input_data_path}")
    input_file = os.path.join(input_data_path, "input_batch.nc")
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"Missing input batch file: {input_file}")
    
    inputs = xr.open_dataset(input_file)

    print("Executing GenCast Ensemble Forecast (50 members)...")
    rng = jax.random.PRNGKey(42)
    
    # Run the forecast (GenCast uses empty state)
    predictions = predictor.predict(params, {}, rng, inputs)

    print(f"Saving forecast results to: {output_data_path}")
    os.makedirs(output_data_path, exist_ok=True)
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(output_data_path, f"gencast_forecast_{timestamp}.nc")
    predictions.to_netcdf(output_file)
    print(f"Forecast complete: {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--model_path", required=True)
    parser.add_argument("--stats_path", required=True)
    parser.add_argument("--input_data_path", required=True)
    parser.add_argument("--output_data_path", required=True)
    args = parser.parse_args()

    run_inference(
        args.model_path,
        args.stats_path,
        args.input_data_path,
        args.output_data_path
    )
