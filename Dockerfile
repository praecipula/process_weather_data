# Dockerfile for GenCast Worker

FROM python:3.10-slim-buster

# Install essential system dependencies
# These include tools needed for cloning Git repos, building Python packages,
# and supporting NetCDF/HDF5 which are common for scientific data.
RUN apt-get update && 
    apt-get install -y --no-install-recommends 
    build-essential 
    git 
    curl 
    libffi-dev 
    libssl-dev 
    libnetcdf-dev 
    hdf5-tools 
    # Clean up APT cache to reduce image size
    && rm -rf /var/lib/apt/lists/*

# Set working directory to /app
WORKDIR /app

# Clone the google-deepmind/graphcast repository
# This repository contains the GenCast source code.
RUN git clone https://github.com/google-deepmind/graphcast.git /app/graphcast

# Set the working directory to the cloned repository
WORKDIR /app/graphcast

# Install Python dependencies
# Ensure pip is up-to-date
RUN pip install --no-cache-dir --upgrade pip

# Install JAX with TPU support.
# This specific command ensures the correct libtpu wheel is found for Cloud TPUs.
# We also install other core libraries like dm-haiku, optax, and data handling tools.
# NOTE: Specific JAX/libtpu versions might need to be pinned for stability.
# Refer to the official JAX documentation for the latest recommended installation for Cloud TPUs.
RUN pip install --no-cache-dir 
    "jax[tpu]>=0.4.23" -f https://storage.googleapis.com/jax-releases/libtpu_releases.html 
    dm-haiku 
    optax 
    xarray 
    netcdf4 
    pandas 
    matplotlib 
    absl-py 
    numpy 
    scipy 
    # Install additional dependencies from GenCast's own setup.py if necessary
    # (or from a requirements.txt if available in the cloned repo)
    # Example: pip install -e . # if setup.py is present and installable

# Set default command (can be overridden by docker-compose run)
# This will be overridden by the gcp-run-forecast.sh script to execute GenCast.
CMD ["/bin/bash"]
