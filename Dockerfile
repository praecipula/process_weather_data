# Dockerfile for GenCast Worker

FROM python:3.10-slim-bookworm

# Install essential system dependencies
# These include tools needed for cloning Git repos, building Python packages,
# and supporting NetCDF/HDF5 which are common for scientific data.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    git \
    curl \
    libffi-dev \
    libssl-dev \
    libnetcdf-dev \
    hdf5-tools \
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
RUN pip install --no-cache-dir \
    "jax[tpu]>=0.4.23" -f https://storage.googleapis.com/jax-releases/libtpu_releases.html \
    dm-haiku \
    optax \
    xarray \
    netcdf4 \
    pandas \
    matplotlib \
    absl-py \
    numpy \
    scipy

# Set default command (can be overridden by docker-compose run)
CMD ["/bin/bash"]
