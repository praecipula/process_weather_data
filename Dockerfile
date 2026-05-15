# Dockerfile for GenCast Worker

FROM python:3.10-slim-bookworm

# Install essential system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    git \
    curl \
    libffi-dev \
    libssl-dev \
    libnetcdf-dev \
    hdf5-tools \
    && rm -rf /var/lib/apt/lists/*

# Install the GraphCast library into /opt to avoid volume mount overwrite
WORKDIR /opt
RUN git clone https://github.com/google-deepmind/graphcast.git
WORKDIR /opt/graphcast

# Install dependencies and the library itself
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir \
    "jax[tpu]>=0.4.23" -f https://storage.googleapis.com/jax-releases/libtpu_releases.html \
    dm-haiku \
    optax \
    xarray \
    netcdf4 \
    h5netcdf \
    h5py \
    pandas \
    matplotlib \
    absl-py \
    numpy \
    scipy \
    cartopy \
    dask \
    papermill \
    ipykernel \
    ipywidgets \
    tqdm

# Register the kernel for Jupyter/Papermill
RUN python -m ipykernel install --name python3 --display-name "Python 3"

# Install the graphcast package in editable mode
RUN pip install -e .

# Set working directory to /app (where user code will be mounted)
WORKDIR /app

# Set default command
CMD ["/bin/bash"]
