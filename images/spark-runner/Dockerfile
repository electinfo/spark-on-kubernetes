# spark-runner: Spark client for executing Spark jobs via Spark Connect
# Used as sidecar in rundeck for running spark-submit commands
#
# Includes geopandas for shapefile processing (runs on client side only)
#
# JAR files are mounted from the rundeck container via shared volume

ARG REGISTRY=registry.container-registry.svc.cluster.local:5000
FROM ${REGISTRY}/electinfo/spark-connect-server:latest

USER root

# Install GDAL system dependencies for geopandas
RUN apt-get update && apt-get install -y --no-install-recommends \
    gdal-bin \
    libgdal-dev \
    && rm -rf /var/lib/apt/lists/*

# Set GDAL environment for fiona compilation
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

# Install geospatial Python packages and AWS CLI (client-side only)
# AWS CLI installed via pip to avoid version conflicts with botocore
RUN pip install --no-cache-dir \
    geopandas \
    shapely \
    pyproj \
    fiona \
    awscli

USER spark
