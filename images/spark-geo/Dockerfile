# spark-geo: Spark image with geospatial libraries (geopandas, GDAL)
# Used for shapefile processing and SVG generation
#
# Extends spark-executor with:
# - GDAL system libraries
# - geopandas, shapely, fiona, pyproj Python packages

ARG REGISTRY=registry.container-registry.svc.cluster.local:5000

FROM ${REGISTRY}/electinfo/spark-executor:latest

USER root

# Install GDAL system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gdal-bin \
    libgdal-dev \
    && rm -rf /var/lib/apt/lists/*

# Set GDAL environment for fiona compilation
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

# Install geospatial Python packages
RUN pip install --no-cache-dir \
    geopandas \
    shapely \
    pyproj \
    fiona

USER spark
