# spark-runner: Spark client for executing Spark jobs via Spark Connect
# Used as sidecar in rundeck for running spark-submit commands
#
# Includes geopandas for shapefile processing (runs on client side only)
#
# JAR files are mounted from the rundeck container via shared volume

ARG REGISTRY=registry.container-registry.svc.cluster.local:5000
FROM ${REGISTRY}/electinfo/spark-connect-server:latest

USER root

# Install GDAL system dependencies for geopandas and unzip for AWS CLI v2
RUN apt-get update && apt-get install -y --no-install-recommends \
    gdal-bin \
    libgdal-dev \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Set GDAL environment for fiona compilation
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

# Install geospatial Python packages (client-side only)
RUN pip install --no-cache-dir \
    geopandas \
    shapely \
    pyproj \
    fiona

# Install AWS CLI v2 (supports --no-checksum-algorithm for s3proxy compatibility)
RUN curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o /tmp/awscliv2.zip \
    && unzip -q /tmp/awscliv2.zip -d /tmp \
    && /tmp/aws/install \
    && rm -rf /tmp/awscliv2.zip /tmp/aws

# Install Unity Catalog CLI (copy from official UC image)
COPY --from=unitycatalog/unitycatalog:v0.4.0 /home/unitycatalog /home/unitycatalog
ENV PATH="/home/unitycatalog/bin:${PATH}"

USER spark
