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
RUN chmod -R a+rX /home/unitycatalog
ENV PATH="/home/unitycatalog/bin:${PATH}"

# Install Playwright + Chromium for ORESTAR browser automation (bypasses F5 bot protection)
# Browsers installed to /opt/playwright so spark user can access them at runtime
ENV PLAYWRIGHT_BROWSERS_PATH=/opt/playwright
RUN pip install --no-cache-dir playwright xlrd \
    && playwright install --with-deps chromium \
    && chmod -R a+rX /opt/playwright

# Stable pipeline Python dependencies (cached normally)
RUN pip install --no-cache-dir pyyaml pandas zstandard nameparser \
    "pydantic>=2.0.0" "hydra-core>=1.3.0" "omegaconf>=2.3.0"

# Splink probabilistic entity resolution + DuckDB backend for dev/testing
RUN pip install --no-cache-dir "splink[spark]" duckdb

# electinfo-common from Nexus PyPI (cache-busted on each triggered build).
ARG PIP_CACHE_EPOCH=0
RUN pip install --no-cache-dir \
    --index-url http://nexus-nexus-repository-manager.nexus.svc.cluster.local:8081/repository/pypi-group/simple/ \
    --trusted-host nexus-nexus-repository-manager.nexus.svc.cluster.local \
    electinfo-common

# pure-translation ships parsers/conf/forms/**/*.yaml — the Hydra-derived FEC
# schemas consumed by pipelines/utilities/fec_hydra_schema.py for the
# fec_filings medallion. We only need the wheel's package data (the YAMLs),
# not its runtime imports, so --no-deps sidesteps the siege-utilities>=3.3.0
# resolution wall in pypi-group that pip can't traverse without netrc auth
# against pypi-hosted's /simple/ endpoint (same snag the Zeppelin notebook
# fec validation hit). pydantic / hydra-core / omegaconf are already
# explicitly installed in the layer above.
RUN pip install --no-cache-dir --no-deps \
    --index-url http://nexus-nexus-repository-manager.nexus.svc.cluster.local:8081/repository/pypi-group/simple/ \
    --trusted-host nexus-nexus-repository-manager.nexus.svc.cluster.local \
    pure-translation

USER spark
