# enterprise-runner: Dheeraj's enterprise pipeline runner
#
# Extends spark-runner with siege_utilities (full geo stack),
# pure-translation CLI, and all enterprise pipeline dependencies.
#
# Key differences from fec-runner (Steve's image):
# - siege-utilities pinned to >=3.12.1 (RDH, redistricting, compactness, boundary providers)
# - Full [geo] extras (geopandas, shapely, pyproj, fiona, h3, rtree)
# - Full [data] extras (pyarrow, duckdb)
# - PostGIS client libraries (psycopg2)
# - Census geocoder, NCES, NLRB modules
#
# Image: localhost:32000/electinfo/enterprise-runner:latest
# Deployment: ops/util-enterprise-runner/

ARG REGISTRY=registry.container-registry.svc.cluster.local:5000
FROM ${REGISTRY}/electinfo/spark-runner:latest

USER root

# System dependencies for full geo stack
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgdal-dev \
    libgeos-dev \
    libproj-dev \
    libspatialindex-dev \
    && rm -rf /var/lib/apt/lists/*

# Python dependencies — enterprise pipeline + siege_utilities full stack
RUN pip install --no-cache-dir \
    "siege-utilities[geo,data,geodjango,database,h3]>=3.12.1" \
    "typer[all]>=0.9.0" \
    "pydantic>=2.0.0" \
    "hydra-core>=1.3.0" \
    "omegaconf>=2.3.0" \
    "rapidfuzz>=3.0.0" \
    "rich>=13.0.0" \
    "Django>=5.0" \
    "djangorestframework>=3.15.0" \
    "djangorestframework-gis>=1.0" \
    "django-cors-headers>=4.0" \
    "django-extensions>=3.0" \
    "psycopg2-binary>=2.9.0" \
    "pyarrow>=14.0.0" \
    "duckdb>=0.7.0" \
    "delta-spark>=3.0.0" \
    "censusgeocode>=0.5.2" \
    "geopy>=2.4.1" \
    "mapclassify>=2.5.0" \
    "h3>=4.0.0" \
    "rtree>=1.0.0"

# Create directory for enterprise code mount
RUN mkdir -p /opt/enterprise && chown spark:spark /opt/enterprise

USER spark
