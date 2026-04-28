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

# Install Python 3.11 (base image has 3.10, siege_utilities requires 3.11+)
RUN apt-get update && apt-get install -y --no-install-recommends \
    software-properties-common \
    && add-apt-repository -y ppa:deadsnakes/ppa \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
    python3.11 \
    python3.11-venv \
    python3.11-dev \
    python3.11-distutils \
    libgdal-dev \
    libgeos-dev \
    libproj-dev \
    libspatialindex-dev \
    && rm -rf /var/lib/apt/lists/*

# Make Python 3.11 the default
RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 1 \
    && update-alternatives --install /usr/bin/python python /usr/bin/python3.11 1

# Install pip for Python 3.11
RUN python3.11 -m ensurepip --upgrade \
    && python3.11 -m pip install --no-cache-dir --upgrade pip setuptools wheel

# Layer 1: siege_utilities with all extras (heaviest — cache this layer)
RUN python3.11 -m pip install --no-cache-dir \
    "siege-utilities[geo,data,geodjango,database,h3]>=3.12.0"

# Layer 2: Enterprise pipeline CLI deps
RUN python3.11 -m pip install --no-cache-dir \
    "typer[all]>=0.9.0" \
    "pydantic>=2.0.0" \
    "hydra-core>=1.3.0" \
    "omegaconf>=2.3.0" \
    "rapidfuzz>=3.0.0" \
    "rich>=13.0.0" \
    "kafka-python>=2.0.0" \
    "boto3>=1.28.0"

# Layer 3: Django + REST + PostGIS
RUN python3.11 -m pip install --no-cache-dir \
    "Django>=5.0" \
    "djangorestframework>=3.15.0" \
    "djangorestframework-gis>=1.0" \
    "django-cors-headers>=4.0" \
    "django-extensions>=3.0" \
    "psycopg2-binary>=2.9.0"

# Layer 4: Data processing extras
RUN python3.11 -m pip install --no-cache-dir \
    "duckdb>=0.7.0" \
    "delta-spark>=3.0.0"

# Layer 5: Spark Connect client (ELE-2255)
RUN python3.11 -m pip install --no-cache-dir \
    "pyspark[connect]>=3.5.0"

# Install kubectl (required for SparkOperator job submission via kubectl apply)
RUN apt-get update && apt-get install -y --no-install-recommends \
    apt-transport-https ca-certificates curl gnupg \
    && curl -fsSL https://pkgs.k8s.io/core:/stable:/v1.29/deb/Release.key \
       | gpg --dearmor -o /etc/apt/keyrings/kubernetes-apt-keyring.gpg \
    && echo 'deb [signed-by=/etc/apt/keyrings/kubernetes-apt-keyring.gpg] https://pkgs.k8s.io/core:/stable:/v1.29/deb/ /' \
       > /etc/apt/sources.list.d/kubernetes.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends kubectl \
    && rm -rf /var/lib/apt/lists/*

# Install gdal-bin / ogr2ogr for spatial ingest jobs (ELE-2354)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gdal-bin \
    && rm -rf /var/lib/apt/lists/*

# Create directory for enterprise code mount
RUN mkdir -p /opt/enterprise && chown spark:spark /opt/enterprise

USER spark
