# spark-base: Foundation Spark image with Python packages
# All other Spark images inherit from this
#
# RUNTIME CONFIG FILES (mounted via K8s ConfigMap):
# ┌─────────────────────────────────┬─────────────────────────────────┐
# │ Source (this repo)              │ Mount Path                      │
# ├─────────────────────────────────┼─────────────────────────────────┤
# │ config/core-site.xml            │ /opt/spark/conf/core-site.xml   │
# │ config/spark-defaults.conf      │ /opt/spark/conf/spark-defaults.conf │
# └─────────────────────────────────┴─────────────────────────────────┘

FROM apache/spark:4.1.0-scala2.13-java21-python3-ubuntu

USER root

RUN apt-get update && apt-get install -y --no-install-recommends python-is-python3 && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    pyarrow>=15.0.0 \
    pandas>=2.2.0 \
    numpy \
    s3fs \
    boto3 \
    grpcio>=1.76.0 \
    grpcio-status>=1.76.0 \
    zstandard>=0.25.0 \
    py4j \
    PyYAML \
    requests \
    graphframes \
    opensearch-py>=2.4.0

ENV HADOOP_CONF_DIR=/opt/spark/conf

USER spark