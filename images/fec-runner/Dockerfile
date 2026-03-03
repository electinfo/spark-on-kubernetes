# fec-runner: Enterprise FEC pipeline runner extending spark-runner
#
# Inherits Spark Connect client, PySpark, pyarrow, pandas, boto3 etc.
# from spark-runner. Adds pure-translation CLI dependencies.
#
# The enterprise code is mounted at runtime from a git clone on magnum.
# The container startup script pip-installs it for the `fec` CLI entry point.
#
# Image: localhost:32000/electinfo/fec-cli:latest
# Deployment: ops/util-fec-runner/

ARG REGISTRY=registry.container-registry.svc.cluster.local:5000
FROM ${REGISTRY}/electinfo/spark-runner:latest

USER root

# Install pure-translation CLI dependencies not already in spark-runner
# (pyspark, pyarrow, pandas, boto3, grpcio, PyYAML, requests already in base)
RUN pip install --no-cache-dir \
    "typer[all]>=0.9.0" \
    "pydantic>=2.0.0" \
    "hydra-core>=1.3.0" \
    "omegaconf>=2.3.0" \
    "siege-utilities>=3.4.0" \
    "rapidfuzz>=3.0.0" \
    "rich>=13.0.0"

# Create directory for enterprise code mount
RUN mkdir -p /opt/enterprise && chown spark:spark /opt/enterprise

USER spark
