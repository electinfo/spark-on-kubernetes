# spark-connect-server: Spark Connect server for remote Spark access
# Runs as StatefulSet/Deployment exposing gRPC endpoint
#
# RUNTIME CONFIG FILES (mounted via K8s ConfigMap):
# ┌─────────────────────────────────────────┬─────────────────────────────────────────┐
# │ Source (this repo)                      │ Mount Path                              │
# ├─────────────────────────────────────────┼─────────────────────────────────────────┤
# │ config/core-site.xml                    │ /opt/spark/conf/core-site.xml           │
# │ config/spark-defaults.conf              │ /opt/spark/conf/spark-defaults.conf     │
# │ config/executor-pod-template.yaml       │ /opt/spark/conf/executor-pod-template.yaml │
# └─────────────────────────────────────────┴─────────────────────────────────────────┘
#
# RUNTIME VOLUMES:
# ┌─────────────────────────────────────────┬─────────────────────────────────┐
# │ Volume                                  │ Mount Path                      │
# ├─────────────────────────────────────────┼─────────────────────────────────┤
# │ NFS 10.10.0.10:/volume1/spark-checkpoints │ /mnt/spark-checkpoints        │
# └─────────────────────────────────────────┴─────────────────────────────────┘

ARG REGISTRY=registry.container-registry.svc.cluster.local:5000
FROM ${REGISTRY}/electinfo/spark-base:latest

USER root

# electinfo-common from Nexus PyPI (cache-busted on each triggered build)
ARG PIP_CACHE_EPOCH=0
RUN pip install --no-cache-dir \
    --index-url http://nexus-nexus-repository-manager.nexus.svc.cluster.local:8081/repository/pypi-group/simple/ \
    --trusted-host nexus-nexus-repository-manager.nexus.svc.cluster.local \
    electinfo-common

USER spark