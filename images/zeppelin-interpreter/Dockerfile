# zeppelin-interpreter: Zeppelin interpreter pod (Spark driver)
# Spawned by Zeppelin server to run notebooks
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
# RUNTIME VOLUMES (defined in config/100-interpreter-spec.yaml):
# ┌─────────────────────────────────────────┬─────────────────────────────────┐
# │ Volume                                  │ Mount Path                      │
# ├─────────────────────────────────────────┼─────────────────────────────────┤
# │ NFS 10.10.0.10:/volume1/spark-checkpoints │ /mnt/spark-checkpoints        │
# │ PVC zeppelin-ivy-cache-pvc              │ /home/zeppelin/.ivy2            │
# │ emptyDir spark-local                    │ /tmp/spark-local                │
# │ ConfigMap spark-conf                    │ /opt/spark/conf                 │
# └─────────────────────────────────────────┴─────────────────────────────────┘

ARG REGISTRY=registry.container-registry.svc.cluster.local:5000
FROM ${REGISTRY}/electinfo/spark-base:latest

USER root

# Download Zeppelin interpreter JAR for Spark communication
ENV ZEPPELIN_VERSION=0.12.0
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/* && \
    curl -fsSL https://archive.apache.org/dist/zeppelin/zeppelin-${ZEPPELIN_VERSION}/zeppelin-${ZEPPELIN_VERSION}-bin-all.tgz | \
    tar -xz --strip-components=1 -C /tmp zeppelin-${ZEPPELIN_VERSION}-bin-all/interpreter/spark && \
    mkdir -p /opt/zeppelin/interpreter/spark && \
    cp /tmp/interpreter/spark/*.jar /opt/zeppelin/interpreter/spark/ && \
    rm -rf /tmp/interpreter

ENV ZEPPELIN_HOME=/opt/zeppelin

USER spark
