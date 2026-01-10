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

# Stage 1: Get bin scripts from official Zeppelin image
FROM apache/zeppelin:0.12.0 AS zeppelin

# Stage 2: Build interpreter image from spark-base
FROM ${REGISTRY}/electinfo/spark-base:latest

USER root

# Copy Zeppelin bin scripts and interpreter JARs from official image
COPY --from=zeppelin /opt/zeppelin/bin /opt/zeppelin/bin
COPY --from=zeppelin /opt/zeppelin/interpreter/spark /opt/zeppelin/interpreter/spark

# Create logs directory and set permissions for spark user (uid 185)
RUN mkdir -p /opt/zeppelin/logs /opt/zeppelin/local-repo && \
    chown -R 185:185 /opt/zeppelin

ENV ZEPPELIN_HOME=/opt/zeppelin

USER spark
