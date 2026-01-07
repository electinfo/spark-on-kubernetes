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

ARG REGISTRY=localhost:32000
FROM ${REGISTRY}/electinfo/spark-base:latest

USER spark