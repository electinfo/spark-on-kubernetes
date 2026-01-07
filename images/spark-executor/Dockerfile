# spark-executor: Spark executor container for K8s
# Used by: spark.kubernetes.container.image
#
# RUNTIME CONFIG FILES (mounted via K8s ConfigMap):
# ┌─────────────────────────────────┬─────────────────────────────────┐
# │ Source (this repo)              │ Mount Path                      │
# ├─────────────────────────────────┼─────────────────────────────────┤
# │ config/core-site.xml            │ /opt/spark/conf/core-site.xml   │
# └─────────────────────────────────┴─────────────────────────────────┘
#
# RUNTIME VOLUMES (defined in config/executor-pod-template.yaml):
# ┌─────────────────────────────────┬─────────────────────────────────┐
# │ Volume                          │ Mount Path                      │
# ├─────────────────────────────────┼─────────────────────────────────┤
# │ NFS 10.10.0.10:/volume1/spark-checkpoints │ /mnt/spark-checkpoints │
# └─────────────────────────────────┴─────────────────────────────────┘

ARG REGISTRY=localhost:32000
FROM ${REGISTRY}/electinfo/spark-base:latest

USER spark