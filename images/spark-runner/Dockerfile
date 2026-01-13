# spark-runner: Spark client for executing Spark jobs via Spark Connect
# Used as sidecar in rundeck for running spark-submit commands
#
# JAR files are mounted from the rundeck container via shared volume

ARG REGISTRY=registry.container-registry.svc.cluster.local:5000
FROM ${REGISTRY}/electinfo/spark-connect-server:latest

USER spark