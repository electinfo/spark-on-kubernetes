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

# Stage 1: Download Hive 4.x metastore client dependencies
FROM maven:3.9-eclipse-temurin-21 AS hive4-deps
WORKDIR /deps
COPY pom-hive4.xml .
# Download ALL transitive dependencies - IsolatedClientLoader needs complete classpath
RUN mvn dependency:copy-dependencies -f pom-hive4.xml -DoutputDirectory=/deps/hive4 -DincludeScope=runtime

# Stage 2: Build executor image
ARG REGISTRY=registry.container-registry.svc.cluster.local:5000
FROM ${REGISTRY}/electinfo/spark-base:latest

USER root

# Copy Hive 4.x metastore client JARs
# Use with: spark.sql.hive.metastore.jars=path
#           spark.sql.hive.metastore.jars.path=file:///opt/spark/hive4
COPY --from=hive4-deps /deps/hive4/ /opt/spark/hive4/

USER spark