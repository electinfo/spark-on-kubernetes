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

# Stage 1: Fetch dependencies using Maven
FROM maven:3.9-eclipse-temurin-21 AS deps
WORKDIR /deps
ADD pom.xml .
ADD pom-hive4.xml .
# S3A dependencies go to /deps/jars (merged into /opt/spark/jars)
RUN mvn dependency:copy-dependencies -f pom.xml -DoutputDirectory=/deps/jars -DincludeScope=runtime
# Hive 4.x metastore client JARs go to separate directory (for spark.sql.hive.metastore.jars=path)
RUN mvn dependency:copy-dependencies -f pom-hive4.xml -DoutputDirectory=/deps/hive4 -DincludeScope=runtime

# Stage 2: Build final image
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

# Copy S3A dependencies from Maven stage
COPY --from=deps /deps/jars/*.jar /opt/spark/jars/

# Remove bundled Hive 2.3.x JARs and replace with Hive 4.1.0
# This fixes "Invalid method name: 'get_table'" error when connecting to Hive 4.x metastore
RUN rm -f /opt/spark/jars/hive-*.jar
COPY --from=deps /deps/hive4/*.jar /opt/spark/jars/

USER spark