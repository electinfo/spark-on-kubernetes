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

# Stage 1: Fetch S3A dependencies using Maven
FROM maven:3.9-eclipse-temurin-21 AS deps
WORKDIR /deps
ADD pom.xml .
# S3A dependencies go to /deps/jars (merged into /opt/spark/jars)
RUN mvn dependency:copy-dependencies -f pom.xml -DoutputDirectory=/deps/jars -DincludeScope=runtime

# Stage 2: Build final image
FROM apache/spark:4.1.0-scala2.13-java21-python3-ubuntu

USER root

RUN apt-get update && apt-get install -y --no-install-recommends python-is-python3 zip && rm -rf /var/lib/apt/lists/*

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

# Copy Ivy settings for local maven repository
COPY conf/ivysettings.xml /opt/spark/conf/ivysettings.xml

# Patch ALL ivy JARs to use our custom ivysettings.xml as the default
# This ensures Hive's IsolatedClientLoader (which ignores external settings) uses our Nexus proxy
# Must patch both /opt/spark/jars/ivy-*.jar AND /opt/spark/hive4/ivy-*.jar
RUN mkdir -p /tmp/ivy-patch/org/apache/ivy/core/settings && \
    cp /opt/spark/conf/ivysettings.xml /tmp/ivy-patch/org/apache/ivy/core/settings/ivysettings.xml && \
    cd /tmp/ivy-patch && \
    for IVY_JAR in $(find /opt/spark -name "ivy-*.jar" 2>/dev/null); do \
        echo "Patching $IVY_JAR with custom ivysettings.xml" && \
        zip -u "$IVY_JAR" org/apache/ivy/core/settings/ivysettings.xml; \
    done && \
    rm -rf /tmp/ivy-patch

USER spark
