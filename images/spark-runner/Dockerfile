# spark-runner: Spark client with pre-built electinfo-generators JAR
# Used as sidecar in rundeck for executing Spark jobs via Spark Connect
#
# Extends spark-connect-server with:
# - Pre-built electinfo-generators.jar for static page generation

ARG REGISTRY=registry.container-registry.svc.cluster.local:5000
FROM ${REGISTRY}/electinfo/spark-connect-server:latest AS base

# Build stage - compile the Scala JAR
FROM eclipse-temurin:21-jdk AS builder

# Install sbt
RUN apt-get update && \
    apt-get install -y --no-install-recommends apt-transport-https curl gnupg && \
    echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list && \
    echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | tee /etc/apt/sources.list.d/sbt_old.list && \
    curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | gpg --dearmor | tee /etc/apt/trusted.gpg.d/sbt.gpg > /dev/null && \
    apt-get update && \
    apt-get install -y --no-install-recommends sbt git && \
    rm -rf /var/lib/apt/lists/*

# Clone and build
WORKDIR /build
RUN git clone --depth 1 https://github.com/electinfo/rundeck.git
WORKDIR /build/rundeck
RUN sbt assembly

# Final stage - copy JAR to spark image
FROM base

USER root
RUN mkdir -p /opt/electinfo/lib
COPY --from=builder /build/rundeck/target/scala-2.13/electinfo-generators.jar /opt/electinfo/lib/
RUN chown -R spark:spark /opt/electinfo

USER spark