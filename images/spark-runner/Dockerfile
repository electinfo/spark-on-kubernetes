# spark-runner: Spark client with sbt for building and running Scala jobs
# Used as sidecar in rundeck for executing Spark jobs via Spark Connect
#
# Extends spark-connect-server with:
# - sbt for building Scala projects
# - Write access to build directories

ARG REGISTRY=registry.container-registry.svc.cluster.local:5000
FROM ${REGISTRY}/electinfo/spark-connect-server:latest

USER root

# Install sbt for Scala builds
RUN apt-get update && \
    apt-get install -y --no-install-recommends apt-transport-https curl gnupg && \
    echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list && \
    echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | tee /etc/apt/sources.list.d/sbt_old.list && \
    curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | gpg --dearmor | tee /etc/apt/trusted.gpg.d/sbt.gpg > /dev/null && \
    apt-get update && \
    apt-get install -y --no-install-recommends sbt && \
    rm -rf /var/lib/apt/lists/*

# Create home directory for spark user with proper permissions
RUN mkdir -p /home/spark && chown spark:spark /home/spark

ENV HOME=/home/spark

USER spark