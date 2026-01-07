# zeppelin-server: Zeppelin notebook server with Spark
# Manages notebooks and spawns interpreter pods
#
# RUNTIME CONFIG FILES (mounted via K8s ConfigMap/Volume):
# ┌─────────────────────────────────────────┬─────────────────────────────────────────────────────┐
# │ Source (this repo)                      │ Mount Path                                          │
# ├─────────────────────────────────────────┼─────────────────────────────────────────────────────┤
# │ config/100-interpreter-spec.yaml        │ /opt/zeppelin/k8s/interpreter/100-interpreter-spec.yaml │
# │ config/core-site.xml                    │ /opt/spark/conf/core-site.xml                       │
# │ config/spark-defaults.conf              │ /opt/spark/conf/spark-defaults.conf                 │
# └─────────────────────────────────────────┴─────────────────────────────────────────────────────┘
#
# RUNTIME VOLUMES:
# ┌─────────────────────────────────────────┬─────────────────────────────────┐
# │ Volume                                  │ Mount Path                      │
# ├─────────────────────────────────────────┼─────────────────────────────────┤
# │ PVC zeppelin-notebook-pvc               │ /opt/zeppelin/notebook          │
# │ PVC zeppelin-conf-pvc                   │ /opt/zeppelin/conf              │
# │ ConfigMap spark-conf                    │ /opt/spark/conf                 │
# │ ConfigMap zeppelin-interpreter-spec     │ /opt/zeppelin/k8s/interpreter   │
# └─────────────────────────────────────────┴─────────────────────────────────┘

ARG REGISTRY=registry.container-registry.svc.cluster.local:5000

# Stage 1: Get Spark from spark-base
FROM ${REGISTRY}/electinfo/spark-base:latest AS spark

# Stage 2: Zeppelin with Spark baked in
FROM apache/zeppelin:0.12.0

USER root

RUN apt-get update

# Install Java 21 and core tools
RUN apt-get install -y openjdk-21-jdk git openssh-client curl

# Install pyenv build dependencies
RUN apt-get install -y build-essential libssl-dev zlib1g-dev libbz2-dev \
    libreadline-dev libsqlite3-dev libncursesw5-dev xz-utils \
    tk-dev libxml2-dev libxmlsec1-dev libffi-dev liblzma-dev

RUN apt-get clean && rm -rf /var/lib/apt/lists/*

# Install pyenv and Python 3.10
ENV PYENV_ROOT=/opt/pyenv
ENV PATH="$PYENV_ROOT/shims:$PYENV_ROOT/bin:$PATH"
RUN curl https://pyenv.run | bash && \
    pyenv install 3.10 && \
    pyenv global 3.10 && \
    pip install --upgrade pip

# Install PySpark dependencies (match spark-base)
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
    graphframes

# Configure git for zeppelin user
RUN mkdir -p /home/zeppelin/.ssh && \
    chmod 700 /home/zeppelin/.ssh && \
    ssh-keyscan github.com >> /home/zeppelin/.ssh/known_hosts 2>/dev/null && \
    chmod 644 /home/zeppelin/.ssh/known_hosts && \
    chown -R 1000:1000 /home/zeppelin/.ssh && \
    git config --system user.email "zeppelin@elect.info" && \
    git config --system user.name "Zeppelin Server" && \
    git config --system --add safe.directory /opt/zeppelin/notebook

# Set Java 21 as default
ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Copy Spark from spark-base (binaries only)
COPY --from=spark /opt/spark /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

USER 1000