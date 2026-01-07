# spark-on-kubernetes

Consolidated Spark configuration, images, and K8s manifests for electinfo.

## Directory Structure

```
spark-on-kubernetes/
├── images/                         # Docker images (code only, no configs)
│   ├── spark-base/                 # Foundation: Spark + Python packages
│   ├── spark-executor/             # K8s executor pods
│   ├── spark-driver/               # K8s driver pods
│   ├── spark-connect-server/       # Spark Connect gRPC server
│   ├── zeppelin-server/            # Zeppelin notebook server
│   └── zeppelin-interpreter/       # Zeppelin interpreter (Spark driver)
├── config/                         # Configuration files (mounted at runtime)
│   ├── core-site.xml               # Hadoop/S3A config
│   ├── spark-defaults.conf         # Spark defaults
│   ├── executor-pod-template.yaml  # Executor volume mounts
│   └── 100-interpreter-spec.yaml   # Zeppelin interpreter pod spec
├── manifests/                      # K8s deployment manifests
│   ├── spark-connect/              # Spark Connect server deployment
│   │   ├── application.yaml        # ArgoCD Application
│   │   ├── base/                   # K8s resources
│   │   └── tasks/                  # Tekton pipelines
│   └── zeppelin/                   # Zeppelin deployment
│       ├── application.yaml        # ArgoCD Application
│       ├── base/                   # K8s resources
│       └── tasks/                  # Tekton pipelines
└── docker-bake.hcl                 # Build all images
```

## Design Principles

1. **Images contain code, not config** - Dockerfiles only install packages/binaries
2. **Configs are mounted at runtime** - Via K8s ConfigMaps/volumes
3. **Single source of truth** - All Spark/Zeppelin configs live here
4. **No inheritance between images** - Each Dockerfile is self-contained

## Images

| Image | Purpose | Used By |
|-------|---------|---------|
| spark-base | Reference image (not deployed) | - |
| spark-executor | Spark executor pods | `spark.kubernetes.container.image` |
| spark-driver | Spark driver pods | `spark.kubernetes.driver.container.image` |
| spark-connect-server | Spark Connect server | StatefulSet |
| zeppelin-server | Zeppelin notebook server | Deployment |
| zeppelin-interpreter | Zeppelin interpreter pods | spawned by zeppelin-server |

## Building Images

```bash
# Build all images
docker buildx bake

# Build specific image
docker buildx bake spark-executor

# Build with custom registry/tag
docker buildx bake --set "*.tags=myregistry/spark-executor:v1"
```

## Configuration

### Runtime Config Mounts

Configs are mounted via K8s ConfigMaps:

| Config File | Mount Path | Used By |
|-------------|------------|---------|
| core-site.xml | /opt/spark/conf/ | All Spark containers |
| spark-defaults.conf | /opt/spark/conf/ | All Spark containers |
| executor-pod-template.yaml | /opt/spark/conf/ | Spark driver |
| 100-interpreter-spec.yaml | /opt/zeppelin/k8s/interpreter/ | zeppelin-server |

### ConfigMap

Create from config files:
```bash
kubectl create configmap spark-conf \
  --from-file=config/core-site.xml \
  --from-file=config/spark-defaults.conf \
  --from-file=config/executor-pod-template.yaml \
  -n zeppelin
```

## Volume Mounts

### Checkpointing (NFS)

Spark checkpointing requires shared storage accessible by driver and executors:
- NFS server: 10.10.0.10
- Export: /volume1/spark-checkpoints
- Mount: /mnt/spark-checkpoints

Configured in:
- `executor-pod-template.yaml` (for executor pods)
- `100-interpreter-spec.yaml` (for interpreter/driver pods)

**DO NOT** use native Spark volume configs (`spark.kubernetes.executor.volumes.*`) -
they conflict with pod templates and cause "mountPath must be unique" errors.

## Deployment

### Via ArgoCD

```bash
# Apply ArgoCD Applications
kubectl apply -f manifests/spark-connect/application.yaml
kubectl apply -f manifests/zeppelin/application.yaml
```

### Manual

```bash
# Deploy Spark Connect
kubectl apply -k manifests/spark-connect/base/

# Deploy Zeppelin
kubectl apply -k manifests/zeppelin/base/
```

## Troubleshooting

### Duplicate mountPath Error

If executor pods fail with "mountPath must be unique":
1. Check that spark-defaults.conf uses ONLY `podTemplateFile`, not native volume configs
2. Verify interpreter.json has no `spark.kubernetes.executor.volumes.*` properties
3. Check ZEPPELIN_SPARK_CONF env var on interpreter pod for duplicates

### Python Package Mismatch

Ensure driver and executor images have identical Python packages to avoid UDF serialization errors.