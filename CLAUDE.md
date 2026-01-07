# spark-on-kubernetes

> **SESSION START**: Always read all markdown files in ALL electinfo repositories at session start:
> ```
> ~/git/electinfo/CLAUDE.md
> ~/git/electinfo/*/CLAUDE.md
> ~/git/electinfo/docs/*.md
> ```

Consolidated Spark and Zeppelin deployment for electinfo Kubernetes cluster.

## Repository Purpose

This repo is the **single source of truth** for all Spark-related:
- Docker images (code/packages only, no configs)
- Configuration files (mounted at runtime via K8s)
- K8s deployment manifests (ArgoCD Applications)

## Key Principles

1. **Images = Code Only**: Dockerfiles install packages, NOT configuration files
2. **Configs = Runtime Mounts**: All configs mounted via K8s ConfigMaps/volumes
 3. **spark-base is the Foundation**: All Spark images inherit from spark-base
4. **One Place for Everything**: No Spark configs in ops/, zeppelin/, or other repos

## Directory Structure

| Path | Purpose |
|------|---------|
| `images/` | Dockerfiles for each container type |
| `config/` | Configuration files (ConfigMap sources) |
| `manifests/` | K8s deployment manifests |
| `docker-bake.hcl` | Build all images |

## Images

| Image | Base | Purpose |
|-------|------|---------|
| spark-base | apache/spark:4.1.0 | Foundation with Python packages |
| spark-executor | spark-base | K8s executor pods |
| spark-driver | spark-base | K8s driver pods |
| spark-connect-server | spark-base | Spark Connect gRPC |
| zeppelin-interpreter | spark-base | Interpreter pods (Spark driver) |
| zeppelin-server | zeppelin:0.12.0 + spark-base | Notebook server |

## Configuration Files

| File | Mount Path | Description |
|------|------------|-------------|
| core-site.xml | /opt/spark/conf/ | S3A/Hadoop config |
| spark-defaults.conf | /opt/spark/conf/ | Spark defaults |
| executor-pod-template.yaml | /opt/spark/conf/ | Executor volume mounts |
| 100-interpreter-spec.yaml | /opt/zeppelin/k8s/interpreter/ | Interpreter pod spec |

## Volume Strategy

### NFS Checkpoint Mount

GraphFrames ConnectedComponents requires shared checkpoints:
- Server: 10.10.0.10
- Export: /volume1/spark-checkpoints
- Mount: /mnt/spark-checkpoints

Configured in TWO places:
1. `executor-pod-template.yaml` - for executor pods
2. `100-interpreter-spec.yaml` - for interpreter (driver) pods

### DO NOT USE

Never use native Spark volume configs:
```
# WRONG - causes duplicate mount errors
spark.kubernetes.executor.volumes.nfs.checkpoint.mount.path=/mnt/spark-checkpoints
```

Only use pod templates for executor volumes.

## Building Images

```bash
cd ~/git/electinfo/spark-on-kubernetes

# Build all
docker buildx bake

# Build specific
docker buildx bake spark-executor

# Build and push
docker buildx bake --push
```

## Deployment

Manifests are deployed via ArgoCD:
- `manifests/spark-connect/application.yaml`
- `manifests/zeppelin/application.yaml`

## Common Issues

### "mountPath must be unique"

Cause: Both native volume config AND pod template define the same mount.

Fix:
1. Remove `spark.kubernetes.executor.volumes.*` from interpreter.json
2. Use ONLY `spark.kubernetes.executor.podTemplateFile` in spark-defaults.conf
3. Restart Zeppelin to clear cached interpreter settings

### ZEPPELIN_SPARK_CONF has old configs

The interpreter pod inherits configs from Zeppelin. Check:
```bash
kubectl exec -n zeppelin <interpreter-pod> -- env | grep ZEPPELIN_SPARK_CONF
```

If it contains volume configs, they're being passed from Zeppelin server's interpreter.json.

## Related Repos

| Repo | Relationship |
|------|--------------|
| electinfo/ops | ArgoCD root, cluster manifests |
| electinfo/zeppelin | Zeppelin notebooks (NOT images/configs) |
| electinfo/rundeck | Job definitions using Spark |