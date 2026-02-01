n# Spark on Kubernetes Roadmap

## Completed

- [x] Fix spark-connect-server CrashLoopBackOff (checkpoint.dir issue)
- [x] Refactor spark-connect-server to use --properties-file
- [x] Consolidate Spark/Zeppelin configs into this repo

## In Progress

- [ ] **Zeppelin Spark interpreter verification**
  - Zeppelin server is running
  - interpreter.json has `spark.remote` set to `sc://spark-connect.spark.svc:15002`
  - Need to test: spawn interpreter pod and run Spark notebook
  - May need to verify interpreter image has Spark Connect client

## Next Steps

### Zeppelin Integration
- [ ] Test Zeppelin Spark interpreter with Spark Connect
- [ ] Verify zeppelin-interpreter image compatibility with Spark 4.x Connect
- [ ] Update interpreter.json if needed for Spark Connect mode

### Configuration Cleanup
- [ ] Remove duplicate configs from `config/` directory (now in manifests)
- [ ] Align Zeppelin's spark-conf ConfigMap with spark-connect-server's spark-defaults.conf

### Testing
- [ ] Create test notebook that connects to Spark Connect
- [ ] Test rundeck jobs with Spark Connect endpoint
- [ ] Verify Hive metastore connectivity
- [ ] Test GraphFrames with NFS checkpoint mounts

### Documentation
- [ ] Add Spark Connect client connection examples
- [ ] Document rundeck job configuration for Spark Connect
- [ ] Add Spark Operator integration guide

## Future

- [ ] Consider dynamic allocation for spark-connect executors
- [ ] Add Grafana dashboards for Spark Connect metrics
- [ ] Implement executor autoscaling based on queue depth
