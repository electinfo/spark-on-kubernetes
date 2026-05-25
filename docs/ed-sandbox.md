# ED Sandbox

> Debug Unity Catalog for iterating on catalog-plugin (`PatchedUCSingleCatalog`, `PatchedUCSingleCatalogDelta`) and UC/Delta interaction failure modes at ~30s/iteration instead of running the full EE pipeline smoke arc at ~30 min/cycle.

**Catalog name:** `enterprise_debug` ("ED" in shorthand)
**Catalog id:** `342a939c-bddc-44d1-a27f-b955c76559a3`
**Storage root:** `s3a://hive-warehouse/enterprise_debug/`
**Created:** 2026-05-24 (during the `spark-on-kubernetes#19` Delta-write postmortem)
**Home ticket:** [`electinfo/spark-on-kubernetes#19`](https://github.com/electinfo/spark-on-kubernetes/issues/19)
**Codification ticket:** [`electinfo/enterprise#2122`](https://github.com/electinfo/enterprise/issues/2122)

---

## When to use ED

Use the ED sandbox **before opening any PR that touches**:

- `images/spark-base/src/info/elect/spark/catalog/PatchedUCSingleCatalog.java`
- `images/spark-base/src/info/elect/spark/catalog/PatchedUCSingleCatalogDelta.java`
- Any other class in `info.elect.spark.catalog.*` that affects how UC, Delta, or Parquet writes flow through the catalog plugin

Also use it as the default place to reproduce **any UC/catalog/Delta-write defect** that the EE pipeline surfaces. It exists so that the next debug cycle doesn't burn a 30-minute production smoke run.

## When NOT to use ED

- Pipeline-level validation (use the actual EE smoke arc in `airflow/dags/enterprise_ee_*` for that — ED is for catalog-plugin behavior, not pipeline semantics).
- Steve-owned production catalogs (`fec_filings.*`, `fec_bulk.*`, `quicksilver.*`, `platinum.*`, `bullion.*`) — Steve coordination required regardless of catalog plugin state.
- Production data — ED is a sandbox with a single 57,708-row seed slice; it is not a stand-in for real cycle data.

## Persistent state (do not delete)

These artifacts are the durable surface of the sandbox; future debug sessions reuse them.

| Artifact | Path | Purpose |
|---|---|---|
| UC catalog | `enterprise_debug` | The sandbox itself; created via UC REST `POST /catalogs` |
| Schemas | `enterprise_debug.{bronze, silver, gold}` | Mirrors EE's bronze/silver/gold layering |
| Seed slice (parquet) | `s3a://hive-warehouse/enterprise_debug/raw/bronze.sb_oct2022/` | 57,708-row SB17 slice from cycle 2024 bronze, `year_month=202210`. Cheap, real-shaped, reusable. |
| Storage root | `s3a://hive-warehouse/enterprise_debug/pipeline-storage/<schema>/<table>/` | Where new sandbox tables write to |

**Do not** drop the seed slice between sessions. Other artifacts (per-session probe tables, S3 orphans) are cheap to leave but cheap to clean up; preference is to leave them so future sessions can compare.

## Pre-requisites

- Network access from the worker pod (any `airflow-worker-*` in namespace `airflow`) to:
  - Spark Connect: `sc://spark-connect.spark.svc.cluster.local:15002`
  - UC REST: `http://app-unity-catalog.electinfo.svc.cluster.local:8080`
- UC catalog `enterprise_debug` already exists (the operator created it 2026-05-24); no setup beyond `kubectl exec` into a worker pod.

## Workflow

The fast iteration loop is direct `spark-connect` probes — not the DLT pipeline framework. Each probe is one to a few SQL statements; you don't need pipeline yamls or DAG triggers.

### 1. Open a session on a worker

```bash
kubectl exec -it -n airflow airflow-worker-XXXXX -c worker -- bash
```

### 2. Reproduce the failure on the seed slice

Use `df.write.format("delta").saveAsTable("enterprise_debug.silver.<probe-name>")` (or whatever the failure shape requires). For example, the canonical Delta-on-CTAS-through-catalog reproduction:

```python
from pyspark.sql import SparkSession
s = SparkSession.builder.remote("sc://spark-connect.spark.svc.cluster.local:15002").getOrCreate()

# Read the seed
df = s.read.parquet("s3a://hive-warehouse/enterprise_debug/raw/bronze.sb_oct2022/")
print("seed rows:", df.count())   # expect 57708

# Reproduce Delta-CTAS failure (the original spark-on-kubernetes#19 symptom)
df.write.format("delta").partitionBy("cycle", "year_month") \
    .saveAsTable("enterprise_debug.silver.sb_delta_v1")
# expect: UnsupportedOperationException: Cannot stage non-V1 table: DeltaTableV2
#         (pre-PR #25) or one of the F1/F2/F3 failure modes below (post-PR #25)
```

### 3. Make the catalog-plugin change locally

Edit `images/spark-base/src/info/elect/spark/catalog/*.java`. Build a new image via the standard Tekton `spark-images-build` flow (or `docker bake` locally for quick iteration; check the build instructions in the per-image Dockerfile). Push to the cluster registry, restart `spark-connect-server`.

### 4. Re-run the probe

The same probe should now succeed (or fail differently — at which point you're investigating a new failure mode, not the original one).

### 5. Clean up per-session probe tables

```python
s.sql("DROP TABLE IF EXISTS enterprise_debug.silver.sb_delta_v1")
# ... etc for each probe table you created
```

Leave the seed slice (`enterprise_debug.raw/bronze.sb_oct2022/`) and the persistent schemas in place.

## Known failure modes (catalog plugin + UC + Delta interaction)

Reproduced in ED 2026-05-25 against the post-PR #25 image. Documented as durable knowledge so future debug sessions don't repeat the investigation. See [`spark-on-kubernetes#19`](https://github.com/electinfo/spark-on-kubernetes/issues/19) comments for the full trace.

### F1 — UC name validation rejects `delta.<s3-uri>` registration

`DeltaTable.createIfNotExists(spark).location(path).execute()` internally tries to register `delta.<full-s3-uri>` as a UC table name. UC's REST validator (`^[A-Za-z0-9_-]+$`) rejects names containing `:`, `/`, `.`. **Not a bug in PR #25** — structural incompatibility between Delta's path-as-name pattern and UC's strict-name catalog. The `bootstrapDeltaLog`-via-`DeltaTable.createIfNotExists` approach is fundamentally on the wrong shape.

**Symptom:** `400 INVALID_ARGUMENT: Name cannot contain a period, space, forward-slash, or control characters`.

**Recommended fix shape:** bootstrap the Delta log via raw Hadoop `FileSystem.create` and skip Delta's UC name registration entirely. Tracked as path B in [`electinfo/enterprise#2112`](https://github.com/electinfo/enterprise/issues/2112) (D2).

### F2 — Cached `DeltaTableV2` doesn't refresh after bootstrap

Even when bootstrap successfully writes `_delta_log/00000000000000000000.json` to S3 after `super.createTable` returns, the `DeltaTableV2` returned by `super.createTable` has memoized `toBaseRelation` / `toLogicalRelation` against an empty pre-bootstrap path. Subsequent `WriteIntoDeltaBuilder.insert` re-uses the cached relation and reports `DELTA_PATH_DOES_NOT_EXIST`, even though the path is valid via `spark.read.format("delta").load(...)`.

**Symptom:** `DeltaAnalysisException: [DELTA_PATH_DOES_NOT_EXIST]`, with the path visibly present on S3 + readable via a fresh `DeltaLog` instance.

**Recommended fix shape:** invalidate the cached relation via `DeltaLog.clearCache()` for the path after bootstrap. Or sidestep the bootstrap entirely by using a post-write `CONVERT TO DELTA` step (path C / D3 in `enterprise#2112`).

### F3 — V2 staging proxy's `commitStagedChanges` no-op leaves V1Table parquet unwritten

Spark's `AtomicCreateTableAsSelectExec` expects `commitStagedChanges` to make the staged write durable; the proxy in PR #25 has a no-op `commitStagedChanges`, which makes the V2 write code think nothing was written. For parquet V1Table, `Table does not support writes` surfaces from `V2Writes$$anon$1.asWritable`.

**Symptom:** `AnalysisException: Table does not support writes` (parquet CTAS), even after PR #25 fixed the "Cannot stage non-V1 table" symptom.

**Recommended fix shape:** drop the V2 staging proxy in favor of `BasicStagedTable` that defers data write to `commitStagedChanges`. Touches Steve's prod `PatchedUCSingleCatalog` — wider blast radius (path D / D4 in `enterprise#2112`).

## Sandbox state inventory (as of 2026-05-25)

Post-postmortem state in `enterprise_debug.silver`. The probe tables can be cleaned up at any time; the seed slice must stay.

| UC entry | Rows | Purpose |
|---|---|---|
| `parq_ins_v7a72ae` | 57,708 | Parquet sanity baseline (working) |
| `delta_precreate_t943efe` | 500 | Hand-created Delta via REST (working) |
| `create_only_v7a72ae` | 0 | createTable + nothing else |
| `create_delta_only_v7a72ae` | 0 | Delta variant of above |
| `sb_delta_v1` | 0 | Failure repro target (Cannot stage non-V1 / DELTA_PATH_DOES_NOT_EXIST depending on image) |
| `sb_test_v3` | 57,708 | Parquet via `PatchedUCSingleCatalog` (working) |

Plus several S3 orphans under `s3a://hive-warehouse/enterprise_debug/silver/delta_*` and `probe_s3a_*` from past iterations — each has a `_delta_log/00000000000000000000.json` + parquet files, no UC entry. Cleanup is one-liner per name when convenient.

## References

- [`spark-on-kubernetes#19`](https://github.com/electinfo/spark-on-kubernetes/issues/19) — Original Delta-write postmortem; comment thread is the canonical investigation history.
- [`electinfo/enterprise#2112`](https://github.com/electinfo/enterprise/issues/2112) — Delta migration sub-epic (D1-D6 path tracking).
- [`electinfo/enterprise#2122`](https://github.com/electinfo/enterprise/issues/2122) — D6: codify the ED-first validation gate (this doc plus `CONTRIBUTING.md`).
- [`unitycatalog/unitycatalog`](https://github.com/unitycatalog/unitycatalog) — Upstream UC. Path F in #2112 is the "report cause-1 upstream" item.
