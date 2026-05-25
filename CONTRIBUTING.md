# Contributing to electinfo/spark-on-kubernetes

This repo holds the Spark/Zeppelin Docker images, runtime configuration, and Kubernetes manifests for the electinfo cluster. The catalog plugin (`info.elect.spark.catalog.PatchedUCSingleCatalog` and `PatchedUCSingleCatalogDelta`) is the most production-load-bearing code here — it sits between every Spark write and Unity Catalog, and breaks in it cascade to every EE pipeline and Steve's ESQ daily.

Read this before opening a PR.

## Branch and merge convention

- Branch off `main`.
- PRs target `main` (no `develop` branch on this repo).
- **Merge-commit on merge by default** (verified 2026-05-25 against the most-recent 8 merges on `main`; all use GitHub's `Merge pull request #N from ...` style). Squash is appropriate when a multi-commit PR represents iterative drafting that doesn't need to be preserved (e.g., review-feedback amendments); use judgement.

## Catalog plugin changes require ED sandbox validation

**Any change to `images/spark-base/src/info/elect/spark/catalog/PatchedUCSingleCatalog.java` or `PatchedUCSingleCatalogDelta.java` must be validated against the ED sandbox before the PR is opened for review.**

This is per Steve's directive on [`#19`](https://github.com/electinfo/spark-on-kubernetes/issues/19#issuecomment-4535590314):

> i am not surprised that PatchedUCSingleCatalog does not seamlessly support delta tables. its probably worth the effort to make it, but this is a crucial single point of failure that needs to be robustly tested on non-production catalogs with non-production deployment rather than experimenting directly in the UC container or wrappers

The ED sandbox is the `enterprise_debug` UC catalog. Full procedure, persistent state, and known failure modes are documented in [`docs/ed-sandbox.md`](docs/ed-sandbox.md).

### What "validated against ED" means

At minimum:

1. Read [`docs/ed-sandbox.md`](docs/ed-sandbox.md) including the F1/F2/F3 known failure modes — so you know what's already documented and what's net-new.
2. Reproduce the failure mode you're fixing (or the behavior you're changing) using direct `spark-connect` probes against `enterprise_debug.silver.<your-probe>`. The seed slice at `enterprise_debug.raw/bronze.sb_oct2022/` is the standard starting point (57,708 rows of SB17 data; cheap; real-shaped).
3. Build the new catalog jar via the Tekton `spark-images-build` flow (or `docker bake` locally for fast iteration), push to the cluster registry, restart `spark-connect-server`, and re-run the same probe. Confirm the fix.
4. Document the probes you ran in the PR description (commands + observed before/after). The PR template prompts for this.

### Why this is a hard gate

- The catalog plugin is a single shared dependency of every Spark write in the cluster. A regression here breaks both EE (`fec_filings_enterprise_experimental.*`) and Steve's ESQ daily simultaneously.
- The EE smoke arc historically burned 30 min per cycle to surface catalog-side failures. ED iteration is ~30 s. Using the EE smoke arc as your debug loop wastes hours of wall-clock and risks contaminating production parquet directories.
- "I tested it on my laptop" or "the unit tests pass" does not substitute. The behavior under test is the catalog plugin's interaction with the deployed UC, Spark Connect, and Delta library versions. Only the in-cluster ED sandbox exercises that combination.

### What is NOT under this gate

Changes that do **not** require ED validation:

- Docker image package updates that don't touch catalog code
- Spark/Zeppelin config (`config/*.xml`, `config/*.conf`)
- K8s manifests, ArgoCD Applications, Tekton tasks
- Documentation, READMEs, this file
- ROADMAP entries

For those, the standard PR review is sufficient.

## Other production-load-bearing areas

These don't have as formal a sandbox gate yet but warrant equivalent care:

- `images/spark-connect-server/Dockerfile` — singleton deployment; rolling restart drops in-flight sessions. Plan for a maintenance window if touching.
- `images/spark-base/Dockerfile` — base for every Spark image; any change cascades to executor + driver + connect-server + zeppelin.
- `manifests/spark-connect/` — ArgoCD-managed; mis-configuration causes pod restart loops.

## Attribution policy

This repo follows the canonical electinfo attribution policy from [`electinfo/electinfo_claude_skills`](https://github.com/electinfo/electinfo_claude_skills) CLAUDE.md:

> NEVER include AI/agent attribution (Co-Authored-By, "Generated with", etc.) in commits, PRs, issues, comments, or any public-facing content.

The `no-attribution.sh` hook in the canonical skills repo enforces this for Claude Code sessions on cyberpower. Other agent surfaces (Craft Agent in particular) do not have the hook wired — suppress manually.

## References

- [`spark-on-kubernetes#19`](https://github.com/electinfo/spark-on-kubernetes/issues/19) — ED Delta postmortem; canonical history of catalog plugin failure modes.
- [`electinfo/enterprise#2112`](https://github.com/electinfo/enterprise/issues/2112) — Delta migration sub-epic (path A-F tracking).
- [`electinfo/enterprise#2122`](https://github.com/electinfo/enterprise/issues/2122) — D6: codify the ED-first validation gate (this file plus `docs/ed-sandbox.md`).
