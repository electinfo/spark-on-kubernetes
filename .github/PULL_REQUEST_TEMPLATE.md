## Summary

<!-- 1-3 bullets. What changes; why. -->

## Context

<!-- Linked issues. e.g. Refs: #19, Closes: enterprise#2122 -->

## Test plan

<!-- Commands run; what passed; what's still pending. Include image build, deployment, and any in-cluster verification. -->

- [ ] Image builds (`docker bake` or Tekton `spark-images-build`)
- [ ] Manifest applies cleanly (`kubectl apply --dry-run=server -f manifests/...`)
- [ ] Smoke test passes (specify which)

## Catalog plugin validation

If this PR touches `images/spark-base/src/info/elect/spark/catalog/*.java`, the **ED sandbox validation gate** applies — see [CONTRIBUTING.md](../CONTRIBUTING.md#catalog-plugin-changes-require-ed-sandbox-validation) and [docs/ed-sandbox.md](../docs/ed-sandbox.md).

- [ ] **N/A** — this PR does not modify `info.elect.spark.catalog.*`.
- [ ] **Validated** — I reproduced the behavior in the ED sandbox (`enterprise_debug` UC catalog) on the SB17 seed slice before opening this PR. Evidence below:
  - Probe commands: <!-- paste the `spark-connect` snippets you ran -->
  - Before behavior: <!-- error message / row count / etc. -->
  - After behavior: <!-- post-fix result -->
  - F1/F2/F3 known failure modes: <!-- which (if any) still apply, which were addressed -->

## Attribution

This PR follows the canonical electinfo no-attribution policy. No `Co-Authored-By`, no AI tool attribution in commit messages, PR body, or comments.
