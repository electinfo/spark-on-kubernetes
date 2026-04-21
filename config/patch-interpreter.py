#!/usr/bin/env python3
"""
Enforces config-as-code interpreter settings at Zeppelin startup.

Patches interpreter.json to ensure runtime values match the desired
configuration, overriding any stale PVC-persisted values.

Override priority (highest wins):
  1. interpreter-overrides.json in the notebook repo (./zeppelin git repo)
  2. Hard-coded rules below
  3. Zeppelin defaults

Usage: python3 patch-interpreter.py <path-to-interpreter.json>
"""
import json, os, sys

f = sys.argv[1]
c = json.load(open(f))

for k, s in c.get("interpreterSettings", {}).items():

    # Spark: disable unsupported version check (Spark 4.x not yet in allowlist)
    if s.get("group") == "spark":
        for pkey, pval in s.get("properties", {}).items():
            if "enableSupportedVersionCheck" in pkey and isinstance(pval, dict):
                pval["value"] = "false"

    # Markdown: run in-process (no K8s pod) — interpreter pods for %md always fail
    if s.get("group") == "md":
        s.setdefault("option", {})["remote"] = False

    # JDBC: point to db-postgis in default namespace
    if s.get("group") == "jdbc":
        p = s.get("properties", {})
        for key, val in {
            "default.driver": "org.postgresql.Driver",
            "default.url": "jdbc:postgresql://db-postgis-master.default.svc.cluster.local:5432/electinfo",
            "default.user": "postgres",
            "default.password": "electinfo",
        }.items():
            if key in p and isinstance(p[key], dict):
                p[key]["value"] = val

# Apply overrides from the notebook git repo (electinfo/zeppelin).
# Any interpreter group listed in conf/interpreter-overrides.json takes
# precedence over the hard-coded rules above.  This lets runtime tweaks
# (e.g. option.remote, property values) be committed to ./zeppelin and
# picked up automatically on the next pod restart without touching this file.
NOTEBOOK_DIR = os.environ.get("ZEPPELIN_NOTEBOOK_DIR", "/opt/zeppelin/notebook")
overrides_path = os.path.join(NOTEBOOK_DIR, "conf", "interpreter-overrides.json")
if os.path.exists(overrides_path):
    overrides = json.load(open(overrides_path))
    settings = c.get("interpreterSettings", {})
    for k, s in settings.items():
        group = s.get("group", "")
        if group in overrides:
            ov = overrides[group]
            # Merge option flags
            if "option" in ov:
                s.setdefault("option", {}).update(ov["option"])
            # Merge property values
            if "properties" in ov:
                p = s.setdefault("properties", {})
                for pk, pv in ov["properties"].items():
                    if pk in p and isinstance(p[pk], dict):
                        p[pk]["value"] = pv
                    else:
                        p[pk] = {"value": pv}
    print(f"Applied overrides from {overrides_path}: {list(overrides.keys())}")
else:
    print(f"No notebook overrides found at {overrides_path}")

json.dump(c, open(f, "w"), indent=2)
print("interpreter.json patched successfully")
