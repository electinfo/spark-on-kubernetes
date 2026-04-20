#!/usr/bin/env python3
"""
Enforces config-as-code interpreter settings at Zeppelin startup.

Patches interpreter.json to ensure runtime values match the desired
configuration, overriding any stale PVC-persisted values.

Usage: python3 patch-interpreter.py <path-to-interpreter.json>
"""
import json, sys

f = sys.argv[1]
c = json.load(open(f))

for k, s in c.get("interpreterSettings", {}).items():

    # Spark: disable unsupported version check (Spark 4.x not yet in allowlist)
    if s.get("group") == "spark":
        for pkey, pval in s.get("properties", {}).items():
            if "enableSupportedVersionCheck" in pkey and isinstance(pval, dict):
                pval["value"] = "false"

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

json.dump(c, open(f, "w"), indent=2)
print("interpreter.json patched successfully")
