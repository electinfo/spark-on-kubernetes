#!/usr/bin/env python3
"""
Apply interpreter-overrides.json to Zeppelin's IN-MEMORY state via REST API.

Phase 1 (patch-interpreter.py) writes overrides to interpreter.json on disk,
but Zeppelin loads interpreter.json once at startup and treats its in-memory
state as the source of truth — so the disk patch alone never reaches the
running config. This script reconciles the two by:

  1. Reading conf/interpreter-overrides.json from the notebook git repo.
  2. For every group present in the override file, GETting the current
     setting via the REST API, merging the override option/properties on
     top, and PUTting it back.

Same merge semantics as patch-interpreter.py, so disk and memory stay
consistent. Idempotent — safe to re-run.

Usage: python3 apply-runtime-overrides.py
Env:
  ZEPPELIN_API_URL      (default: http://localhost:8080/api)
  ZEPPELIN_NOTEBOOK_DIR (default: /opt/zeppelin/notebook)
"""
import json
import os
import urllib.request

api = os.environ.get("ZEPPELIN_API_URL", "http://localhost:8080/api")
notebook_dir = os.environ.get("ZEPPELIN_NOTEBOOK_DIR", "/opt/zeppelin/notebook")
overrides_path = os.path.join(notebook_dir, "conf", "interpreter-overrides.json")

if not os.path.exists(overrides_path):
    print(f"No overrides at {overrides_path}; skipping API update")
    raise SystemExit(0)

overrides = json.load(open(overrides_path))

with urllib.request.urlopen(f"{api}/interpreter/setting") as r:
    settings = json.load(r)["body"]

for setting in settings:
    group = setting.get("group")
    if group not in overrides:
        continue
    ov = overrides[group]
    if "option" in ov:
        setting.setdefault("option", {}).update(ov["option"])
    if "properties" in ov:
        p = setting.setdefault("properties", {})
        for pk, pv in ov["properties"].items():
            if pk in p and isinstance(p[pk], dict):
                p[pk]["value"] = pv
            else:
                p[pk] = {"name": pk, "value": pv, "type": "string"}
    body = json.dumps({
        "name":         setting.get("name"),
        "group":        setting.get("group"),
        "properties":   setting.get("properties", {}),
        "dependencies": setting.get("dependencies", []),
        "option":       setting.get("option", {}),
    }).encode()
    req = urllib.request.Request(
        f"{api}/interpreter/setting/{setting['id']}",
        data=body, method="PUT",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req) as r:
            print(f"  {group} interpreter updated (HTTP {r.status})")
    except Exception as e:
        print(f"  {group} interpreter update FAILED: {e}")
