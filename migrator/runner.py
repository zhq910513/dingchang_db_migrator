from __future__ import annotations
import importlib
import sys
import json
from .state import reset_state

TABLE_MODULE_MAP = {
    "role": ("migrator.tables.role", "role_new"),
    "user": ("migrator.tables.user", "user_new"),
}

def run_one(name: str, mode: str):
    modpath, _ = TABLE_MODULE_MAP[name]
    mod = importlib.import_module(modpath)
    return mod.run(mode=mode)

def main(argv):
    if len(argv) < 2:
        raise SystemExit("Usage: python -m migrator.runner <full|inc> <table1> [table2 ...]")
    cmd = argv[0].lower()
    mode = "updated_at"
    tables = argv[1:]
    for t in tables:
        if t not in TABLE_MODULE_MAP:
            raise SystemExit(f"Unknown table {t}. Supported: {', '.join(TABLE_MODULE_MAP.keys())}")

    if cmd == "full":
        for t in tables:
            _, tgt = TABLE_MODULE_MAP[t]
            reset_state(tgt)
        out = [run_one(t, mode) for t in tables]
        print(json.dumps(out, ensure_ascii=False, indent=2))
        return

    if cmd == "inc":
        out = [run_one(t, mode) for t in tables]
        print(json.dumps(out, ensure_ascii=False, indent=2))
        return

    raise SystemExit("Usage: python -m migrator.runner <full|inc> <table1> [table2 ...]")

if __name__ == "__main__":
    main(sys.argv[1:])
