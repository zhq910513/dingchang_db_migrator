from __future__ import annotations
import importlib
import sys
import json
from .tables.base_table import preview_rows

TABLE_SRC = {
    "user": "user",
    "role": "role",
}

def main(argv):
    if not argv:
        raise SystemExit("Usage: python -m migrator.preview <table> [limit]")
    name = argv[0]
    limit = int(argv[1]) if len(argv) > 1 else 10
    src = TABLE_SRC.get(name)
    if not src:
        raise SystemExit(f"Unknown table {name}. Supported: {', '.join(TABLE_SRC.keys())}")
    rows = preview_rows(src, limit=limit)
    print(json.dumps(rows, ensure_ascii=False, indent=2, default=str))

if __name__ == "__main__":
    main(sys.argv[1:])
