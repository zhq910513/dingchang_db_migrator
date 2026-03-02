from __future__ import annotations
from typing import List
from .db import connect
from .config import load_config

def get_columns(table_name: str) -> List[str]:
    cfg = load_config()
    with connect(cfg) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COLUMN_NAME FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s ORDER BY ORDINAL_POSITION",
                (cfg.db, table_name),
            )
            return [r["COLUMN_NAME"] for r in cur.fetchall()]

def common_columns(src: str, tgt: str) -> List[str]:
    s = get_columns(src)
    t = set(get_columns(tgt))
    return [c for c in s if c in t]
