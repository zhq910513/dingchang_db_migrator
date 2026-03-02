from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Literal
from .db import connect
from .config import load_config

Mode = Literal["updated_at", "created_at", "id"]

@dataclass
class Watermark:
    table_name: str
    mode: Mode = "updated_at"
    last_ts: Optional[str] = None
    last_id: int = 0

def get_state(table_name: str) -> Watermark:
    cfg = load_config()
    with connect(cfg) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT table_name, mode, last_ts, last_id FROM migration_state WHERE table_name=%s", (table_name,))
            row = cur.fetchone()
    if not row:
        return Watermark(table_name=table_name)
    return Watermark(
        table_name=row["table_name"],
        mode=row["mode"],
        last_ts=row["last_ts"].strftime("%Y-%m-%d %H:%M:%S") if row["last_ts"] else None,
        last_id=int(row["last_id"] or 0),
    )

def set_state(wm: Watermark) -> None:
    cfg = load_config()
    with connect(cfg) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO migration_state(table_name, mode, last_ts, last_id) VALUES(%s,%s,%s,%s) "
                "ON DUPLICATE KEY UPDATE mode=VALUES(mode), last_ts=VALUES(last_ts), last_id=VALUES(last_id)",
                (wm.table_name, wm.mode, wm.last_ts, wm.last_id),
            )
        conn.commit()

def reset_state(table_name: str) -> None:
    cfg = load_config()
    with connect(cfg) as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM migration_state WHERE table_name=%s", (table_name,))
        conn.commit()
