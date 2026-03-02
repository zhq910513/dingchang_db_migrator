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
    last_ts: Optional[str] = None  # 'YYYY-MM-DD HH:MM:SS'
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

def show_state() -> list[dict]:
    cfg = load_config()
    with connect(cfg) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT table_name, mode, last_ts, last_id, updated_at FROM migration_state ORDER BY table_name")
            rows = cur.fetchall()
    out = []
    for r in rows:
        out.append({
            "table_name": r["table_name"],
            "mode": r["mode"],
            "last_ts": r["last_ts"].strftime("%Y-%m-%d %H:%M:%S") if r["last_ts"] else None,
            "last_id": int(r["last_id"] or 0),
            "updated_at": r["updated_at"].strftime("%Y-%m-%d %H:%M:%S") if r["updated_at"] else None,
        })
    return out

if __name__ == "__main__":
    import sys, json
    cmd = (sys.argv[1] if len(sys.argv) > 1 else "show").lower()
    if cmd == "show":
        print(json.dumps(show_state(), ensure_ascii=False, indent=2))
    elif cmd == "reset":
        if len(sys.argv) < 3:
            raise SystemExit("Usage: python -m migrator.state reset <table_name>")
        reset_state(sys.argv[2])
        print("OK")
    else:
        raise SystemExit("Usage: python -m migrator.state [show|reset <table_name>]")
