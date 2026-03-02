from __future__ import annotations
from typing import Dict, Any
from ..config import load_config
from ..db import connect
from ..state import get_state, set_state, Watermark
from ..introspect import common_columns
from ..utils import build_incremental_where
from ..strict import exec_one_strict

BATCH_SIZE_DEFAULT = 2000

def preview_rows(src_table: str, limit: int = 10) -> list[dict]:
    cfg = load_config()
    with connect(cfg) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM `{src_table}` ORDER BY `id` ASC LIMIT {int(limit)}")
            return cur.fetchall()

def upsert_table_strict(src_table: str, tgt_table: str, mode: str, batch_size: int = BATCH_SIZE_DEFAULT) -> Dict[str, Any]:
    cols = common_columns(src_table, tgt_table)
    if not cols:
        raise RuntimeError(f"No common columns between {src_table} and {tgt_table}")

    wm = get_state(tgt_table)
    wm.mode = mode
    where_sql, params, order_sql = build_incremental_where(cols, wm)

    select_sql = f"SELECT {', '.join('`'+c+'`' for c in cols)} FROM `{src_table}` {where_sql} {order_sql} LIMIT {int(batch_size)}"
    insert_cols_sql = ", ".join('`'+c+'`' for c in cols)
    values_sql = ", ".join(["%("+c+")s" for c in cols])
    update_cols = [c for c in cols if c != "id"]
    update_sql = ", ".join([f"`{c}`=VALUES(`{c}`)" for c in update_cols]) if update_cols else "`id`=`id`"
    upsert_sql = f"INSERT INTO `{tgt_table}` ({insert_cols_sql}) VALUES ({values_sql}) ON DUPLICATE KEY UPDATE {update_sql}"

    total = 0
    max_ts = wm.last_ts
    max_id = wm.last_id

    cfg = load_config()
    with connect(cfg) as conn:
        with conn.cursor() as cur:
            while True:
                cur.execute(select_sql, params)
                rows = cur.fetchall()
                if not rows:
                    break
                for r in rows:
                    exec_one_strict(cur, upsert_sql, r, row_for_log=r, table_hint=tgt_table)
                    total += 1
                    if wm.mode in ("updated_at","created_at") and wm.mode in r and r[wm.mode] is not None:
                        max_ts = r[wm.mode].strftime("%Y-%m-%d %H:%M:%S")
                        max_id = int(r.get("id") or 0)
                    elif "id" in r and r["id"] is not None:
                        max_id = int(r["id"])
                    wm.last_ts = max_ts
                    wm.last_id = max_id

                where_sql, params, order_sql = build_incremental_where(cols, wm)
                select_sql = f"SELECT {', '.join('`'+c+'`' for c in cols)} FROM `{src_table}` {where_sql} {order_sql} LIMIT {int(batch_size)}"

        conn.commit()

    if total:
        set_state(Watermark(table_name=tgt_table, mode=wm.mode, last_ts=max_ts, last_id=max_id))
    return {"src": src_table, "tgt": tgt_table, "mode": wm.mode, "rows": total, "last_ts": max_ts, "last_id": max_id}
