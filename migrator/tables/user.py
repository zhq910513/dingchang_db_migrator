from __future__ import annotations
from typing import Dict, Any
from pymysql.err import MySQLError
from ..config import load_config
from ..db import connect
from ..introspect import common_columns
from ..state import get_state, set_state, Watermark
from ..utils import build_incremental_where
from ..strict import exec_one_strict

SRC_TABLE = "user"
TGT_TABLE = "user_new"
BATCH_SIZE_DEFAULT = 2000

def _upsert_parent_null(mode: str, batch_size: int) -> Dict[str, Any]:
    cols = common_columns(SRC_TABLE, TGT_TABLE)
    if "parent_id" not in cols:
        raise RuntimeError("Expected parent_id in user tables")

    select_cols = [("NULL AS `parent_id`" if c == "parent_id" else f"`{c}`") for c in cols]
    insert_cols_sql = ", ".join('`'+c+'`' for c in cols)
    values_sql = ", ".join(["%("+c+")s" for c in cols])
    update_cols = [c for c in cols if c != "id"]
    update_sql = ", ".join([f"`{c}`=VALUES(`{c}`)" for c in update_cols]) if update_cols else "`id`=`id`"
    upsert_sql = f"INSERT INTO `{TGT_TABLE}` ({insert_cols_sql}) VALUES ({values_sql}) ON DUPLICATE KEY UPDATE {update_sql}"

    wm = get_state(TGT_TABLE)
    wm.mode = mode
    where_sql, params, order_sql = build_incremental_where(cols, wm)

    total = 0
    max_ts = wm.last_ts
    max_id = wm.last_id

    cfg = load_config()
    with connect(cfg) as conn:
        with conn.cursor() as cur:
            while True:
                sql = f"SELECT {', '.join(select_cols)} FROM `{SRC_TABLE}` {where_sql} {order_sql} LIMIT {int(batch_size)}"
                cur.execute(sql, params)
                rows = cur.fetchall()
                if not rows:
                    break
                for r in rows:
                    exec_one_strict(cur, upsert_sql, r, row_for_log=r, table_hint=TGT_TABLE)
                    total += 1
                    if wm.mode in ("updated_at","created_at") and wm.mode in r and r[wm.mode] is not None:
                        max_ts = r[wm.mode].strftime("%Y-%m-%d %H:%M:%S")
                        max_id = int(r.get("id") or 0)
                    elif "id" in r and r["id"] is not None:
                        max_id = int(r["id"])
                    wm.last_ts = max_ts
                    wm.last_id = max_id
                where_sql, params, order_sql = build_incremental_where(cols, wm)

        conn.commit()

    if total:
        set_state(Watermark(table_name=TGT_TABLE, mode=wm.mode, last_ts=max_ts, last_id=max_id))
    return {"phase": "upsert_parent_null", "rows": total, "last_ts": max_ts, "last_id": max_id}

def _backfill_parent_id() -> Dict[str, Any]:
    cfg = load_config()
    with connect(cfg) as conn:
        with conn.cursor() as cur:
            # 逐行回填：先取需要回填的集合（小数据量更安全）；大数据量再升级成批处理
            cur.execute(f"SELECT s.id, s.parent_id FROM `{SRC_TABLE}` s WHERE s.parent_id IS NOT NULL ORDER BY s.id ASC")
            pairs = cur.fetchall()
            sql = f"UPDATE `{TGT_TABLE}` SET parent_id=%(parent_id)s WHERE id=%(id)s"
            cnt = 0
            for p in pairs:
                exec_one_strict(cur, sql, p, row_for_log=p, table_hint=TGT_TABLE+":parent_id")
                cnt += 1
        conn.commit()
    return {"phase": "backfill_parent_id", "rows": cnt}

def run(mode: str = "updated_at", batch_size: int = BATCH_SIZE_DEFAULT):
    r1 = _upsert_parent_null(mode, batch_size)
    r2 = _backfill_parent_id()
    return {"table": TGT_TABLE, "steps": [r1, r2]}
