from __future__ import annotations

import json
import re
from typing import Any, Dict, Optional

# 依赖后端规则：你需要保证迁移容器能 import 到 app.core.slot_fact_config
from app.core.slot_fact_config import ORDER_FIELDS, COMPOSE_RULES, SLOT_FIELDS  # type: ignore

from ..config import load_config
from ..db import connect
from ..introspect import common_columns
from ..state import get_state, set_state, Watermark
from ..strict import exec_one_strict
from ..utils import build_incremental_where

SRC_TABLE = "order"
TGT_TABLE = "order_new"
BATCH_SIZE_DEFAULT = 200  # JSON 大，保守

_YYYYMMDD = re.compile(r"^\d{8}$")
_YYYY_MM_DD = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _to_dict(v: Any) -> Dict[str, Any]:
    if v is None:
        return {}
    if isinstance(v, dict):
        return v
    if isinstance(v, (bytes, bytearray)):
        v = v.decode("utf-8", errors="replace")
    s = str(v).strip()
    if not s:
        return {}
    obj = json.loads(s)
    return obj if isinstance(obj, dict) else {}


def _g_words(wr: Dict[str, Any], key: str) -> Optional[str]:
    v = wr.get(key)
    if isinstance(v, dict):
        return v.get("words")
    return None


def _norm_date(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    if _YYYY_MM_DD.match(s):
        return s
    if _YYYYMMDD.match(s):
        return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
    return None


def _extract_vehicle_cert(raw: Dict[str, Any]) -> Dict[str, Any]:
    wr = (raw or {}).get("words_result") or {}
    return {
        "vin": wr.get("VinNo"),
        "engine_no": wr.get("EngineNo"),
        "vehicle_model": wr.get("CarModel"),
        "approved_passenger_count": wr.get("SeatingCapacity"),
        "vehicle_brand_name": wr.get("CarBrand"),
        "manufacturer_name": wr.get("Manufacturer"),
    }


def _extract_idcard_front(raw: Dict[str, Any]) -> Dict[str, Any]:
    wr = (raw or {}).get("words_result") or {}
    return {
        "id_name": _g_words(wr, "姓名"),
        "id_number": _g_words(wr, "公民身份号码"),
        "id_address": _g_words(wr, "住址"),
        "id_birth_date": _g_words(wr, "出生"),
        "id_gender": _g_words(wr, "性别"),
        "id_ethnicity": _g_words(wr, "民族"),
    }


def _extract_driving_license_main(raw: Dict[str, Any]) -> Dict[str, Any]:
    wr = (raw or {}).get("words_result") or {}
    return {
        "plate_no": _g_words(wr, "号牌号码"),
        "owner_name": _g_words(wr, "所有人"),
        "vin": _g_words(wr, "车辆识别代号"),
        "engine_no": _g_words(wr, "发动机号码"),
        "vehicle_model": _g_words(wr, "品牌型号"),
        "vehicle_type": _g_words(wr, "车辆类型"),
        "use_nature": _g_words(wr, "使用性质"),
        "first_register_date": _norm_date(_g_words(wr, "注册日期")),
        "issue_date": _norm_date(_g_words(wr, "发证日期")),
        "issuer_org": _g_words(wr, "发证单位"),
    }


def _slot_recognized(ocr_raw_json: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}

    # 目前先覆盖你最关键的 3 个槽位（可按 SLOT_FIELDS 扩展）
    if "vehicle_cert" in ocr_raw_json:
        out["vehicle_cert"] = _extract_vehicle_cert(_to_dict(ocr_raw_json.get("vehicle_cert")))
    if "idcard_front" in ocr_raw_json:
        out["idcard_front"] = _extract_idcard_front(_to_dict(ocr_raw_json.get("idcard_front")))
    if "driving_license_main" in ocr_raw_json:
        out["driving_license_main"] = _extract_driving_license_main(_to_dict(ocr_raw_json.get("driving_license_main")))

    # 白名单过滤到 SLOT_FIELDS
    cleaned: Dict[str, Dict[str, Any]] = {}
    for slot, d in out.items():
        allow = set(SLOT_FIELDS.get(slot, []))
        cleaned[slot] = {k: d.get(k) for k in allow}
    return cleaned


def _compose(rec_by_slot: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    # 固定字段集必须完整存在（值可 None）
    fact = {k: None for k in ORDER_FIELDS}

    for out_key, rules in COMPOSE_RULES.items():
        for r in rules:
            src = (rec_by_slot.get(r.from_slot) or {}).get(r.from_key)
            if getattr(r, "transform", None) == "ymd":
                src = _norm_date(src)

            if getattr(r, "merge_mode", "fill_if_empty") == "always_override":
                fact[out_key] = src
                break

            # fill_if_empty
            if fact.get(out_key) in (None, "") and src not in (None, ""):
                fact[out_key] = src
                break

    return fact


def run(mode: str = "updated_at", batch_size: int = BATCH_SIZE_DEFAULT):
    cols = common_columns(SRC_TABLE, TGT_TABLE)
    if "dynamic_data" not in cols or "ocr_raw_json" not in cols:
        raise RuntimeError("order migration expects dynamic_data and ocr_raw_json columns")

    wm = get_state(TGT_TABLE)
    wm.mode = mode

    where_sql, params, order_sql = build_incremental_where(cols, wm)
    select_sql = f"SELECT {', '.join('`' + c + '`' for c in cols)} FROM `{SRC_TABLE}` {where_sql} {order_sql} LIMIT {int(batch_size)}"

    insert_cols_sql = ", ".join('`' + c + '`' for c in cols)
    values_sql = ", ".join([f"%({c})s" for c in cols])
    update_cols = [c for c in cols if c != "id"]
    update_sql = ", ".join([f"`{c}`=VALUES(`{c}`)" for c in update_cols]) if update_cols else "`id`=`id`"
    upsert_sql = f"INSERT INTO `{TGT_TABLE}` ({insert_cols_sql}) VALUES ({values_sql}) ON DUPLICATE KEY UPDATE {update_sql}"

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
                    oid = int(r.get("id") or 0)
                    try:
                        ocr_raw = _to_dict(r.get("ocr_raw_json"))
                        rec = _slot_recognized(ocr_raw)
                        fact = _compose(rec)

                        # 写入 order_new.dynamic_data：只保留固定字段集
                        r["dynamic_data"] = json.dumps({k: fact.get(k) for k in ORDER_FIELDS}, ensure_ascii=False)

                    except Exception as e:
                        print("=== ORDER CLEAN ERROR (STOP) ===")
                        print("order_id:", oid)
                        print("error:", repr(e))
                        print("row_json:", json.dumps(r, ensure_ascii=False, default=str)[:4000])
                        raise

                    exec_one_strict(cur, upsert_sql, r, row_for_log=r, table_hint=TGT_TABLE)
                    total += 1

                    if wm.mode in ("updated_at", "created_at") and wm.mode in r and r[wm.mode] is not None:
                        max_ts = r[wm.mode].strftime("%Y-%m-%d %H:%M:%S")
                        max_id = oid
                    else:
                        max_id = oid
                    wm.last_ts = max_ts
                    wm.last_id = max_id

                where_sql, params, order_sql = build_incremental_where(cols, wm)
                select_sql = f"SELECT {', '.join('`' + c + '`' for c in cols)} FROM `{SRC_TABLE}` {where_sql} {order_sql} LIMIT {int(batch_size)}"

        conn.commit()

    if total:
        set_state(Watermark(table_name=TGT_TABLE, mode=wm.mode, last_ts=max_ts, last_id=max_id))

    return {"src": SRC_TABLE, "tgt": TGT_TABLE, "mode": wm.mode, "rows": total, "last_ts": max_ts, "last_id": max_id}
