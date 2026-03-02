from __future__ import annotations

import json
import re
from typing import Any, Dict, Optional, Tuple

# 依赖后端规则文件：迁移容器需能 import app.core.slot_fact_config
from app.core.slot_fact_config import ORDER_FIELDS, COMPOSE_RULES, SLOT_FIELDS  # type: ignore

from ..config import load_config
from ..db import connect
from ..introspect import common_columns
from ..state import get_state, set_state, Watermark
from ..strict import exec_one_strict
from ..utils import build_incremental_where

SRC_TABLE = "order"
TGT_TABLE = "order_new"
TGT_SLOT_TABLE = "order_slot_result_new"
TGT_FACT_TABLE = "order_fact_new"

BATCH_SIZE_DEFAULT = 200  # JSON 大，保守

_YYYYMMDD = re.compile(r"^\d{8}$")
_YYYY_MM_DD = re.compile(r"^\d{4}-\d{2}-\d{2}$")


# ========= utils =========
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


def _json_dumps(obj: Any) -> str:
    # 让 MySQL JSON 列吃到稳定 JSON 文本
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


# ========= slot extractors (基于你现有样本结构) =========
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


def _extract_idcard_back(raw: Dict[str, Any]) -> Dict[str, Any]:
    wr = (raw or {}).get("words_result") or {}
    # 你样本里背面字段：签发机关/签发日期/失效日期（以及“长期”）
    issuer = _g_words(wr, "签发机关")
    valid_from = _norm_date(_g_words(wr, "签发日期"))
    valid_to_raw = _g_words(wr, "失效日期")
    valid_to = _norm_date(valid_to_raw) if valid_to_raw not in ("长期", "永久") else None
    validity = str(valid_to_raw).strip() if valid_to_raw is not None else None
    return {
        "id_issuer": issuer,
        "id_valid_from": valid_from,
        "id_valid_to": valid_to,
        "id_validity": validity,
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


def _slot_meta(slot_key: str) -> Tuple[str, str]:
    """
    返回 (api_type, side)
    必填：order_slot_result_new.api_type NOT NULL, side 有默认但我们也填更清晰
    """
    if slot_key == "vehicle_cert":
        return ("vehicle_certificate", "none")
    if slot_key == "idcard_front":
        return ("idcard", "front")
    if slot_key == "idcard_back":
        return ("idcard", "back")
    if slot_key == "driving_license_main":
        return ("vehicle_license", "front")
    if slot_key == "driving_license_sub":
        return ("vehicle_license", "back")
    return ("unknown", "none")


def _slot_recognized_and_raw(ocr_raw_json: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """
    返回：slot_key -> {
      raw_json: dict,
      recognized: dict
    }
    仅对你规则里定义的槽位产出 recognized_json（并白名单过滤）
    """
    out: Dict[str, Dict[str, Any]] = {}

    def put(slot_key: str, extractor):
        raw_obj = _to_dict(ocr_raw_json.get(slot_key))
        rec = extractor(raw_obj)
        allow = set(SLOT_FIELDS.get(slot_key, []))
        rec2 = {k: rec.get(k) for k in allow}
        out[slot_key] = {"raw_json": raw_obj, "recognized": rec2}

    if isinstance(ocr_raw_json, dict):
        if "vehicle_cert" in ocr_raw_json:
            put("vehicle_cert", _extract_vehicle_cert)
        if "idcard_front" in ocr_raw_json:
            put("idcard_front", _extract_idcard_front)
        if "idcard_back" in ocr_raw_json:
            put("idcard_back", _extract_idcard_back)
        if "driving_license_main" in ocr_raw_json:
            put("driving_license_main", _extract_driving_license_main)

    return out


# ========= compose =========
def _compose_order_fact(rec_by_slot: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    # 固定字段集必须完整存在（值可 None）
    fact: Dict[str, Any] = {k: None for k in ORDER_FIELDS}

    for out_key, rules in COMPOSE_RULES.items():
        for r in rules:
            src = (rec_by_slot.get(r.from_slot) or {}).get(r.from_key)
            if getattr(r, "transform", None) == "ymd":
                src = _norm_date(src)

            if getattr(r, "merge_mode", "fill_if_empty") == "always_override":
                fact[out_key] = src
                break

            if fact.get(out_key) in (None, "") and src not in (None, ""):
                fact[out_key] = src
                break

    return fact


# ========= upserts =========
def _upsert_order_new(cur, row: Dict[str, Any], cols: list[str]) -> None:
    insert_cols_sql = ", ".join(f"`{c}`" for c in cols)
    values_sql = ", ".join(f"%({c})s" for c in cols)
    update_cols = [c for c in cols if c != "id"]
    update_sql = ", ".join([f"`{c}`=VALUES(`{c}`)" for c in update_cols]) if update_cols else "`id`=`id`"
    sql = f"INSERT INTO `{TGT_TABLE}` ({insert_cols_sql}) VALUES ({values_sql}) ON DUPLICATE KEY UPDATE {update_sql}"
    exec_one_strict(cur, sql, row, row_for_log=row, table_hint=TGT_TABLE)


def _upsert_slot_result(cur, order_id: int, slot_key: str, raw_json_obj: Dict[str, Any],
                        recognized_obj: Dict[str, Any]) -> None:
    api_type, side = _slot_meta(slot_key)
    params = {
        "order_id": order_id,
        "slot_key": slot_key,
        "provider": "baidu",
        "api_type": api_type,
        "side": side,
        "status": "ok",
        "error_message": None,
        "raw_json": _json_dumps(raw_json_obj),
        "recognized_json": _json_dumps(recognized_obj),
    }

    sql = (
        "INSERT INTO `order_slot_result_new` "
        "(`order_id`,`slot_key`,`provider`,`api_type`,`side`,`status`,`error_message`,`raw_json`,`recognized_json`) "
        "VALUES (%(order_id)s,%(slot_key)s,%(provider)s,%(api_type)s,%(side)s,%(status)s,%(error_message)s,%(raw_json)s,%(recognized_json)s) "
        "ON DUPLICATE KEY UPDATE "
        "`provider`=VALUES(`provider`),"
        "`api_type`=VALUES(`api_type`),"
        "`side`=VALUES(`side`),"
        "`status`=VALUES(`status`),"
        "`error_message`=VALUES(`error_message`),"
        "`raw_json`=VALUES(`raw_json`),"
        "`recognized_json`=VALUES(`recognized_json`),"
        "`updated_at`=CURRENT_TIMESTAMP"
    )
    exec_one_strict(cur, sql, params, row_for_log=params, table_hint=TGT_SLOT_TABLE)


def _upsert_order_fact(cur, order_id: int, fact: Dict[str, Any]) -> None:
    # order_fact_new.first_register_date 是 DATE：传 'YYYY-MM-DD' 字符串即可
    frd = fact.get("first_register_date")
    frd = _norm_date(frd)  # 确保写入 DATE 兼容格式

    params = {
        "order_id": order_id,
        "vin": fact.get("vin"),
        "plate_no": fact.get("plate_no"),
        "owner_name": fact.get("owner_name"),
        "engine_no": fact.get("engine_no"),
        "vehicle_model": fact.get("vehicle_model"),
        "first_register_date": frd,
        "id_number": fact.get("id_number"),
    }

    sql = (
        "INSERT INTO `order_fact_new` "
        "(`order_id`,`vin`,`plate_no`,`owner_name`,`engine_no`,`vehicle_model`,`first_register_date`,`id_number`) "
        "VALUES (%(order_id)s,%(vin)s,%(plate_no)s,%(owner_name)s,%(engine_no)s,%(vehicle_model)s,%(first_register_date)s,%(id_number)s) "
        "ON DUPLICATE KEY UPDATE "
        "`vin`=VALUES(`vin`),"
        "`plate_no`=VALUES(`plate_no`),"
        "`owner_name`=VALUES(`owner_name`),"
        "`engine_no`=VALUES(`engine_no`),"
        "`vehicle_model`=VALUES(`vehicle_model`),"
        "`first_register_date`=VALUES(`first_register_date`),"
        "`id_number`=VALUES(`id_number`),"
        "`updated_at`=CURRENT_TIMESTAMP"
    )
    exec_one_strict(cur, sql, params, row_for_log=params, table_hint=TGT_FACT_TABLE)


def run(mode: str = "updated_at", batch_size: int = BATCH_SIZE_DEFAULT):
    cols = common_columns(SRC_TABLE, TGT_TABLE)
    if "dynamic_data" not in cols or "ocr_raw_json" not in cols:
        raise RuntimeError("order migration expects dynamic_data and ocr_raw_json columns")

    wm = get_state(TGT_TABLE)
    wm.mode = mode

    where_sql, params, order_sql = build_incremental_where(cols, wm)
    select_sql = f"SELECT {', '.join('`' + c + '`' for c in cols)} FROM `{SRC_TABLE}` {where_sql} {order_sql} LIMIT {int(batch_size)}"

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
                        slot_pack = _slot_recognized_and_raw(ocr_raw)

                        # rec_by_slot: slot -> recognized dict（用于 compose）
                        rec_by_slot = {k: v["recognized"] for k, v in slot_pack.items()}
                        fact = _compose_order_fact(rec_by_slot)

                        # 1) order_new.dynamic_data 固定字段集
                        r["dynamic_data"] = _json_dumps({k: fact.get(k) for k in ORDER_FIELDS})

                    except Exception as e:
                        print("=== ORDER FACT BUILD ERROR (STOP) ===")
                        print("order_id:", oid)
                        print("error:", repr(e))
                        print("row_json:", json.dumps(r, ensure_ascii=False, default=str)[:4000])
                        raise

                    # 2) 写 order_new（必须先写，FK 约束依赖）
                    _upsert_order_new(cur, r, cols)

                    # 3) 写事实层：order_slot_result_new（只对存在的槽位）
                    for slot_key, pack in slot_pack.items():
                        _upsert_slot_result(cur, oid, slot_key, pack["raw_json"], pack["recognized"])

                    # 4) 写投影层：order_fact_new
                    _upsert_order_fact(cur, oid, fact)

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
