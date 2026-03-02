# selfcheck_fact.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import os
import re
import sys
from typing import Any, Dict, List, Tuple

import pymysql

# ====== 1) 引入你的最新规则文件（按实际路径改）======
# 如果你放在后端项目里：from app.core.slot_fact_config import ...
# 这里先假设你把该文件也同步到了当前目录或 PYTHONPATH 可见
from app.core.slot_fact_config import (  # type: ignore
    SLOT_FIELDS,
    ORDER_FIELDS,
    COMPOSE_RULES,
    SourceRule,
)

DB_HOST = os.getenv("MYSQL_HOST", "dingchang_mysql")
DB_PORT = int(os.getenv("MYSQL_PORT", "3306"))
DB_USER = os.getenv("MYSQL_USER", "dingchang_app")
DB_PASS = os.getenv("MYSQL_PASSWORD", "")
DB_NAME = os.getenv("MYSQL_DB", "order_system")
DB_CHARSET = os.getenv("MYSQL_CHARSET", "utf8mb4")

DATE_YMD = re.compile(r"^\d{4}-\d{2}-\d{2}$")

FORBIDDEN_PREFIXES = ("dl_",)  # 你后续还可以扩展


def die(msg: str) -> None:
    print("FATAL:", msg)
    sys.exit(2)


def warn(msg: str) -> None:
    print("WARN:", msg)


def ok(msg: str) -> None:
    print("OK:", msg)


def _connect():
    return pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        charset=DB_CHARSET,
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
    )


# ====== 2) 配置一致性自检 ======
def check_config_consistency() -> None:
    # 2.1 ORDER_FIELDS 去重
    if len(set(ORDER_FIELDS)) != len(ORDER_FIELDS):
        die(f"ORDER_FIELDS has duplicates: {ORDER_FIELDS}")

    # 2.2 COMPOSE_RULES 的 key 必须在 ORDER_FIELDS
    for k in COMPOSE_RULES.keys():
        if k not in ORDER_FIELDS:
            die(f"COMPOSE_RULES key not in ORDER_FIELDS: {k}")

    # 2.3 每条 SourceRule 的 slot/key 必须存在于 SLOT_FIELDS
    for out_key, rules in COMPOSE_RULES.items():
        if not isinstance(rules, list) or not rules:
            die(f"COMPOSE_RULES[{out_key}] empty")
        for r in rules:
            if r.from_slot not in SLOT_FIELDS:
                die(f"Unknown slot in COMPOSE_RULES[{out_key}]: {r.from_slot}")
            if r.from_key not in SLOT_FIELDS[r.from_slot]:
                die(
                    f"Key '{r.from_key}' not allowed by SLOT_FIELDS[{r.from_slot}] "
                    f"for COMPOSE_RULES[{out_key}]"
                )
            if r.transform not in (None, "ymd"):
                die(f"Unsupported transform in COMPOSE_RULES[{out_key}]: {r.transform}")

    ok("config consistency check passed")


# ====== 3) DB 数据自检：order_new.dynamic_data 是否符合 ORDER_FIELDS ======
def parse_json(val: Any, *, ctx: str) -> Dict[str, Any]:
    if val is None:
        return {}
    if isinstance(val, dict):
        return val
    if isinstance(val, (bytes, bytearray)):
        val = val.decode("utf-8", errors="replace")
    if isinstance(val, str):
        s = val.strip()
        if not s:
            return {}
        try:
            obj = json.loads(s)
            if isinstance(obj, dict):
                return obj
            return {}
        except Exception as e:
            die(f"{ctx}: invalid JSON: {e} (head={s[:200]})")
    # 兜底
    return {}


def check_order_new_dynamic_data(sample_limit: int = 0) -> None:
    bad_forbidden: List[Tuple[int, List[str]]] = []
    bad_extra: List[Tuple[int, List[str]]] = []
    bad_missing: List[Tuple[int, List[str]]] = []
    bad_date: List[Tuple[int, Any]] = []
    bad_types: List[Tuple[int, str, Any]] = []

    with _connect() as conn:
        with conn.cursor() as cur:
            sql = "SELECT id, dynamic_data FROM order_new ORDER BY id ASC"
            if sample_limit and sample_limit > 0:
                sql += f" LIMIT {int(sample_limit)}"
            cur.execute(sql)
            rows = cur.fetchall()

    allowed = set(ORDER_FIELDS)

    for row in rows:
        oid = int(row["id"])
        dyn = parse_json(row.get("dynamic_data"), ctx=f"order_new.id={oid}.dynamic_data")

        keys = set(dyn.keys())

        # 3.1 禁止 dl_*
        forbidden = [k for k in keys if k.startswith(FORBIDDEN_PREFIXES)]
        if forbidden:
            bad_forbidden.append((oid, sorted(forbidden)))

        # 3.2 不允许额外键
        extra = sorted(list(keys - allowed))
        if extra:
            bad_extra.append((oid, extra))

        # 3.3 必须包含固定字段集（允许值为 null，但键必须存在）
        missing = sorted(list(allowed - keys))
        if missing:
            bad_missing.append((oid, missing))

        # 3.4 first_register_date 必须是 null 或 YYYY-MM-DD
        frd = dyn.get("first_register_date")
        if frd is not None:
            s = str(frd).strip()
            if s and not DATE_YMD.match(s):
                bad_date.append((oid, frd))

        # 3.5 基本类型校验（你可按需加严）
        for k in ORDER_FIELDS:
            v = dyn.get(k, None)
            if v is None:
                continue
            # vin/plate_no/owner_name/engine_no/vehicle_model/id_number 期望字符串
            if k != "first_register_date" and not isinstance(v, str):
                bad_types.append((oid, k, v))

    # 汇总输出（严格：任何一类错误都算失败）
    if bad_forbidden:
        print("FAIL: forbidden keys found (dl_* etc). examples:", bad_forbidden[:10])
        sys.exit(3)
    if bad_extra:
        print("FAIL: extra keys not in ORDER_FIELDS. examples:", bad_extra[:10])
        sys.exit(4)
    if bad_missing:
        print("FAIL: missing fixed keys (should exist with null). examples:", bad_missing[:10])
        sys.exit(5)
    if bad_date:
        print("FAIL: invalid first_register_date format. examples:", bad_date[:10])
        sys.exit(6)
    if bad_types:
        print("FAIL: invalid value types. examples:", bad_types[:10])
        sys.exit(7)

    ok("order_new.dynamic_data conforms to ORDER_FIELDS (no dl_*, no extra keys, fixed keys present, dates OK)")


if __name__ == "__main__":
    # 先查配置，再查数据
    check_config_consistency()

    # sample_limit=0 表示全表；你也可以先用 50 做快速试跑
    sample = int(os.getenv("SAMPLE_LIMIT", "0") or "0")
    check_order_new_dynamic_data(sample_limit=sample)
