from __future__ import annotations

import json
import os
import re
import sys

import pymysql

ORDER_FIELDS = ["vin", "plate_no", "owner_name", "engine_no", "vehicle_model", "first_register_date", "id_number"]
DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def connect():
    return pymysql.connect(
        host=os.getenv("MYSQL_HOST", "dingchang_mysql"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.getenv("MYSQL_USER", "dingchang_app"),
        password=os.getenv("MYSQL_PASSWORD", ""),
        database=os.getenv("MYSQL_DB", "order_system"),
        charset=os.getenv("MYSQL_CHARSET", "utf8mb4"),
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
    )


def parse(v):
    if v is None: return {}
    if isinstance(v, dict): return v
    if isinstance(v, (bytes, bytearray)): v = v.decode("utf-8", "replace")
    s = str(v).strip()
    if not s: return {}
    return json.loads(s)


def main():
    sample = int(os.getenv("SAMPLE_LIMIT", "0") or "0")
    with connect() as conn:
        with conn.cursor() as cur:
            sql = "SELECT id, dynamic_data FROM order_new ORDER BY id ASC"
            if sample > 0: sql += f" LIMIT {sample}"
            cur.execute(sql)
            rows = cur.fetchall()

    allowed = set(ORDER_FIELDS)

    for r in rows:
        oid = int(r["id"])
        d = parse(r["dynamic_data"])
        keys = set(d.keys())

        extra = keys - allowed
        missing = allowed - keys

        if extra:
            print("FAIL extra keys", oid, sorted(extra));
            sys.exit(2)
        if missing:
            print("FAIL missing keys", oid, sorted(missing));
            sys.exit(3)

        frd = d.get("first_register_date")
        if frd not in (None, "") and not DATE.match(str(frd)):
            print("FAIL date", oid, frd);
            sys.exit(4)

    print("OK order_new.dynamic_data conforms to ORDER_FIELDS")


if __name__ == "__main__":
    main()
