from __future__ import annotations
import os, json, sys
import pymysql

ORDER_FIELDS = ["vin","plate_no","owner_name","engine_no","vehicle_model","first_register_date","id_number"]

def connect():
    return pymysql.connect(
        host=os.getenv("MYSQL_HOST","dingchang_mysql"),
        port=int(os.getenv("MYSQL_PORT","3306")),
        user=os.getenv("MYSQL_USER","dingchang_app"),
        password=os.getenv("MYSQL_PASSWORD",""),
        database=os.getenv("MYSQL_DB","order_system"),
        charset=os.getenv("MYSQL_CHARSET","utf8mb4"),
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
    )

def parse_json(v):
    if v is None: return {}
    if isinstance(v, dict): return v
    if isinstance(v, (bytes, bytearray)): v=v.decode("utf-8","replace")
    s=str(v).strip()
    if not s: return {}
    return json.loads(s)

def as_ymd(d):
    if d is None: return None
    # pymysql date -> datetime.date
    return str(d)

def main():
    sample = int(os.getenv("SAMPLE_LIMIT","0") or "0")

    with connect() as conn:
        with conn.cursor() as cur:
            # 1) order_new.dynamic_data 固定字段集检查
            sql = "SELECT id, dynamic_data FROM order_new ORDER BY id ASC"
            if sample>0: sql += f" LIMIT {sample}"
            cur.execute(sql)
            orders = cur.fetchall()

            allowed=set(ORDER_FIELDS)

            for r in orders:
                oid=int(r["id"])
                d=parse_json(r["dynamic_data"])
                keys=set(d.keys())
                extra=keys-allowed
                missing=allowed-keys
                if extra:
                    print("FAIL order_new.extra_keys", oid, sorted(extra)); sys.exit(2)
                if missing:
                    print("FAIL order_new.missing_keys", oid, sorted(missing)); sys.exit(3)

            # 2) order_fact_new 覆盖率（必须每个 order_new 都有一行）
            cur.execute("SELECT order_id FROM order_fact_new")
            fact_ids=set(int(x["order_id"]) for x in cur.fetchall())
            missing_fact=[int(r["id"]) for r in orders if int(r["id"]) not in fact_ids]
            if missing_fact:
                print("FAIL missing order_fact_new rows. examples:", missing_fact[:20]); sys.exit(4)

            # 3) order_slot_result_new 唯一性（DB 已有 UNIQUE，但这里做口径核验：每单每槽最多1条）
            cur.execute("SELECT order_id, slot_key, COUNT(*) AS c FROM order_slot_result_new GROUP BY order_id, slot_key HAVING c>1 LIMIT 5")
            dup = cur.fetchall()
            if dup:
                print("FAIL slot_result duplicates:", dup); sys.exit(5)

            # 4) 三方一致性：order_new.dynamic_data vs order_fact_new（同一订单字段一致）
            cur.execute("SELECT order_id, vin, plate_no, owner_name, engine_no, vehicle_model, first_register_date, id_number FROM order_fact_new")
            fact_map={int(x["order_id"]): x for x in cur.fetchall()}

            for r in orders:
                oid=int(r["id"])
                dd=parse_json(r["dynamic_data"])
                f=fact_map.get(oid)
                if not f:
                    print("FAIL missing fact row", oid); sys.exit(6)

                # 对齐 first_register_date：dynamic_data 是 'YYYY-MM-DD' or null；fact 是 DATE or null
                f_frd = as_ymd(f.get("first_register_date"))
                d_frd = dd.get("first_register_date")
                if d_frd is not None and str(d_frd).strip()=="":
                    d_frd = None

                pairs = [
                    ("vin", dd.get("vin"), f.get("vin")),
                    ("plate_no", dd.get("plate_no"), f.get("plate_no")),
                    ("owner_name", dd.get("owner_name"), f.get("owner_name")),
                    ("engine_no", dd.get("engine_no"), f.get("engine_no")),
                    ("vehicle_model", dd.get("vehicle_model"), f.get("vehicle_model")),
                    ("id_number", dd.get("id_number"), f.get("id_number")),
                    ("first_register_date", d_frd, f_frd),
                ]
                for k, a, b in pairs:
                    a = None if a in ("", []) else a
                    b = None if b in ("", []) else b
                    if a != b:
                        print("FAIL mismatch", {"order_id": oid, "field": k, "dynamic_data": a, "order_fact": b})
                        sys.exit(7)

    print("OK: order_new.dynamic_data + order_slot_result_new + order_fact_new all consistent")

if __name__=="__main__":
    main()