from __future__ import annotations
import json
from typing import Dict, Any
from pymysql.err import MySQLError

def dump_row(row: Dict[str, Any]) -> str:
    def _default(o):
        try:
            return str(o)
        except Exception:
            return "<unserializable>"
    return json.dumps(row, ensure_ascii=False, default=_default)

def exec_one_strict(cur, sql: str, params: Dict[str, Any], *, row_for_log: Dict[str, Any], table_hint: str):
    try:
        cur.execute(sql, params)
    except MySQLError as e:
        print("=== MIGRATION ERROR (STOP) ===")
        print("table:", table_hint)
        print("sql:", sql)
        print("error:", repr(e))
        print("row_json:", dump_row(row_for_log))
        raise
