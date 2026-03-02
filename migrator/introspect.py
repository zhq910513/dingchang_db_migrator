from __future__ import annotations

from typing import List, Set, Tuple

from .config import load_config
from .db import connect


def _get_columns_and_generated_flags(table_name: str) -> List[Tuple[str, bool]]:
    """
    返回 (column_name, is_generated) 列表，按 ORDINAL_POSITION 排序
    MySQL: EXTRA 包含 'VIRTUAL GENERATED' / 'STORED GENERATED' 等
    """
    cfg = load_config()
    with connect(cfg) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COLUMN_NAME, EXTRA "
                "FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s "
                "ORDER BY ORDINAL_POSITION",
                (cfg.db, table_name),
            )
            rows = cur.fetchall()
    out: List[Tuple[str, bool]] = []
    for r in rows:
        extra = (r.get("EXTRA") or "").upper()
        out.append((r["COLUMN_NAME"], "GENERATED" in extra))
    return out


def get_columns(table_name: str) -> List[str]:
    return [c for c, _ in _get_columns_and_generated_flags(table_name)]


def get_writable_columns(table_name: str) -> List[str]:
    """可写列：排除 generated columns"""
    return [c for c, is_gen in _get_columns_and_generated_flags(table_name) if not is_gen]


def common_columns(src: str, tgt: str) -> List[str]:
    """
    取 src 的列顺序为准，但只保留：
    - tgt 存在
    - 且 tgt 为可写列（非 generated）
    """
    src_cols = get_columns(src)
    tgt_writable: Set[str] = set(get_writable_columns(tgt))
    return [c for c in src_cols if c in tgt_writable]
