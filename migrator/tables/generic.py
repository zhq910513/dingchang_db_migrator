from __future__ import annotations
from .base_table import upsert_table_strict
def run_generic(src_table: str, tgt_table: str, mode: str = "updated_at", batch_size: int = 2000):
    return upsert_table_strict(src_table, tgt_table, mode=mode, batch_size=batch_size)
