from __future__ import annotations
from .base_table import upsert_table_strict
SRC_TABLE = "customer_group"
TGT_TABLE = "customer_group_new"
def run(mode: str = "updated_at", batch_size: int = 2000):
    return upsert_table_strict(SRC_TABLE, TGT_TABLE, mode=mode, batch_size=batch_size)
