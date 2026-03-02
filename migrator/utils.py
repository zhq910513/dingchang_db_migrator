from __future__ import annotations
from typing import List, Tuple
from .state import Watermark

def build_incremental_where(cols: List[str], wm: Watermark):
    mode = wm.mode
    if mode in ("updated_at", "created_at") and mode in cols:
        if wm.last_ts is None:
            return "", (), f"ORDER BY `{mode}` ASC, `id` ASC"
        return f"WHERE (`{mode}` > %s) OR (`{mode}` = %s AND `id` > %s)", (wm.last_ts, wm.last_ts, wm.last_id), f"ORDER BY `{mode}` ASC, `id` ASC"
    if "id" not in cols:
        return "", (), ""
    return "WHERE `id` > %s", (wm.last_id,), "ORDER BY `id` ASC"
