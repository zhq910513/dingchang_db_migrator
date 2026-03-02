from __future__ import annotations
import importlib, sys, json
from .state import reset_state

TABLE_MODULE_MAP = {'user': ('migrator.tables.user', 'user_new'), 'role': ('migrator.tables.role', 'role_new'), 'customer_group': ('migrator.tables.customer_group', 'customer_group_new'), 'channel_group': ('migrator.tables.channel_group', 'channel_group_new'), 'field_config': ('migrator.tables.field_config', 'field_config_new'), 'field_group': ('migrator.tables.field_group', 'field_group_new'), 'field_group_field': ('migrator.tables.field_group_field', 'field_group_field_new'), 'order': ('migrator.tables.order', 'order_new'), 'order_info': ('migrator.tables.order_info', 'order_info_new'), 'image_file': ('migrator.tables.image_file', 'image_file_new'), 'order_image': ('migrator.tables.order_image', 'order_image_new'), 'finance_record': ('migrator.tables.finance_record', 'finance_record_new'), 'ocr_task': ('migrator.tables.ocr_task', 'ocr_task_new'), 'ocr_image_cache': ('migrator.tables.ocr_image_cache', 'ocr_image_cache_new'), 'image_ocr_result': ('migrator.tables.image_ocr_result', 'image_ocr_result_new'), 'user_role': ('migrator.tables.user_role', 'user_role_new'), 'user_session': ('migrator.tables.user_session', 'user_session_new')}

def run_one(name: str, mode: str):
    modpath, _ = TABLE_MODULE_MAP[name]
    mod = importlib.import_module(modpath)
    return mod.run(mode=mode)

def main(argv):
    if len(argv) < 2:
        raise SystemExit("Usage: python -m migrator.runner <full|inc> <table1> [table2 ...]")
    cmd = argv[0].lower()
    mode = "updated_at"
    tables = argv[1:]
    for t in tables:
        if t not in TABLE_MODULE_MAP:
            raise SystemExit(f"Unknown table {t}. Supported: {', '.join(TABLE_MODULE_MAP.keys())}")
    if cmd == "full":
        for t in tables:
            _, tgt = TABLE_MODULE_MAP[t]
            reset_state(tgt)
        out = [run_one(t, mode) for t in tables]
        print(json.dumps(out, ensure_ascii=False, indent=2))
        return
    if cmd == "inc":
        out = [run_one(t, mode) for t in tables]
        print(json.dumps(out, ensure_ascii=False, indent=2))
        return
    raise SystemExit("Usage: python -m migrator.runner <full|inc> <table1> [table2 ...]")
if __name__ == "__main__":
    main(sys.argv[1:])
