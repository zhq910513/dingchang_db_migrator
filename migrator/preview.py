from __future__ import annotations
import sys, json
from .tables.base_table import preview_rows
TABLE_SRC = {'user': 'user', 'role': 'role', 'customer_group': 'customer_group', 'channel_group': 'channel_group', 'field_config': 'field_config', 'field_group': 'field_group', 'field_group_field': 'field_group_field', 'order': 'order', 'order_info': 'order_info', 'image_file': 'image_file', 'order_image': 'order_image', 'finance_record': 'finance_record', 'ocr_task': 'ocr_task', 'ocr_image_cache': 'ocr_image_cache', 'image_ocr_result': 'image_ocr_result', 'user_role': 'user_role', 'user_session': 'user_session'}

def main(argv):
    if not argv:
        raise SystemExit('Usage: python -m migrator.preview <table> [limit]')
    name=argv[0]
    limit=int(argv[1]) if len(argv)>1 else 10
    src=TABLE_SRC.get(name)
    if not src:
        raise SystemExit(f"Unknown table {name}. Supported: {', '.join(TABLE_SRC.keys())}")
    rows=preview_rows(src, limit=limit)
    print(json.dumps(rows, ensure_ascii=False, indent=2, default=str))

if __name__=='__main__':
    main(sys.argv[1:])
