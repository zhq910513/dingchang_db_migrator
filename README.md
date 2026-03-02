# Dingchang DB Migrator (STRICT)
最高规格迁移器：逐行写入、先抽样预览、遇到任何一行报错立即打印原始数据并停止。

## 特性
- 每张表一个脚本：`migrator/tables/<table>.py`
- 迁移前必须先 preview（抽样查看）
- 写入模式：逐行 INSERT/UPSERT（不批量）
- 任意一行失败：打印该行 JSON + SQL + 错误并停止（raise）
- 支持全量 / 增量（migration_state 水位线）

## 安装
```bash
pip install -r requirements.txt
```

## 环境变量
```bash
export MYSQL_HOST=127.0.0.1
export MYSQL_PORT=3306
export MYSQL_USER=dingchang_app
export MYSQL_PASSWORD='***'
export MYSQL_DB=order_system
export MYSQL_CHARSET=utf8mb4
```

## 用法
### 1) 抽样预览（不会写）
```bash
python -m migrator.preview user
```

### 2) 写入（必须你确认后再执行）
- 全量：会 reset 对应 new 表的水位线
```bash
python -m migrator.runner full user
```

- 增量（可重复跑）
```bash
python -m migrator.runner inc user
```

## 当前内置表
- user -> user_new（两段式：先 parent_id=NULL upsert，再回填 parent_id）
- role -> role_new（通用 upsert）
