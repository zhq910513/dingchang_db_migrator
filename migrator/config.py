from __future__ import annotations
import os
from dataclasses import dataclass

@dataclass(frozen=True)
class DBConfig:
    host: str
    port: int
    user: str
    password: str
    db: str
    charset: str = "utf8mb4"

def load_config() -> DBConfig:
    return DBConfig(
        host=os.getenv("MYSQL_HOST", "127.0.0.1"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.getenv("MYSQL_USER", "root"),
        password=os.getenv("MYSQL_PASSWORD", ""),
        db=os.getenv("MYSQL_DB", "order_system"),
        charset=os.getenv("MYSQL_CHARSET", "utf8mb4"),
    )
