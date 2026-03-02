from __future__ import annotations
import pymysql
from pymysql.cursors import DictCursor
from .config import DBConfig

def connect(cfg: DBConfig):
    return pymysql.connect(
        host=cfg.host,
        port=cfg.port,
        user=cfg.user,
        password=cfg.password,
        database=cfg.db,
        charset=cfg.charset,
        autocommit=False,
        cursorclass=DictCursor,
    )
