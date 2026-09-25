# Executor 的 SQLite WAL 收尾与共享数据库使用协调。

from __future__ import annotations

import sqlite3
import stat
import threading
from contextlib import closing
from pathlib import Path


def checkpoint_database(database: Path) -> dict[str, object]:
    """对已有数据库执行有限等待的 TRUNCATE，返回维护结果及具体 SQLite 原因。"""
    try:
        if any(parent.is_symlink() for parent in database.absolute().parents):
            return {"status": "failed", "code": "wal_checkpoint_path_invalid", "message": "数据库目录不能包含符号链接"}
        for path in (database, Path(str(database) + "-wal"), Path(str(database) + "-shm")):
            if path.is_symlink():
                return {"status": "failed", "code": "wal_checkpoint_path_invalid", "message": "数据库及 WAL/SHM 文件不能是符号链接"}
        if database.exists() and not stat.S_ISREG(database.stat().st_mode):
            return {"status": "failed", "code": "wal_checkpoint_path_invalid", "message": "数据库路径必须是普通文件"}
        # mode=rw 禁止维护操作创建数据库；连接关闭前必须读取 checkpoint 返回值。
        with closing(sqlite3.connect(database.absolute().as_uri() + "?mode=rw", uri=True, timeout=1.0)) as connection:
            busy, log_pages, checkpointed_pages = connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        return {
            "status": "succeeded" if busy == 0 else "failed",
            "code": "wal_checkpoint_complete" if busy == 0 else "wal_checkpoint_busy",
            "busy": busy,
            "log_pages": log_pages,
            "checkpointed_pages": checkpointed_pages,
        }
    except sqlite3.Error as exc:
        # 维护故障独立于业务结果，保留 SQLite 的错误类别和具体消息。
        return {
            "status": "failed", "code": "wal_checkpoint_sqlite_error",
            "sqlite_errorcode": getattr(exc, "sqlite_errorcode", None),
            "sqlite_errorname": getattr(exc, "sqlite_errorname", None),
            "message": str(exc),
        }
    except OSError as exc:
        return {"status": "failed", "code": "wal_checkpoint_filesystem_error", "errno": exc.errno, "message": str(exc)}


class DatabaseMaintenance:
    """记录 Executor 内数据库使用数，最后一个已回收进程负责 WAL 收尾。"""

    def __init__(self) -> None:
        """初始化使用数和收尾集合，阻止维护期间启动同库进程。"""
        self._condition = threading.Condition()
        self._users: dict[Path, int] = {}
        self._checkpointing: set[Path] = set()

    def acquire(self, database: Path) -> None:
        """在启动进程之前注册数据库使用，允许同库任务正常并发执行。"""
        with self._condition:
            self._condition.wait_for(lambda: database not in self._checkpointing)
            self._users[database] = self._users.get(database, 0) + 1

    def release(self, database: Path, *, process_reaped: bool) -> dict[str, object]:
        """进程回收后释放使用数并维护空闲数据库；未知进程继续占用注册。"""
        with self._condition:
            if not process_reaped:
                return {"status": "skipped", "code": "wal_checkpoint_process_unreaped"}
            users = self._users[database] - 1
            if users:
                self._users[database] = users
                return {"status": "deferred", "code": "wal_checkpoint_database_in_use"}
            del self._users[database]
            self._checkpointing.add(database)
        try:
            return checkpoint_database(database)
        finally:
            with self._condition:
                self._checkpointing.remove(database)
                self._condition.notify_all()
