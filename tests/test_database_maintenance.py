# 使用真实 SQLite 连接和进程验证 Executor 数据库收尾。

import sqlite3
import subprocess
import sys
import threading
import time
from contextlib import closing
from pathlib import Path

from executor.database_maintenance import DatabaseMaintenance, checkpoint_database
from executor.process_supervisor import ProcessSupervisor


def test_checkpoint_preserves_committed_rows_and_truncates_wal(tmp_path: Path) -> None:
    """真实 WAL 中已提交的数据在 TRUNCATE 后仍然完整可读。"""
    database = tmp_path / "opencode.db"
    with closing(sqlite3.connect(database)) as writer:
        writer.execute("PRAGMA journal_mode=WAL")
        writer.execute("CREATE TABLE evidence (value TEXT)")
        writer.execute("INSERT INTO evidence VALUES (?)", ("checkpoint-evidence",))
        writer.commit()
        wal = Path(str(database) + "-wal")
        assert wal.stat().st_size > 0
        result = checkpoint_database(database)
        assert result == {
            "status": "succeeded", "code": "wal_checkpoint_complete",
            "busy": 0, "log_pages": 0, "checkpointed_pages": 0,
        }
        assert wal.stat().st_size == 0
        assert writer.execute("SELECT value FROM evidence").fetchone() == ("checkpoint-evidence",)


def test_busy_reader_returns_bounded_diagnostic(tmp_path: Path) -> None:
    """读取事务占用旧快照时返回 busy 页数，释放读取事务后可以重新维护。"""
    database = tmp_path / "opencode.db"
    with closing(sqlite3.connect(database)) as writer, closing(sqlite3.connect(database)) as reader:
        writer.execute("PRAGMA journal_mode=WAL")
        writer.execute("CREATE TABLE evidence (value INTEGER)")
        writer.commit()
        reader.execute("BEGIN")
        reader.execute("SELECT * FROM evidence").fetchall()
        writer.execute("INSERT INTO evidence VALUES (1)")
        writer.commit()
        started = time.monotonic()
        result = checkpoint_database(database)
        assert time.monotonic() - started < 3
        assert result["code"] == "wal_checkpoint_busy"
        assert result["status"] == "failed"
        assert result["busy"] == 1
        assert result["log_pages"] > result["checkpointed_pages"]
        reader.rollback()
        assert checkpoint_database(database)["status"] == "succeeded"


def test_missing_and_invalid_database_preserve_sqlite_reason(tmp_path: Path) -> None:
    """缺失文件不被创建，损坏文件返回 SQLite 原始类别和具体消息。"""
    database = tmp_path / "opencode.db"
    result = checkpoint_database(database)
    assert result["sqlite_errorname"] == "SQLITE_CANTOPEN"
    assert result["message"]
    assert not database.exists()
    database.write_bytes(b"invalid sqlite database" * 64)
    result = checkpoint_database(database)
    assert result["code"] == "wal_checkpoint_sqlite_error"
    assert result["sqlite_errorname"] == "SQLITE_NOTADB"
    assert result["message"] == "file is not a database"


def test_shared_database_checkpoints_only_after_last_user(tmp_path: Path) -> None:
    """共享数据库最后一个使用者释放之前保留 WAL，未回收进程继续占用注册。"""
    database = tmp_path / "opencode.db"
    maintenance = DatabaseMaintenance()
    with closing(sqlite3.connect(database)) as writer:
        writer.execute("PRAGMA journal_mode=WAL")
        writer.execute("CREATE TABLE evidence (value INTEGER)")
        writer.commit()
        maintenance.acquire(database)
        maintenance.acquire(database)
        assert maintenance.release(database, process_reaped=True)["status"] == "deferred"
        assert Path(str(database) + "-wal").stat().st_size > 0
        assert maintenance.release(database, process_reaped=True)["status"] == "succeeded"
        assert Path(str(database) + "-wal").stat().st_size == 0
        maintenance.acquire(database)
        assert maintenance.release(database, process_reaped=False)["code"] == "wal_checkpoint_process_unreaped"
        maintenance.acquire(database)
        assert maintenance.release(database, process_reaped=True)["status"] == "deferred"


def test_process_exit_then_checkpoint(tmp_path: Path) -> None:
    """真实 SQLite 写入进程被终止并回收后，维护连接回收 WAL 空间且保留数据。"""
    database = tmp_path / "opencode.db"
    maintenance = DatabaseMaintenance()
    maintenance.acquire(database)
    process = subprocess.Popen(
        [sys.executable, str(Path(__file__).parent / "support" / "wal_writer.py"), str(database)],
        stdout=subprocess.PIPE, stdin=subprocess.PIPE, text=True, start_new_session=True,
    )
    try:
        assert process.stdout.readline().strip() == "committed"
        assert Path(str(database) + "-wal").stat().st_size > 0
        termination = ProcessSupervisor().terminate(process)
        assert termination.reaped
        assert maintenance.release(database, process_reaped=termination.reaped)["status"] == "succeeded"
        wal = Path(str(database) + "-wal")
        assert not wal.exists() or wal.stat().st_size == 0
        with closing(sqlite3.connect(database)) as reader:
            assert reader.execute("SELECT value FROM evidence").fetchall() == [("committed",)]
    finally:
        ProcessSupervisor().terminate(process)
        process.stdout.close()
        process.stdin.close()


def test_registration_waits_for_running_checkpoint(tmp_path: Path) -> None:
    """真实写入锁使 checkpoint 等待期间，新的同库进程注册也必须等待。"""
    database = tmp_path / "opencode.db"
    maintenance = DatabaseMaintenance()
    with closing(sqlite3.connect(database)) as writer:
        writer.execute("PRAGMA journal_mode=WAL")
        writer.execute("CREATE TABLE evidence (value INTEGER)")
        writer.commit()
        writer.execute("BEGIN IMMEDIATE")
        maintenance.acquire(database)
        results = []
        finished = threading.Event()

        def release() -> None:
            """让真实 checkpoint 在写入锁上等待，收集实际返回结果。"""
            results.append(maintenance.release(database, process_reaped=True))

        def acquire() -> None:
            """注册后记录完成事件，验证注册与收尾操作的顺序。"""
            maintenance.acquire(database)
            finished.set()

        checkpoint_thread = threading.Thread(target=release)
        checkpoint_thread.start()
        deadline = time.monotonic() + 2
        while database not in maintenance._checkpointing and time.monotonic() < deadline:
            time.sleep(0.001)
        assert database in maintenance._checkpointing
        # 其他任务的独立数据库不应被当前数据库的 SQLite 等待阻止。
        other_database = tmp_path / "other.db"
        with closing(sqlite3.connect(other_database)) as other:
            other.execute("CREATE TABLE evidence (value INTEGER)")
        maintenance.acquire(other_database)
        assert maintenance.release(other_database, process_reaped=True)["status"] == "succeeded"
        start_thread = threading.Thread(target=acquire)
        start_thread.start()
        assert not finished.wait(0.05)
        writer.rollback()
        checkpoint_thread.join(3)
        start_thread.join(3)
        assert finished.is_set()
        assert results[0]["status"] == "succeeded"
        assert maintenance.release(database, process_reaped=True)["status"] == "succeeded"


def test_checkpoint_rejects_database_and_sidecar_symlinks(tmp_path: Path) -> None:
    """维护操作拒绝数据库及 sidecar 符号链接，保持目标文件内容不变。"""
    target = tmp_path / "target"
    target.write_text("protected")
    database = tmp_path / "opencode.db"
    for path in (database, Path(str(database) + "-wal"), Path(str(database) + "-shm")):
        path.symlink_to(target)
        assert checkpoint_database(database)["code"] == "wal_checkpoint_path_invalid"
        assert target.read_text() == "protected"
        path.unlink()
