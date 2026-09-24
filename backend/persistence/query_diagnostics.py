# 记录 SQL 与事务提交耗时，供数据库连接等待调查使用。

from __future__ import annotations

import threading
import time
import inspect

from loguru import logger
from sqlalchemy import event
from sqlalchemy.engine import Engine


def current_operation() -> str:
    """读取最近业务调用的模块和函数名，不保存参数、文件路径或局部变量。"""
    frame = inspect.currentframe()
    try:
        for _ in range(48):
            frame = frame.f_back if frame is not None else None
            if frame is None:
                break
            module = str(frame.f_globals.get("__name__", ""))
            if module.startswith(("backend.", "server.")) and module != __name__:
                return f"{module}.{frame.f_code.co_name}"[:160]
        return "unknown"
    finally:
        del frame


def attach_query_diagnostics(engine: Engine) -> None:
    """安装 Engine SQL 计时；只记录操作类型与错误类别，不输出 SQL 或参数。"""

    @event.listens_for(engine, "before_cursor_execute")
    def before_execute(connection, cursor, statement, parameters, context, executemany):
        """保存本次执行开始时间，供完成与异常事件共同读取。"""
        context._diagnostic_started_at = time.monotonic()

    @event.listens_for(engine, "after_cursor_execute")
    def after_execute(connection, cursor, statement, parameters, context, executemany):
        """记录超过阈值的 SQL，包括等待数据库锁的时间。"""
        duration_ms = int((time.monotonic() - context._diagnostic_started_at) * 1000)
        if duration_ms < 100:
            return
        compiled = context.compiled
        operation = type(compiled.statement).__name__ if compiled is not None else "driver_sql"
        logger.warning("db_statement_slow operation={} statement_type={} duration_ms={} thread_id={} executemany={}", current_operation(), operation, duration_ms, threading.get_ident(), executemany)

    @event.listens_for(engine, "handle_error")
    def handle_error(context):
        """保留数据库异常类别和 SQLSTATE，业务异常继续向调用者传播。"""
        original = context.original_exception
        logger.error("db_statement_error error_type={} sqlstate={} thread_id={}", type(original).__name__, getattr(original, "sqlstate", None), threading.get_ident())
