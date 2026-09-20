from __future__ import annotations

import asyncio
from contextvars import ContextVar
import os
from pathlib import Path
import threading
from uuid import uuid4

from fastapi import FastAPI, HTTPException
import pytest
from sqlalchemy import create_engine, delete, event
from starlette.requests import Request

import api
from backend.config import Settings
from backend.persistence.models import Scope, ScopeContext
from backend.persistence.resources import DatabaseResources
from backend.scope import ScopeServiceFactory
from backend.search_http import SearchRequest


# 使用真实搜索入口和数据库查询验证线程调度及请求上下文传播。
def test_search_database_queries_run_in_worker_thread(tmp_path: Path) -> None:
    """空测试 scope 通过真实缓存查询返回 503，所有查询均在工作线程执行。"""
    url = os.environ.get("MEMEMEOW_TEST_DATABASE_URL")
    if not url:
        pytest.skip("需要显式提供开发测试 PostgreSQL 连接")
    engine = create_engine(url, pool_size=1, max_overflow=0, pool_timeout=3)
    scope = ScopeContext(f"search-threadpool-validation-{uuid4().hex}")
    resources = DatabaseResources(engine, image_root=tmp_path / "images", data_root=tmp_path, require_local_scope=False)
    settings = Settings(data_root=tmp_path, image_root=tmp_path / "images")
    factory = ScopeServiceFactory(resources, settings)
    request_context = ContextVar("search_request_context", default="missing")
    observed: list[tuple[int, str]] = []

    def record_query(_connection, _cursor, _statement, _parameters, _context, _executemany) -> None:
        """通过 SQLAlchemy 事件观察真实查询的线程和请求上下文。"""
        observed.append((threading.get_ident(), request_context.get()))

    try:
        with resources.factory() as session, session.begin():
            session.add(Scope(id=scope.scope_id))
        application = FastAPI()
        application.state.settings = settings
        request = Request({"type": "http", "method": "POST", "path": "/search", "headers": [], "app": application})
        request.state.scope = scope
        request.state.services = factory.for_scope(scope)

        async def search() -> None:
            """通过真实 API handler 验证查询线程、上下文和错误传播。"""
            loop_thread = threading.get_ident()
            token = request_context.set(scope.scope_id)
            event.listen(engine, "before_cursor_execute", record_query)
            try:
                with pytest.raises(HTTPException) as captured:
                    await api.search_images(request, SearchRequest(query="线程调度测试"))
                assert captured.value.status_code == 503
                assert captured.value.detail["error"] == "cache_not_ready"
                assert observed
                assert all(thread_id != loop_thread for thread_id, _ in observed)
                assert all(context == scope.scope_id for _, context in observed)
                assert engine.pool.checkedout() == 0
            finally:
                event.remove(engine, "before_cursor_execute", record_query)
                request_context.reset(token)

        asyncio.run(search())
    finally:
        factory.shutdown()
        with resources.factory() as session, session.begin():
            session.execute(delete(Scope).where(Scope.id == scope.scope_id))
        engine.dispose()
