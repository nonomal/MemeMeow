# 使用真实连接池等待验证任务查询能够让事件循环继续执行。

import asyncio
from uuid import uuid4

from dotenv import dotenv_values
from fastapi import FastAPI, Request, HTTPException
from sqlalchemy import delete

from backend.image_processing import ImageProcessingRepository
from backend.persistence.engine import create_engine_for_url
from backend.persistence.models import Scope, ScopeContext
from backend.persistence.resources import DatabaseResources
from backend.services.tasks import PostgresTaskService
from backend.task_http import get_task


def test_task_read_releases_event_loop_while_pool_busy(tmp_path):
    """查询等待唯一连接时，同一事件循环仍能归还连接并取得真实任务结果。"""
    engine = create_engine_for_url(dotenv_values(".env")["MEMEMEOW_DATABASE_URL"], pool_size=1, max_overflow=0, pool_timeout=2)
    scope = ScopeContext("test-http-pool-" + uuid4().hex)
    with engine.begin() as connection:
        connection.execute(Scope.__table__.insert().values(id=scope.scope_id))
    resources = DatabaseResources(engine, image_root=tmp_path / "images", data_root=tmp_path / "data", require_local_scope=False)
    tasks = PostgresTaskService(resources, scope_id=scope.scope_id)
    record = tasks.submit("cache_generation", {}, schedule=False)
    request = Request({"type": "http", "app": FastAPI(), "headers": []})

    async def exercise():
        """占用测试专用连接池，再由事件循环自身解除等待。"""
        connection = engine.connect()
        try:
            pending = asyncio.create_task(get_task(request, record.task_id, service=lambda received, name: tasks, error=lambda status, code, message: HTTPException(status, code), processing_repository=lambda received: ImageProcessingRepository(resources, scope)))
            await asyncio.sleep(0.05)
            assert not pending.done()
            connection.close()
            result = await asyncio.wait_for(pending, 3)
            assert result["task_id"] == record.task_id
            assert result["status"] == "queued"
        finally:
            connection.close()

    try:
        asyncio.run(exercise())
    finally:
        tasks.shutdown()
        with engine.begin() as connection:
            connection.execute(delete(Scope).where(Scope.id == scope.scope_id))
        engine.dispose()
