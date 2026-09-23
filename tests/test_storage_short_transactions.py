# 使用真实 PostgreSQL、图片文件和独立 scope 验证存储事务及进程互斥。

from __future__ import annotations

import hashlib
import multiprocessing
import os
import sys
from io import BytesIO
from pathlib import Path
from uuid import uuid4

import pytest
from PIL import Image
from sqlalchemy import delete, select

from backend.persistence.engine import create_engine_for_url
from backend.persistence.file_lock import shared_file_lock
from backend.persistence.models import Meme, Scope, StorageOperation
from backend.persistence.resources import DatabaseResources
from backend.persistence.storage import StorageCoordinator
from backend.services.metadata import PostgresMetadataService
from backend.services.thumbnails import DerivedThumbnailService


def _upload_process(url, root, scope, content, result, initialized):
    """独立进程使用单连接池上传真实图片，返回内容身份。"""
    engine = create_engine_for_url(url, pool_size=1, max_overflow=0, pool_timeout=2)
    try:
        resources = DatabaseResources(engine, image_root=root / "images", data_root=root / "data", require_local_scope=False)
        initialized.wait(timeout=20)
        record = StorageCoordinator(resources, scope_id=scope).upload(content, extension=".png", context={}, provenance={})
        result.put(str(record.id))
    finally:
        engine.dispose()


def _hold_lock(path, ready):
    """持有真实文件锁，供父进程验证中断后的自动释放。"""
    with shared_file_lock(path):
        ready.send(True)
        ready.recv()


@pytest.fixture
def storage_case(tmp_path):
    """创建独立测试 scope；只清理本次 scope 的记录。"""
    url = os.environ["MEMEMEOW_TEST_DATABASE_URL"]
    engine = create_engine_for_url(url, pool_size=1, max_overflow=0, pool_timeout=2)
    scope = "test-short-transactions-" + uuid4().hex
    with engine.begin() as connection:
        connection.execute(Scope.__table__.insert().values(id=scope))
    resources = DatabaseResources(engine, image_root=tmp_path / "images", data_root=tmp_path / "data", require_local_scope=False)
    buffer = BytesIO()
    Image.new("RGB", (64, 64), "blue").save(buffer, format="PNG")
    try:
        yield resources, scope, buffer.getvalue(), url, tmp_path
    finally:
        sys.setprofile(None)
        with engine.begin() as connection:
            connection.execute(delete(StorageOperation).where(StorageOperation.scope_id == scope))
            connection.execute(delete(Scope).where(Scope.id == scope))
        engine.dispose()


def test_upload_generate_delete_single_connection(storage_case):
    """完整执行上传、读取、生成和删除，单连接池应始终能够归还连接。"""
    resources, scope, content, _, _ = storage_case
    observed = []

    def observe_file_calls(frame, event, argument):
        """记录真实文件操作入口的连接占用，不替换任何业务调用。"""
        if event == "call" and frame.f_code.co_name in {"exists_with_identity", "stage_bytes", "link_move", "quarantine", "unlink", "_identity", "_read_identity"}:
            observed.append((frame.f_code.co_name, resources.engine.pool.checkedout()))

    sys.setprofile(observe_file_calls)
    storage = StorageCoordinator(resources, scope_id=scope)
    record = storage.upload(content, extension=".png", context={}, provenance={})
    assert resources.engine.pool.checkedout() == 0
    assert storage.upload(content, extension=".png", context={}, provenance={}).id == record.id
    metadata = PostgresMetadataService(resources, scope_id=scope)
    assert metadata.image_for_meme(record.id)[1].read_bytes() == content
    thumbnails = DerivedThumbnailService(resources, scope_id=scope)
    assert thumbnails.generate(record.id)["status"] == "available"
    assert thumbnails.media_path(record.id)[0].is_file()
    assert thumbnails.cleanup_for_meme(record.id) == 1
    assert thumbnails.generate(record.id)["status"] == "available"
    storage.delete(record.id)
    with resources.environment(scope) as environment:
        assert environment.memes.get(record.id) is None
    assert resources.engine.pool.checkedout() == 0
    sys.setprofile(None)
    assert observed and all(count == 0 for _, count in observed), observed


def test_multiprocess_upload_and_lock_release(storage_case):
    """两个独立进程上传同一图片只创建一条记录，进程终止会释放文件锁。"""
    resources, scope, content, url, root = storage_case
    context = multiprocessing.get_context("spawn")
    results = context.Queue()
    initialized = context.Barrier(2)
    processes = [context.Process(target=_upload_process, args=(url, root, scope, content, results, initialized)) for _ in range(2)]
    for process in processes:
        process.start()
    for process in processes:
        process.join(30)
        assert process.exitcode == 0
    assert results.get(timeout=2) == results.get(timeout=2)
    path = root / "locks" / "release.lock"
    parent, child = context.Pipe()
    holder = context.Process(target=_hold_lock, args=(path, child))
    holder.start()
    assert parent.poll(10) and parent.recv()
    with shared_file_lock(path, blocking=False) as acquired:
        assert not acquired
        assert resources.engine.pool.checkedout() == 0
    holder.terminate()
    holder.join(10)
    with shared_file_lock(path, blocking=False) as acquired:
        assert acquired


def test_recover_prepared_upload(storage_case):
    """真实 prepared 记录和暂存文件能够由恢复器完成发布。"""
    resources, scope, content, _, _ = storage_case
    storage = StorageCoordinator(resources, scope_id=scope)
    digest = hashlib.sha256(content).hexdigest()
    key = digest + ".png"
    token = uuid4()
    staged = storage.blob_store.stage_bytes(content, token=token)
    with resources.environment(scope) as environment:
        record = environment.memes.create(storage_key=key, extension=".png", size_bytes=len(content), sha256=digest, context={}, provenance={}, status="pending")
        environment.uow.session.add(StorageOperation(scope_id=scope, meme_id=record.id, operation_type="upload", operation_token=token, target_key=key, staging_key=staged, after_sha256=digest, after_size=len(content), status="prepared"))
    assert storage.recover()["completed"] == 1
    assert storage.blob_store.resolve(key).read_bytes() == content
    assert resources.engine.pool.checkedout() == 0
