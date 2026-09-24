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
from backend.persistence.engine import DatabaseError
from backend.persistence.file_lock import shared_file_lock
from backend.persistence.models import Meme, Scope, StorageOperation
from backend.persistence.resources import DatabaseResources
from backend.persistence.storage import StorageCoordinator
from backend.services.metadata import PostgresMetadataService
from backend.metadata import MetadataError
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
    metadata.update_display_name(record.id, "updated-name")
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


@pytest.mark.parametrize("phase", ["prepared", "file_applied"])
def test_recover_delete_preserves_identity(storage_case, phase):
    """恢复原图隔离前后中断的删除，清理相同 Meme 的派生内容。"""
    resources, scope, content, _, _ = storage_case
    storage = StorageCoordinator(resources, scope_id=scope)
    record = storage.upload(content, extension=".png", context={}, provenance={})
    thumbnails = DerivedThumbnailService(resources, scope_id=scope)
    thumbnails.generate(record.id)
    token = uuid4()
    with resources.environment(scope) as environment:
        environment.uow.session.add(StorageOperation(
            scope_id=scope, meme_id=record.id, operation_type="delete", operation_token=token,
            source_key=record.storage_key, target_key=f".quarantine/{token.hex}.blob",
            before_sha256=record.sha256, before_size=record.size_bytes, status=phase,
            error=storage._delete_identity_marker(record.id, record.sha256, record.size_bytes),
        ))
    if phase == "file_applied":
        storage.blob_store.quarantine(record.storage_key, token=token)
    assert storage.recover()["completed"] == 1
    with resources.environment(scope) as environment:
        assert environment.memes.get(record.id) is None
    assert not storage._thumbnail_file_keys(record.id, record.sha256)
    assert not storage.blob_store.exists_with_identity(f".quarantine/{token.hex}.blob")


def test_recovery_rejects_changed_record(storage_case):
    """恢复输入与当前记录不一致时，原图和记录均保持原状。"""
    resources, scope, content, _, _ = storage_case
    storage = StorageCoordinator(resources, scope_id=scope)
    record = storage.upload(content, extension=".png", context={}, provenance={})
    token = uuid4()
    with resources.environment(scope) as environment:
        environment.uow.session.add(StorageOperation(
            scope_id=scope, meme_id=record.id, operation_type="delete", operation_token=token,
            source_key=record.storage_key, target_key=f".quarantine/{token.hex}.blob",
            before_sha256="0" * 64, before_size=record.size_bytes, status="prepared",
        ))
    assert storage.recover()["blocked"] == 1
    assert storage.blob_store.resolve(record.storage_key).read_bytes() == content
    with resources.environment(scope) as environment:
        assert environment.memes.get(record.id) is not None


def _generate_or_delete(url, root, scope, meme_id, generate, initialized, results):
    """独立进程完成初始化后并发执行真实生成或删除。"""
    from backend.services.thumbnails import ThumbnailError

    engine = create_engine_for_url(url, pool_size=1, max_overflow=0, pool_timeout=2)
    try:
        resources = DatabaseResources(engine, image_root=root / "images", data_root=root / "data", require_local_scope=False)
        initialized.wait(timeout=20)
        if generate:
            try:
                results.put(DerivedThumbnailService(resources, scope_id=scope).generate(meme_id)["status"])
            except ThumbnailError as exc:
                results.put(exc.code)
        else:
            StorageCoordinator(resources, scope_id=scope).delete(meme_id)
            results.put("deleted")
    finally:
        engine.dispose()


def test_multiprocess_generation_and_deletion(storage_case):
    """删除与生成同时运行，最终记录和派生文件均应已删除。"""
    resources, scope, content, url, root = storage_case
    storage = StorageCoordinator(resources, scope_id=scope)
    record = storage.upload(content, extension=".png", context={}, provenance={})
    context = multiprocessing.get_context("spawn")
    initialized = context.Barrier(2)
    results = context.Queue()
    processes = [context.Process(target=_generate_or_delete, args=(url, root, scope, record.id, generate, initialized, results)) for generate in (True, False)]
    for process in processes:
        process.start()
    for process in processes:
        process.join(30)
        assert process.exitcode == 0
    outcomes = [results.get(timeout=2), results.get(timeout=2)]
    assert "deleted" in outcomes
    assert set(outcomes) <= {"deleted", "available", "meme_not_found"}
    with resources.environment(scope) as environment:
        assert environment.memes.get(record.id) is None
    assert storage._thumbnail_file_keys(record.id, record.sha256) == []


@pytest.mark.parametrize("reupload", [False, True])
def test_context_update_rejects_deleted_identity(storage_case, reupload):
    """校验文件后发生删除或重新上传时，旧更新必须拒绝写入且不得创建记录。"""
    resources, scope, content, _, _ = storage_case
    storage = StorageCoordinator(resources, scope_id=scope)
    record = storage.upload(content, extension=".png", context={}, provenance={})
    metadata = PostgresMetadataService(resources, scope_id=scope)
    path = storage.blob_store.resolve(record.storage_key)
    replacement = []

    def delete_after_read(frame, event, argument):
        """在真实身份读取返回时完成删除，精确覆盖写回之前的并发窗口。"""
        if event == "return" and frame.f_code is PostgresMetadataService._identity.__code__:
            sys.setprofile(None)
            assert resources.engine.pool.checkedout() == 0
            storage.delete(record.id)
            if reupload:
                replacement.append(storage.upload(content, extension=".png", context={}, provenance={}))

    sys.setprofile(delete_after_read)
    try:
        with pytest.raises(MetadataError, match="metadata_missing"):
            metadata.update_context(path, {"title": "过期结果"}, producer="test")
    finally:
        sys.setprofile(None)
    with resources.environment(scope) as environment:
        assert environment.memes.get(record.id) is None
        current = environment.memes.by_storage_key(record.storage_key)
        if reupload:
            assert current.id == replacement[0].id
            assert current.meme_context.get("title") != "过期结果"
        else:
            assert current is None


def test_pending_registration_and_context_update(storage_case):
    """已有图片登记与元数据更新保持可用，文件读取和锁等待均在事务外。"""
    resources, scope, content, _, _ = storage_case
    storage = StorageCoordinator(resources, scope_id=scope)
    key = hashlib.sha256(content).hexdigest() + ".png"
    stage = storage.blob_store.stage_bytes(content, token=uuid4())
    storage.blob_store.link_move(stage, key)
    metadata = PostgresMetadataService(resources, scope_id=scope)
    path = storage.blob_store.resolve(key)
    observed = []

    def observe(frame, event, argument):
        """核对真实登记及更新调用中的文件读取和文件锁连接占用。"""
        if event == "call" and frame.f_code.co_name in {"_identity", "shared_file_lock"}:
            observed.append(resources.engine.pool.checkedout())

    sys.setprofile(observe)
    try:
        metadata.create_pending(path)
        metadata.create_pending(path)
        result = metadata.update_context(path, {"title": "已登记图片"}, producer="test")
    finally:
        sys.setprofile(None)
    assert result.meme_context.title == "已登记图片"
    assert observed and all(value == 0 for value in observed)


def test_scans_release_connections(storage_case):
    """预检与完整性扫描在文件读取和目录遍历期间归还连接，缺失记录仍能标记。"""
    resources, scope, content, _, _ = storage_case
    storage = StorageCoordinator(resources, scope_id=scope)
    record = storage.upload(content, extension=".png", context={}, provenance={})
    observed = []

    def observe(frame, event, argument):
        """观察真实扫描入口，不替换文件或数据库操作。"""
        if event == "call" and frame.f_code.co_name in {"exists_with_identity", "rglob", "shared_file_lock"}:
            observed.append(resources.engine.pool.checkedout())

    sys.setprofile(observe)
    try:
        assert storage.flat_preflight()["missing_files"] == []
        assert storage.integrity_scan()["missing_files"] == []
        storage.blob_store.unlink(record.storage_key)
        assert storage.flat_preflight()["missing_files"] == [str(record.id)]
        assert storage.integrity_scan()["missing_files"] == [str(record.id)]
    finally:
        sys.setprofile(None)
    assert observed and all(value == 0 for value in observed)
    with resources.environment(scope) as environment:
        assert environment.memes.get(record.id).context_status == "repair_required"


def test_scan_rejects_changed_snapshot(storage_case):
    """扫描检查期间的元数据更新使旧快照失效，不得覆盖最新状态。"""
    resources, scope, content, _, _ = storage_case
    storage = StorageCoordinator(resources, scope_id=scope)
    record = storage.upload(content, extension=".png", context={}, provenance={})
    storage.blob_store.unlink(record.storage_key)

    def update_during_scan(frame, event, argument):
        """在真实文件检查返回后更新记录，检验写回版本检查。"""
        if event == "return" and frame.f_code.co_name == "exists_with_identity":
            sys.setprofile(None)
            with resources.environment(scope) as environment:
                environment.memes.update_display_name(record.id, "changed-during-scan")

    sys.setprofile(update_during_scan)
    try:
        with pytest.raises(DatabaseError, match="storage_scan_target_changed"):
            storage.integrity_scan()
    finally:
        sys.setprofile(None)
    with resources.environment(scope) as environment:
        assert environment.memes.get(record.id).context_status != "repair_required"


def _move_until_link(url, root, scope, source, target, ready):
    """子进程执行真实移动，在 os.link 返回后等待父进程终止。"""
    engine = create_engine_for_url(url, pool_size=1, max_overflow=0, pool_timeout=2)
    try:
        resources = DatabaseResources(engine, image_root=root / "images", data_root=root / "data", require_local_scope=False)
        storage = StorageCoordinator(resources, scope_id=scope)

        def pause_after_link(frame, event, argument):
            """保留真实 link 的中间状态，供父进程验证中断恢复。"""
            if event == "c_return" and argument is os.link:
                ready.send(True)
                ready.recv()

        sys.setprofile(pause_after_link)
        storage.blob_store.link_move(source, target)
    finally:
        sys.setprofile(None)
        engine.dispose()


@pytest.mark.parametrize("operation_type", ["upload", "delete"])
def test_recover_process_terminated_after_link(storage_case, operation_type):
    """真实移动进程在 link 后终止，恢复器完成上传或删除且清理源链接。"""
    resources, scope, content, url, root = storage_case
    storage = StorageCoordinator(resources, scope_id=scope)
    token = uuid4()
    digest = hashlib.sha256(content).hexdigest()
    key = digest + ".png"
    if operation_type == "upload":
        source = storage.blob_store.stage_bytes(content, token=token)
        target = key
        with resources.environment(scope) as environment:
            record = environment.memes.create(storage_key=key, extension=".png", size_bytes=len(content), sha256=digest, context={}, provenance={}, status="pending")
            environment.uow.session.add(StorageOperation(scope_id=scope, meme_id=record.id, operation_type="upload", operation_token=token, target_key=target, staging_key=source, after_sha256=digest, after_size=len(content), status="prepared"))
    else:
        record = storage.upload(content, extension=".png", context={}, provenance={})
        source, target = key, f".quarantine/{token.hex}.blob"
        with resources.environment(scope) as environment:
            environment.uow.session.add(StorageOperation(scope_id=scope, meme_id=record.id, operation_type="delete", operation_token=token, source_key=source, target_key=target, before_sha256=digest, before_size=len(content), status="prepared", error=storage._delete_identity_marker(record.id, digest, len(content))))
    context = multiprocessing.get_context("spawn")
    parent, child = context.Pipe()
    process = context.Process(target=_move_until_link, args=(url, root, scope, source, target, child))
    process.start()
    try:
        assert parent.poll(20) and parent.recv()
    finally:
        process.terminate()
        process.join(10)
        parent.close()
        child.close()
    assert process.exitcode is not None
    assert storage.blob_store._key_path(source).samefile(storage.blob_store._key_path(target))
    assert storage.recover()["completed"] == 1
    assert not storage.blob_store.exists_with_identity(source)
    with resources.environment(scope) as environment:
        assert (environment.memes.get(record.id) is not None) == (operation_type == "upload")
    assert storage.blob_store.exists_with_identity(target) == (operation_type == "upload")


@pytest.mark.parametrize("operation_type", ["upload", "delete"])
def test_recovery_preserves_distinct_files(storage_case, operation_type):
    """内容相同但 inode 不同的两个文件保持不变，恢复记录明确报告歧义。"""
    resources, scope, content, _, _ = storage_case
    storage = StorageCoordinator(resources, scope_id=scope)
    record = storage.upload(content, extension=".png", context={}, provenance={})
    token = uuid4()
    staged = storage.blob_store.stage_bytes(content, token=token)
    if operation_type == "upload":
        source, target = staged, record.storage_key
        operation = StorageOperation(scope_id=scope, meme_id=record.id, operation_type="upload", operation_token=token, staging_key=source, target_key=target, after_sha256=record.sha256, after_size=record.size_bytes, status="prepared")
    else:
        source, target = record.storage_key, f".quarantine/{token.hex}.blob"
        storage.blob_store.link_move(staged, target)
        operation = StorageOperation(scope_id=scope, meme_id=record.id, operation_type="delete", operation_token=token, source_key=source, target_key=target, before_sha256=record.sha256, before_size=record.size_bytes, status="prepared", error=storage._delete_identity_marker(record.id, record.sha256, record.size_bytes))
    with resources.environment(scope) as environment:
        environment.uow.session.add(operation)
    assert not storage.blob_store._key_path(source).samefile(storage.blob_store._key_path(target))
    assert storage.recover()["blocked"] == 1
    assert storage.blob_store._key_path(source).read_bytes() == content
    assert storage.blob_store._key_path(target).read_bytes() == content
    with resources.environment(scope) as environment:
        current = environment.uow.session.get(StorageOperation, operation.id)
        assert current.error["error"] == f"{operation_type}_recovery_ambiguous"


@pytest.mark.skipif(not hasattr(StorageCoordinator, "_recover_rename"), reason="当前版本不支持历史 rename operation")
def test_legacy_rename_recovery_releases_connection(storage_case):
    """历史重命名已经提交记录时，恢复文件移动并保留原有 revision。"""
    resources, scope, content, _, _ = storage_case
    storage = StorageCoordinator(resources, scope_id=scope)
    record = storage.upload(content, extension=".png", context={}, provenance={})
    source = "legacy-image.png"
    storage.blob_store.link_move(record.storage_key, source)
    with resources.environment(scope) as environment:
        current = environment.memes.get(record.id, for_update=True)
        current.revision = 2
        environment.uow.session.add(StorageOperation(scope_id=scope, meme_id=record.id, operation_type="rename", operation_token=uuid4(), source_key=source, target_key=record.storage_key, before_sha256=record.sha256, after_sha256=record.sha256, before_size=record.size_bytes, after_size=record.size_bytes, expected_revision=1, status="prepared"))
    observed = []

    def observe(frame, event, argument):
        """观察历史恢复中的真实文件操作连接占用。"""
        if event == "call" and frame.f_code.co_name in {"exists_with_identity", "link_move", "finish_link_move"}:
            observed.append(resources.engine.pool.checkedout())

    sys.setprofile(observe)
    try:
        assert storage.recover()["completed"] == 1
    finally:
        sys.setprofile(None)
    assert observed and all(value == 0 for value in observed)
    assert storage.blob_store.resolve(record.storage_key).read_bytes() == content
    assert not storage.blob_store.exists_with_identity(source)
    with resources.environment(scope) as environment:
        assert environment.memes.get(record.id).revision == 2
