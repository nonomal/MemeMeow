# 使用真实上传、PostgreSQL 和 Worker 验证当前请求筛选与上传结果。

from io import BytesIO
from time import monotonic, sleep
from uuid import uuid4

import pytest
from dotenv import dotenv_values
from PIL import Image
from sqlalchemy import delete
from sqlalchemy.engine import make_url

from backend.image_processing import ImageProcessingRepository
from backend.operation_policy import AllowAllOperationPolicy, OperationPolicyGateway, PersistentGrantAssociationStore
from backend.persistence.engine import create_engine_for_url
from backend.persistence.models import Scope, ScopeContext
from backend.persistence.resources import DatabaseResources
from backend.scope import ScopeServices
from backend.services.metadata import PostgresMetadataService
from backend.services.tasks import PostgresTaskService
from backend.services.thumbnails import DerivedThumbnailService
from backend.upload_authorization import UploadAuthorization
from backend.upload_processing import prepare_upload, run_upload_processing
from backend.upload_receipts import UploadReceipts


@pytest.mark.parametrize("valid", [True, False])
def test_current_upload_status(tmp_path, valid):
    """真实接收两次上传，只查询指定请求，并验证其他 scope 与失败重试限制。"""
    url = dotenv_values(".env")["MEMEMEOW_DATABASE_URL"]
    address = make_url(url)
    assert address.host in {"localhost", "127.0.0.1"} and address.port in {5434, 25434}
    engine = create_engine_for_url(url)
    scopes = [ScopeContext("test-upload-status-" + uuid4().hex) for _ in range(2)]
    with engine.begin() as connection:
        connection.execute(Scope.__table__.insert(), [{"id": scope.scope_id} for scope in scopes])
    resources = DatabaseResources(engine, image_root=tmp_path / "images", data_root=tmp_path / "data", require_local_scope=False)
    tasks = PostgresTaskService(resources, scope_id=scopes[0])
    other = PostgresTaskService(resources, scope_id=scopes[1])
    metadata = PostgresMetadataService(resources, scope_id=scopes[0])
    thumbnails = DerivedThumbnailService(resources, scope_id=scopes[0], task_service=tasks)
    services = ScopeServices(scopes[0], metadata=metadata, tasks=tasks, search=None, reverse_image=None, visual_search=None, thumbnails=thumbnails)
    jobs = ImageProcessingRepository(resources, scopes[0])
    gateway = OperationPolicyGateway(AllowAllOperationPolicy())
    store = PersistentGrantAssociationStore(resources)

    def release(grant):
        """使用真实授权服务释放重复图片的容量预留。"""
        assert gateway.release(grant).ok
        assert store.transition(grant, "released")

    authorization = UploadAuthorization(scopes[0], gateway, store, release=release)

    def submit_processing(record, image, options):
        """登记真实处理 Job，验证上传结果独立于该 Job 的执行进度。"""
        job = jobs.create_or_reuse(record.id, record.sha256, **options)
        return jobs.snapshot(job.id)

    def handler(payload, progress):
        """运行上传校验、图片保存、授权结算和后续任务提交。"""
        return run_upload_processing(services, payload, progress, prepare=prepare_upload, reserve=authorization.reserve, commit=authorization.finish, submit_processing=submit_processing)

    def thumbnail_handler(payload, progress):
        """执行本次图片的真实缩略图生成。"""
        return thumbnails.generate(payload["meme_id"])

    tasks.register("image_upload", handler)
    tasks.register(thumbnails.TASK_TYPE, thumbnail_handler)
    receipts = UploadReceipts(resources, scopes[0], tasks)
    request_ids = [uuid4().hex, uuid4().hex]
    try:
        for index, request_id in enumerate(request_ids):
            output = BytesIO()
            Image.new("RGB", (32, 32), (index, 40, 70)).save(output, format="PNG")
            content = output.getvalue() if valid else b"invalid image content"
            receipts.accept(BytesIO(content), request_id=request_id, index=0, filename=f"test-status-{index}.png", options={"reverse_image_policy": "forbid", "auto_name": False}, size_bytes=len(content), max_bytes=1024 * 1024, max_pending_bytes=1024 * 1024, reserve=authorization.reserve)
        deadline = monotonic() + 20
        while monotonic() < deadline:
            records = tasks.upload_status(request_ids)
            if len(records) == 2 and all(record["status"] in {"succeeded", "failed"} for record in records):
                break
            sleep(0.05)
        assert len(records) == 2
        selected = tasks.upload_status([request_ids[1]])
        assert len(selected) == 1
        assert selected[0]["upload"]["filename"] == "test-status-1.png"
        assert other.upload_status(request_ids) == []
        assert tasks.upload_status([uuid4().hex]) == []
        assert "processing" not in selected[0]
        assert selected[0]["retryable"] is False
        if valid:
            assert selected[0]["status"] == "succeeded"
            task = tasks.get(selected[0]["task_id"])
            saved, path = metadata.image_for_meme(task.payload["meme_id"])
            assert saved.id and path.is_file()
        else:
            assert selected[0]["status"] == "failed"
            assert selected[0]["error"]["error"] == "invalid_image"
            with pytest.raises(RuntimeError, match="invalid_image"):
                tasks.retry(selected[0]["task_id"])
        deadline = monotonic() + 20
        while monotonic() < deadline:
            attempts = [tasks.get(record["task_id"]) for record in records]
            if all(task.status in {"succeeded", "failed"} for task in attempts):
                break
            sleep(0.05)
        assert all(task.status == ("succeeded" if valid else "failed") for task in attempts), [task.error for task in attempts]
    finally:
        tasks._executor.shutdown(wait=True)
        tasks.shutdown()
        other.shutdown()
        with engine.begin() as connection:
            connection.execute(delete(Scope).where(Scope.id.in_([scope.scope_id for scope in scopes])))
        engine.dispose()
