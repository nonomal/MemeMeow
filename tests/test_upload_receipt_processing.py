# 使用真实文件、图片解码、PostgreSQL 和 Task 执行验证上传接收与后台登记。

from io import BytesIO
from time import monotonic, sleep
from uuid import uuid4

from dotenv import dotenv_values
from PIL import Image
from sqlalchemy import delete, select, func
import pytest

from backend.image_processing import ImageProcessingRepository
from backend.operation_policy import AllowAllOperationPolicy, OperationPolicyGateway, PersistentGrantAssociationStore
from backend.persistence.engine import create_engine_for_url
from backend.persistence.models import Scope, ScopeContext, OperationGrant
from backend.persistence.resources import DatabaseResources
from backend.scope import ScopeServices
from backend.services.metadata import PostgresMetadataService
from backend.services.tasks import PostgresTaskService
from backend.services.thumbnails import DerivedThumbnailService
from backend.upload_authorization import UploadAuthorization
from backend.upload_processing import prepare_upload, run_upload_processing, reconcile_upload
from backend.upload_receipts import UPLOAD_TASK_TYPE, UploadReceipts


@pytest.mark.parametrize("scenario", ["normal", "retry", "receiving", "incomplete", "invalid"])
def test_upload_receipt_registers_image_and_recovers_response(tmp_path, scenario):
    """接收真实 PNG 后由后台登记图片，重复请求保留同一任务与图片。"""
    engine = create_engine_for_url(dotenv_values(".env")["MEMEMEOW_DATABASE_URL"])
    scope = ScopeContext("test-upload-receipt-" + uuid4().hex)
    with engine.begin() as connection:
        connection.execute(Scope.__table__.insert().values(id=scope.scope_id))
    resources = DatabaseResources(engine, image_root=tmp_path / "images", data_root=tmp_path / "data", require_local_scope=False)
    tasks = PostgresTaskService(resources, scope_id=scope)
    metadata = PostgresMetadataService(resources, scope_id=scope.scope_id)
    thumbnails = DerivedThumbnailService(resources, scope_id=scope, task_service=tasks)
    services = ScopeServices(scope, metadata=metadata, tasks=tasks, search=None, reverse_image=None, visual_search=None, thumbnails=thumbnails)
    jobs = ImageProcessingRepository(resources, scope)
    gateway = OperationPolicyGateway(AllowAllOperationPolicy())
    store = PersistentGrantAssociationStore(resources)

    def release(grant):
        """执行真实本地授权释放并保存关联状态。"""
        result = gateway.release(grant)
        assert result.ok
        assert store.transition(grant, "released")

    authorization = UploadAuthorization(scope, gateway, store, release=release)

    def submit_processing(record, image, options):
        """真实登记后续图片 Job；本测试验收范围截至处理提交。"""
        job = jobs.create_or_reuse(record.id, record.sha256, **options)
        return jobs.snapshot(job.id)

    def handler(payload, progress):
        """通过真实 Worker 运行图片登记与授权结算。"""
        return run_upload_processing(services, payload, progress, prepare=prepare_upload, reserve=authorization.reserve, commit=authorization.finish, submit_processing=submit_processing)

    def thumbnail_handler(payload, progress):
        """执行真实缩略图生成并返回派生结果。"""
        return thumbnails.generate(payload["meme_id"])

    tasks.register(UPLOAD_TASK_TYPE, handler)
    tasks.register(thumbnails.TASK_TYPE, thumbnail_handler)
    receiver_tasks = PostgresTaskService(resources, scope_id=scope)
    receiver_tasks.shutdown()
    receipts = UploadReceipts(resources, scope, receiver_tasks)
    image = BytesIO()
    Image.new("RGB", (32, 32), "blue").save(image, format="PNG")
    content = image.getvalue()
    if scenario == "invalid":
        content = b"invalid uploaded image"
    request_id = uuid4().hex
    arguments = dict(request_id=request_id, index=0, filename="test-upload.png", options={"reverse_image_policy": "forbid", "auto_name": False}, size_bytes=len(content), max_bytes=1024 * 1024, max_pending_bytes=1024 * 1024, reserve=authorization.reserve)
    try:
        accepted = receipts.accept(BytesIO(content), **arguments)
        if scenario in {"receiving", "incomplete"}:
            with resources.environment(scope) as environment:
                row = environment.tasks.get(accepted["upload_task_id"], for_update=True)
                row.payload = {key: value for key, value in row.payload.items() if key != "input_digest"}
                row.payload = {**row.payload, "phase": "receiving"}
            if scenario == "incomplete":
                receipts.path(accepted["upload_task_id"]).unlink()
        tasks.schedule(accepted["upload_task_id"])
        deadline = monotonic() + 15
        while monotonic() < deadline:
            task = tasks.get(accepted["upload_task_id"])
            if task.status in {"succeeded", "failed"}:
                break
            sleep(0.02)
        if scenario in {"invalid", "incomplete"}:
            assert task.status == "failed", task.error
            assert not jobs.active_ids(limit=10)
            reconcile_upload(services, task.task_id, authorization)
            if scenario == "incomplete":
                assert tasks.get(task.task_id).payload["input_removed"] is True
            return
        assert task.status == "succeeded", task.error
        assert task.result["processing_job_id"]
        repeated = receipts.accept(BytesIO(content), **arguments)
        assert repeated["upload_task_id"] == accepted["upload_task_id"]
        assert len(jobs.active_ids(limit=10)) == 1
        record, _path = metadata.image_for_meme(task.result["meme_id"])
        assert record.provenance["upload_receipt_id"] == accepted["upload_task_id"]
        if scenario == "retry":
            # 保存图片后任务终态写入中断时，持久状态需要允许显式重试。
            with resources.environment(scope) as environment:
                row = environment.tasks.get(task.task_id, for_update=True)
                row.status = "failed"
                row.error = {"error": "task_interrupted"}
            retry = tasks.retry(task.task_id)
            deadline = monotonic() + 15
            while monotonic() < deadline:
                retried = tasks.get(retry.task_id)
                if retried.status in {"succeeded", "failed"}:
                    break
                sleep(0.02)
            assert retried.status == "succeeded", retried.error
            assert retried.result["meme_id"] == task.result["meme_id"]
            assert len(jobs.active_ids(limit=10)) == 1
            with resources.factory() as session:
                assert session.scalar(select(func.count()).select_from(OperationGrant).where(OperationGrant.scope_id == scope.scope_id)) == 1
            reconcile_upload(services, retried.task_id, authorization)
        reconcile_upload(services, task.task_id, authorization)
        assert not receipts.path(task.task_id).exists()
        assert tasks.get(task.task_id).payload["input_removed"] is True
    finally:
        tasks._executor.shutdown(wait=True)
        tasks.shutdown()
        with engine.begin() as connection:
            connection.execute(delete(Scope).where(Scope.id == scope.scope_id))
        engine.dispose()
