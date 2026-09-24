# 通过真实 PostgreSQL、图片文件和 Task 执行验证扫描游标与恢复。

from io import BytesIO
from uuid import uuid4

from dotenv import dotenv_values
from PIL import Image
from sqlalchemy import delete

from backend.image_processing import ImageProcessingRepository
from backend.library_processing import LIBRARY_TASK_TYPE, run_library_processing
from backend.persistence.engine import create_engine_for_url
from backend.persistence.models import Scope, ScopeContext, utcnow
from backend.persistence.resources import DatabaseResources
from backend.persistence.storage import StorageCoordinator
from backend.services.tasks import PostgresTaskService


def test_library_scan_persists_cursor_and_job_submissions(tmp_path):
    """真实 Task 分页登记三个图片 Job，并保留能够继续执行的游标与统计。"""
    engine = create_engine_for_url(dotenv_values(".env")["MEMEMEOW_DATABASE_URL"])
    scope = ScopeContext("test-library-scan-" + uuid4().hex)
    with engine.begin() as connection:
        connection.execute(Scope.__table__.insert().values(id=scope.scope_id))
    resources = DatabaseResources(engine, image_root=tmp_path / "images", data_root=tmp_path / "data", require_local_scope=False)
    storage = StorageCoordinator(resources, scope_id=scope)
    jobs = ImageProcessingRepository(resources, scope)
    tasks = PostgresTaskService(resources, scope_id=scope)
    identifiers = []
    try:
        for color in ("red", "green", "blue"):
            content = BytesIO()
            Image.new("RGB", (32, 32), color).save(content, format="PNG")
            identifiers.append(storage.upload(content.getvalue(), extension=".png", context={}, provenance={}).id)

        def submit_image(record, options):
            """使用图片任务仓储登记真实 Job，作为扫描器的业务提交操作。"""
            jobs.create_or_reuse(record.id, record.sha256, **options)
            return "submitted_count"

        def handler(payload, progress):
            """通过真实执行器提供的 claim 保存扫描进度。"""
            return run_library_processing(tasks, payload, submit_image, progress, page_size=2)

        tasks.register(LIBRARY_TASK_TYPE, handler)
        task = tasks.submit(LIBRARY_TASK_TYPE, {"upper_id": str(max(identifiers)), "accepted_at": utcnow().isoformat(), "options": {}}, schedule=False)
        tasks._run(task.task_id)
        finished = tasks.get(task.task_id)
        assert finished.status == "succeeded", finished.error
        assert finished.result["scanned_count"] == 3
        assert finished.result["submitted_count"] == 3
        assert finished.payload["cursor"] == str(max(identifiers))
        assert len(jobs.active_ids(limit=10)) == 3
        # 用已经保存的输入执行下一次任务，完成游标不会再次产生提交。
        resumed = tasks.submit(LIBRARY_TASK_TYPE, finished.payload, schedule=False)
        tasks._run(resumed.task_id)
        assert tasks.get(resumed.task_id).result == finished.result
        assert len(jobs.active_ids(limit=10)) == 3
    finally:
        tasks.shutdown()
        with engine.begin() as connection:
            connection.execute(delete(Scope).where(Scope.id == scope.scope_id))
        engine.dispose()
