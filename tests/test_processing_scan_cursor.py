# 使用真实图片与数据库验证协调游标覆盖超过单页的活动 Job。

from io import BytesIO
from uuid import uuid4

from dotenv import dotenv_values
from PIL import Image
from sqlalchemy import delete

from backend.image_processing import ImageProcessingRepository
from backend.persistence.engine import create_engine_for_url
from backend.persistence.models import Scope, ScopeContext
from backend.persistence.resources import DatabaseResources
from backend.persistence.storage import StorageCoordinator


def test_active_job_cursor_visits_every_page(tmp_path):
    """三张真实图片产生三个 Job，分页读取完整覆盖并能重新开始扫描。"""
    engine = create_engine_for_url(dotenv_values(".env")["MEMEMEOW_DATABASE_URL"])
    scope = ScopeContext("test-scan-" + uuid4().hex)
    with engine.begin() as connection:
        connection.execute(Scope.__table__.insert().values(id=scope.scope_id))
    resources = DatabaseResources(engine, image_root=tmp_path / "images", data_root=tmp_path / "data", require_local_scope=False)
    repository = ImageProcessingRepository(resources, scope)
    storage = StorageCoordinator(resources, scope_id=scope)
    identifiers = set()
    try:
        for color in ("red", "green", "blue"):
            content = BytesIO()
            Image.new("RGB", (32, 32), color).save(content, format="PNG")
            meme = storage.upload(content.getvalue(), extension=".png", context={}, provenance={})
            identifiers.add(repository.create_or_reuse(meme.id, meme.sha256).id)
        first = repository.active_ids(limit=2)
        second = repository.active_ids(after_id=first[-1], limit=2)
        assert len(first) == 2 and len(second) == 1
        assert set(first + second) == identifiers
        assert repository.active_ids(after_id=second[-1], limit=2) == []
        assert repository.active_ids(limit=2) == first
    finally:
        with engine.begin() as connection:
            connection.execute(delete(Scope).where(Scope.id == scope.scope_id))
        engine.dispose()
