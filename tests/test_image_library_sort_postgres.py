# 图片库排序的真实 PostgreSQL 验收；只连接明确指定的开发测试数据库。
import os
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.orm import Session

from backend.persistence.engine import DatabaseError, create_engine_for_url
from backend.persistence.models import Meme, Scope, ScopeContext
from backend.persistence.repositories.memes import MemeRepository


@pytest.fixture
def sorting_session():
    """使用显式数据库地址创建事务，结束后清理本次测试的全部记录。"""
    url = os.environ.get("MEMEMEOW_TEST_DATABASE_URL")
    if not url:
        pytest.skip("需要显式设置 MEMEMEOW_TEST_DATABASE_URL")
    engine = create_engine_for_url(url)
    try:
        with engine.connect() as connection, connection.begin() as transaction:
            with Session(bind=connection) as session:
                yield session
            transaction.rollback()
    finally:
        engine.dispose()


def test_library_sort_pagination_and_scope(sorting_session: Session) -> None:
    """验证大小写、同值排序、分页、名称筛选、跨 scope 隔离和重命名更新时间。"""
    session = sorting_session
    scope = Scope(id=f"test-library-sort-{uuid4().hex}")
    other = Scope(id=f"test-library-sort-other-{uuid4().hex}")
    session.add_all([scope, other])
    session.flush()
    baseline = datetime(2025, 1, 1, tzinfo=timezone.utc)
    records = []
    for index, (name, day) in enumerate([("zebra", 3), ("Alpha", 1), ("alpha", 1), ("Beta", 2)], start=1):
        sha = f"{index:064x}"
        record = Meme(
            id=UUID(int=index), scope_id=scope.id, display_name=name,
            storage_key=f"{sha}.png", extension=".png", sha256=sha, size_bytes=1,
            created_at=baseline, updated_at=baseline + timedelta(days=day),
        )
        records.append(record)
        session.add(record)
    sha = "f" * 64
    session.add(Meme(scope_id=other.id, display_name="AAA", storage_key=f"{sha}.png", extension=".png", sha256=sha, size_bytes=1))
    session.flush()
    repository = MemeRepository(session, ScopeContext(scope.id))
    expected = {
        "name_asc": [records[1], records[2], records[3], records[0]],
        "updated_desc": [records[0], records[3], records[2], records[1]],
    }
    for sort, ordered in expected.items():
        first = repository.list(sort=sort, page_size=2)
        second = repository.list(sort=sort, page=2, page_size=2)
        assert [row.id for row in first + second] == [row.id for row in ordered]
        assert repository.list(sort=sort, page=3, page_size=2) == []
        assert repository.count() == 4
    assert [row.id for row in repository.list(search="ALPHA", sort="name_asc")] == [records[1].id, records[2].id]
    assert [row.id for row in repository.list(search="ALPHA", sort="updated_desc")] == [records[2].id, records[1].id]
    assert repository.count(search="ALPHA") == 2
    repository.update_display_name(records[1].id, "renamed")
    assert repository.list(sort="updated_desc")[0].id == records[1].id
    with pytest.raises(DatabaseError, match="图片排序方式必须"):
        repository.list(sort="unknown")


def test_library_sort_indexes(sorting_session: Session) -> None:
    """确认数据库已经完成两种排序所需的索引迁移。"""
    indexes = dict(sorting_session.execute(text(
        "SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = current_schema() "
        "AND tablename = 'memes' AND indexname IN ('ix_memes_scope_name_lower', 'ix_memes_scope_updated')"
    )).all())
    assert "lower" in indexes["ix_memes_scope_name_lower"]
    assert "updated_at DESC, id DESC" in indexes["ix_memes_scope_updated"]
