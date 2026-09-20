# 验证图片 attempt 诊断字段在真实 PostgreSQL 中的空值和受控对象写入。

import os
from uuid import uuid4

import pytest
from sqlalchemy import JSON, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from backend.database import create_engine_for_url
from backend.persistence.models import ImageProcessingAttempt, Scope, Task


@pytest.fixture
def diagnostic_session():
    """为诊断回归提供真实事务，测试结束仅撤销本事务的数据。"""
    url = os.environ.get("MEMEMEOW_DATABASE_URL")
    if not url:
        pytest.skip("需要显式配置 MEMEMEOW_DATABASE_URL")
    engine = create_engine_for_url(url)
    try:
        with engine.connect() as connection, connection.begin() as transaction:
            with Session(connection, join_transaction_mode="create_savepoint") as session:
                yield session
            transaction.rollback()
    finally:
        engine.dispose()


def visual_attempt(session: Session, diagnostic) -> ImageProcessingAttempt:
    """创建本次测试专属视觉 Task，返回明确携带诊断参数的 prepared attempt。"""
    identifier = f"test-attempt-diagnostic-{uuid4().hex}"
    session.add(Scope(id=identifier))
    session.flush()
    session.add(Task(id=identifier, scope_id=identifier, task_type="visual_embedding_generation"))
    session.flush()
    return ImageProcessingAttempt(
        scope_id=identifier,
        task_id=identifier,
        attempt=1,
        attempt_id=identifier,
        stage="visual",
        state="prepared",
        input_digest="a" * 64,
        target_sha256="b" * 64,
        claim_generation=1,
        analysis_diagnostic=diagnostic,
    )


@pytest.mark.parametrize("reason", [
    None,
    "agent_analysis_usage_unavailable",
    "agent_analysis_reminder_plugin_unavailable",
    "agent_maximum_analysis_depth_exceeded",
])
def test_visual_attempt_diagnostic_round_trip(diagnostic_session: Session, reason: str | None):
    """普通视觉 attempt 保存 SQL NULL，三个合法诊断对象可写入并清除。"""
    diagnostic = {"check_stage": "analysis_monitor", "trigger_reason": reason} if reason else None
    row = visual_attempt(diagnostic_session, diagnostic)
    diagnostic_session.add(row)
    diagnostic_session.flush()
    diagnostic_session.refresh(row)
    assert row.analysis_diagnostic == diagnostic
    row.analysis_diagnostic = None
    diagnostic_session.flush()
    assert diagnostic_session.scalar(select(ImageProcessingAttempt.analysis_diagnostic.is_(None)).where(
        ImageProcessingAttempt.attempt_id == row.attempt_id,
    )) is True


@pytest.mark.parametrize("diagnostic", [JSON.NULL, {"check_stage": "analysis_monitor", "trigger_reason": "invalid"}])
def test_attempt_diagnostic_rejects_invalid_json(diagnostic_session: Session, diagnostic):
    """数据库约束拒绝 JSON null 和未定义的诊断对象，并提供明确约束名称。"""
    row = visual_attempt(diagnostic_session, diagnostic)
    with diagnostic_session.begin_nested():
        diagnostic_session.add(row)
        with pytest.raises(IntegrityError) as caught:
            diagnostic_session.flush()
        assert caught.value.orig.diag.constraint_name == "ck_attempt_analysis_diagnostic"
