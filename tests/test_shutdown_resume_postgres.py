# 在现有开发 PostgreSQL 中验证停机续跑状态转换，仅创建独立测试 scope。

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
import os
from uuid import uuid4

import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import make_url

from backend.agent_resume import classify_resume_error
from backend.persistence.models import Scope, ScopeContext, Task
from backend.persistence.resources import DatabaseResources
from backend.persistence.repositories.tasks import TaskRepository


@pytest.fixture
def shutdown_case(tmp_path):
    """连接明确指定的开发数据库，测试记录使用独立 lane 防止后台认领。"""
    url = os.environ.get("MEMEMEOW_DATABASE_URL")
    if not url:
        pytest.skip("需要显式指定现有开发 PostgreSQL")
    parsed = make_url(url)
    assert parsed.host in {"127.0.0.1", "localhost"} and parsed.port in {5434, 25434}
    engine = create_engine(url, pool_size=4, max_overflow=0)
    resources = DatabaseResources(engine, image_root=tmp_path / "images", data_root=tmp_path / "data")
    scope = ScopeContext(f"shutdown-test-{uuid4().hex}")
    task_id = f"shutdown-test-{uuid4().hex}"
    with resources.factory.begin() as session:
        session.add(Scope(id=scope.scope_id))
        session.flush()
        session.add(Task(
            id=task_id, scope_id=scope.scope_id, task_type="meme_context_generation",
            status="running", lane="shutdown-verification", payload={},
            lease_owner="shutdown-test", lease_expires_at=datetime.now(UTC) + timedelta(hours=1),
            claim_generation=1, attempt_count=1, max_attempts=1,
        ))
    try:
        yield resources, scope, task_id
    finally:
        with resources.factory.begin() as session:
            TaskRepository(session, scope).cancel(task_id, error={"error": "test_finished"}, message="停机测试结束")
        engine.dispose()


def requeue(case, *, available=True, budget=4):
    """通过真实 repository 提交停机错误和已经验证的恢复标识。"""
    resources, scope, task_id = case
    with resources.factory.begin() as session:
        return TaskRepository(session, scope).fail_fenced(
            task_id, 1, "shutdown-test", error={"error": "service_shutdown"},
            message="等待服务启动后继续", retry=available,
            resume_available=available, resume_reason="service_shutdown",
            session_id="session-shutdown-test" if available else None,
            executor_attempt_id="attempt-shutdown-test", resume_max_attempts=budget,
            retry_delay_seconds=3600,
        )


def test_shutdown_requeues_single_attempt_task(shutdown_case):
    """停机使用独立续跑预算，保留 Task 身份、max_attempts 和错误原因。"""
    assert requeue(shutdown_case) == (True, True)
    resources, _, task_id = shutdown_case
    with resources.factory() as session:
        task = session.get(Task, task_id)
        assert task.status == "queued" and task.max_attempts == 1
        assert task.resume_session_id == "session-shutdown-test"
        assert task.error["error"] == "service_shutdown"
        assert task.lease_owner is None and task.resume_started_at is not None


@pytest.mark.parametrize("available,budget", [(False, 4), (True, 0)])
def test_shutdown_without_session_or_budget_finishes(shutdown_case, available, budget):
    """session 缺失或预算耗尽时终止 Task，禁止重新开始外部执行。"""
    assert requeue(shutdown_case, available=available, budget=budget) == (True, False)
    resources, _, task_id = shutdown_case
    with resources.factory() as session:
        assert session.get(Task, task_id).status == "failed"


@pytest.mark.parametrize("success", [False, True])
def test_shutdown_cannot_replace_committed_terminal_state(shutdown_case, success):
    """停机线程的迟到写回不能改变已经提交的成功或用户取消结果。"""
    resources, scope, task_id = shutdown_case
    with resources.factory.begin() as session:
        repository = TaskRepository(session, scope)
        if success:
            assert repository.update_fenced(task_id, 1, "shutdown-test", status="succeeded", result={})
        else:
            assert repository.cancel(task_id, error={"error": "task_cancelled"}, message="用户取消")
    assert requeue(shutdown_case) == (False, False)
    with resources.factory() as session:
        task = session.get(Task, task_id)
        assert task.status == ("succeeded" if success else "failed")
        if not success:
            assert task.error["error"] == "task_cancelled"


def test_concurrent_shutdown_write_has_one_winner(shutdown_case):
    """并发执行相同 claim 的停机写回，只有一个事务能够重新排队。"""
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(requeue, [shutdown_case, shutdown_case]))
    assert sorted(results) == [(False, False), (True, True)]


def test_shutdown_error_requires_session_and_preserves_cancellation():
    """正常停机可继续已有 session，用户取消与保存失败均禁止续跑。"""
    assert classify_resume_error("service_shutdown", session_id="session-test").available
    assert classify_resume_error("service_shutdown", session_id=None).reason == "session_missing"
    for code in ("task_interrupted", "agent_attempt_metadata_write_failed"):
        assert not classify_resume_error(code, session_id="session-test").available
