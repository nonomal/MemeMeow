# 检查宿主预留期限在任务持久化边界的判定，覆盖到期瞬间及时间区域。

from datetime import datetime, timedelta, timezone

import pytest

from backend.persistence.engine import DatabaseError
from backend.task_deadline import ensure_task_deadline
from backend.services.tasks import PostgresTaskService


def test_reservation_deadline_boundary() -> None:
    """到期前允许提交，到期瞬间及之后拒绝提交。"""
    deadline = datetime(2026, 9, 25, tzinfo=timezone.utc)
    payload = {"agent_reservation_expires_at": deadline.isoformat()}
    ensure_task_deadline(payload, deadline - timedelta(microseconds=1))
    for now in (deadline, deadline + timedelta(seconds=1)):
        with pytest.raises(DatabaseError, match="agent_reservation_expired"):
            ensure_task_deadline(payload, now)


def test_reservation_deadline_timezone() -> None:
    """带不同时间区域的时刻按同一绝对期限判断。"""
    payload = {"agent_reservation_expires_at": "2026-09-25T08:00:00+08:00"}
    with pytest.raises(DatabaseError, match="agent_reservation_expired"):
        ensure_task_deadline(payload, datetime(2026, 9, 25, tzinfo=timezone.utc))


def test_reservation_deadline_requires_timezone() -> None:
    """宿主提供缺少时间区域的期限时明确报错。"""
    with pytest.raises(DatabaseError, match="agent_reservation_deadline_invalid"):
        ensure_task_deadline(
            {"agent_reservation_expires_at": "2026-09-25T00:00:00"},
            datetime(2026, 9, 25, tzinfo=timezone.utc),
        )


def test_task_without_reservation_deadline() -> None:
    """没有宿主预留期限的任务可以使用原有执行条件。"""
    ensure_task_deadline({}, datetime.now(timezone.utc))


def test_reservation_deadline_preserves_resume_input_digest() -> None:
    """执行准备阶段补充预留期限后，自动续跑仍识别同一业务输入。"""
    payload = {"meme_id": "test-image", "image_sha256": "a" * 64}
    before = PostgresTaskService._image_attempt_input_digest(payload)
    after = PostgresTaskService._image_attempt_input_digest({
        **payload, "agent_reservation_expires_at": "2026-09-25T00:00:00+00:00",
    })
    assert before == after
