# 任务持久化边界共用的执行期限校验，期限由宿主策略写入可信 Task payload。

from datetime import datetime
from typing import Any, Mapping

from backend.persistence.engine import DatabaseError


def ensure_task_deadline(payload: Mapping[str, Any], now: datetime) -> None:
    """在语境保存和任务成功提交前检查宿主给出的绝对期限。"""
    value = payload.get("agent_reservation_expires_at")
    if value is None:
        return
    deadline = datetime.fromisoformat(value)
    if deadline.tzinfo is None:
        raise DatabaseError("agent_reservation_deadline_invalid")
    if now >= deadline:
        raise DatabaseError("agent_reservation_expired")
