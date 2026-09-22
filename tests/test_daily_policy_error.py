from datetime import UTC, datetime

import pytest

from backend.operation_policy import OperationPolicyError, PolicyDecision, require_allowed


def test_daily_limit_preserves_reason_and_reset_time() -> None:
    """日限额拒绝保留公开原因和恢复时间，供上传与能力查询使用。"""
    reset_at = datetime(2026, 9, 22, 16, tzinfo=UTC)
    with pytest.raises(OperationPolicyError) as caught:
        require_allowed(PolicyDecision(False, "operation_daily_limit_exceeded", reset_at))
    assert caught.value.payload()["error"] == "operation_daily_limit_exceeded"
    assert caught.value.payload()["retry_at"] == reset_at.isoformat()
