# 验证宿主公开故障经过授权检查、记录重放和 HTTP 投影后仍保留原因。

import json

import pytest
from fastapi import HTTPException
from loguru import logger

from backend.database import ReverseImageUsageEvent
from backend.operation_diagnostics import OperationDiagnostics
from backend.operation_policy import OperationPolicyError, PolicyDecision, PolicyFailure, require_allowed
from backend.reverse_image import ReverseImageError, ReverseImageService
from backend.reverse_image_http import _policy_failure_http_error


def _http_error(status, code, message):
    """使用 FastAPI 的实际错误对象提供宿主 HTTP 格式。"""
    return HTTPException(status, {"error": code, "message": message})


def test_policy_failure_survives_replay_and_http_projection():
    """验证真实领域类型经过 JSON 保存格式后，首次与重放响应保持原因一致。"""
    failure = PolicyFailure("quota_catalog_unknown", "额度目录版本未定义")
    with pytest.raises(OperationPolicyError) as denied:
        require_allowed(PolicyDecision(False, "operation_policy_unavailable", failure=failure))
    first = ReverseImageError(denied.value.code, str(denied.value), retryable=True, status_code=503, failure=denied.value.failure)
    event = ReverseImageUsageEvent(outcome="failed", provider_called=False, retryable=True, error=json.loads(json.dumps(first.payload())))
    with pytest.raises(ReverseImageError) as replay:
        ReverseImageService._event_output(event)
    initial_http = _policy_failure_http_error(first, error=_http_error)
    replay_http = _policy_failure_http_error(replay.value, error=_http_error)
    assert initial_http.status_code == replay_http.status_code == 503
    assert initial_http.detail == replay_http.detail == {
        "error": "quota_catalog_unknown", "message": "额度目录版本未定义",
        "stage": "quota", "policy_error": "operation_policy_unavailable",
    }


def test_policy_failure_keeps_original_and_final_stages():
    """错误转换后诊断保留首次阶段和公开原因，最终阶段记录保存处理。"""
    messages = []
    sink = logger.add(messages.append, format="{message}")
    try:
        with pytest.raises(ReverseImageError):
            with OperationDiagnostics("policy_transport_test", phase="quota") as diagnostics:
                try:
                    require_allowed(PolicyDecision(False, "operation_policy_unavailable", failure=PolicyFailure("quota_catalog_unknown", "额度目录版本未定义")))
                except OperationPolicyError as exc:
                    diagnostics.failure(exc)
                    diagnostics.phase("persist")
                    raise ReverseImageError(exc.code, str(exc), failure=exc.failure) from exc
    finally:
        logger.remove(sink)
    fields = json.loads(messages[0].record["message"].partition(" ")[2])
    assert fields["policy_error_code"] == "quota_catalog_unknown"
    assert fields["error_stage"] == fields["policy_error_stage"] == "quota"
    assert fields["final_error_stage"] == "persist"


def test_historical_policy_failure_replay_stays_failed():
    """历史记录没有具体原因时仍返回失败，不能把重放当作成功。"""
    event = ReverseImageUsageEvent(outcome="failed", provider_called=False, retryable=True, error={"error": "operation_policy_unavailable"})
    with pytest.raises(ReverseImageError) as replay:
        ReverseImageService._event_output(event)
    assert replay.value.code == "operation_policy_unavailable"
    assert replay.value.failure is None
    assert replay.value.status_code == 503
