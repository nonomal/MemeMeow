# 使用真实文件、进程和 Loguru sink 验证 Executor 失败诊断。

import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

from loguru import logger
import pytest

from executor.analysis_monitor import AnalysisControlError, AnalysisMonitor, PLUGIN_VERSION
from executor.attempt_diagnostics import AttemptOutput, configure_executor_diagnostics, stream_sample
from executor.process_supervisor import ProcessSupervisor
from executor.result_store import ExecutorResultStore, ExecutorResultStoreError
from executor.server import _redact_diagnostic


def read_record(path: Path) -> dict:
    """读取真实日志文件中最后一个失败诊断 JSON 对象。"""
    return json.loads(path.read_text().split(" ERROR ")[-1])


def test_exited_process_output_survives_monitor_exception(tmp_path):
    """真实 Python 进程执行失败后，分析检查异常仍保留其 stderr 和退出码。"""
    sink = configure_executor_diagnostics(tmp_path / "logs")
    output = AttemptOutput()
    monitor = AnalysisMonitor(
        attempt_id="test-attempt", policy={"version": 1, "termination_cost": "0.1"},
        database=tmp_path / "unused.db", directory=tmp_path,
        status_path=tmp_path / "absent.json", startup_deadline=time.monotonic() + 10,
    )
    try:
        with pytest.raises(AnalysisControlError):
            with output.capture(tmp_path, "test-attempt") as (out, err):
                process = subprocess.Popen(
                    [sys.executable, "-c", "import missing_executor_diagnostic_dependency"],
                    stdout=out, stderr=err,
                )
                assert process.wait(timeout=10) == 1
                monitor.check(None, exited=True)
        assert out.closed and err.closed
        output.log_failure(redact=_redact_diagnostic, return_code=process.returncode, status="failed")
        record = read_record(tmp_path / "logs/agent-diagnostics.log")
        assert record["return_code"] == 1
        assert "ModuleNotFoundError" in record["stderr"]
        assert "missing_executor_diagnostic_dependency" in record["stderr"]
        assert record["stderr_bytes"] > 0
    finally:
        logger.remove(sink)


def test_sample_keeps_tail_without_moving_process_offset(tmp_path):
    """读取大文件时保留末尾原因，下一次进程写入继续追加。"""
    with (tmp_path / "output").open("w+b") as stream:
        stream.write(b"begin\n" + b"x" * 100 + b"\nend")
        stream.flush()
        position = stream.tell()
        sample = stream_sample(stream, 16)
        assert sample.startswith(b"begin") and sample.endswith(b"end")
        assert stream.tell() == position
        stream.write(b"-next")
        stream.flush()
        assert (tmp_path / "output").read_bytes().endswith(b"end-next")


def test_output_redacts_credentials_and_omits_transcript(tmp_path):
    """输出包含凭据和普通消息时，日志保留原因并删除凭据及消息正文。"""
    sink = configure_executor_diagnostics(tmp_path / "logs")
    output = AttemptOutput()
    try:
        with output.capture(tmp_path, "test-redaction") as (out, err):
            out.write(json.dumps({"type": "text", "part": {"text": "private transcript"}}).encode() + b"\n")
            out.write(json.dumps({"type": "error", "error": {"name": "APIError", "data": {"message": "HTTP 503 token=private-credential", "statusCode": 503, "responseBody": "private body"}}}).encode() + b"\n")
            err.write(b"initialization failed\nauthorization: Bearer private-credential\n/runtime/private/file.py\n")
        output.log_failure(redact=lambda value: _redact_diagnostic(value, ("private-credential",)))
        content = (tmp_path / "logs/agent-diagnostics.log").read_text()
        assert "private-credential" not in content
        assert "private transcript" not in content
        assert "private body" not in content
        assert "/runtime/private" not in content
        record = read_record(tmp_path / "logs/agent-diagnostics.log")
        assert "HTTP 503" in record["stdout"]
        assert "initialization failed" in record["stderr"]
        assert record["stdout_omitted_lines"] == 1
    finally:
        logger.remove(sink)


def test_rotation_and_negative_return_code(tmp_path):
    """真实进程由 signal 终止时保存负数退出码，并执行 Loguru 文件轮转。"""
    sink = configure_executor_diagnostics(tmp_path, rotation="1 KB")
    try:
        with subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"]) as process:
            process.send_signal(signal.SIGTERM)
            assert process.wait(timeout=10) == -signal.SIGTERM
        for _ in range(5):
            AttemptOutput().log_failure(redact=_redact_diagnostic, return_code=process.returncode)
        assert len(list(tmp_path.glob("agent-diagnostics*.log"))) > 1
        assert read_record(tmp_path / "agent-diagnostics.log")["return_code"] == -signal.SIGTERM
    finally:
        logger.remove(sink)


def test_invalid_rotation_fails_at_configuration(tmp_path):
    """轮转配置非法时直接报告 Loguru 的配置错误。"""
    with pytest.raises(ValueError):
        configure_executor_diagnostics(tmp_path, rotation="invalid capacity")


def test_timeout_keeps_output_after_process_cleanup(tmp_path):
    """真实进程超时并经 supervisor 回收后，仍然能够读取关闭前的输出。"""
    output = AttemptOutput()
    with pytest.raises(subprocess.TimeoutExpired):
        with output.capture(tmp_path, "test-timeout") as (out, err):
            process = subprocess.Popen(
                [sys.executable, "-u", "-c", "import sys,time; print('process started', file=sys.stderr); time.sleep(30)"],
                stdout=out, stderr=err, start_new_session=True,
            )
            try:
                process.wait(timeout=0.5)
            finally:
                assert ProcessSupervisor().terminate(process).reaped
    assert b"process started" in output.samples["stderr"]
    assert process.returncode == -signal.SIGTERM


def test_result_missing_and_large_output_are_reported(tmp_path):
    """结果文件缺失时保存已关闭的输出，并明确报告摘要截断。"""
    output = AttemptOutput()
    sink = configure_executor_diagnostics(tmp_path / "logs")
    try:
        with output.capture(tmp_path, "test-result") as (out, err):
            err.write(b"context\n" * 6000 + b"final diagnostic\n")
        with pytest.raises(ExecutorResultStoreError) as error:
            ExecutorResultStore(tmp_path, filename="result.json.tmp", max_bytes=1024).read(tmp_path / "test-attempt/result.json.tmp", required_fields={"title"})
        output.log_failure(redact=_redact_diagnostic, error_code=error.value.code)
        record = read_record(tmp_path / "logs/agent-diagnostics.log")
        assert record["error_code"] == "agent_result_file_missing"
        assert record["stderr_truncated"] is True
        assert record["stderr_bytes"] > 32 * 1024
        assert "final diagnostic" in record["stderr"]
        assert len(record["stderr"].encode("utf-8")) <= 16 * 1024
    finally:
        logger.remove(sink)


def test_rotation_removes_expired_diagnostic_files(tmp_path):
    """轮转时由 Loguru 删除超过 14 天的本测试日志文件。"""
    expired = tmp_path / "agent-diagnostics.2000-01-01_00-00-00_000000.log"
    expired.write_text("expired diagnostic")
    old_time = time.time() - 15 * 86400
    os.utime(expired, (old_time, old_time))
    sink = configure_executor_diagnostics(tmp_path, rotation="1 B")
    try:
        AttemptOutput().log_failure(redact=_redact_diagnostic, status="failed")
        assert not expired.exists()
        assert (tmp_path / "agent-diagnostics.log").exists()
    finally:
        logger.remove(sink)


def test_monitor_keeps_plugin_initialization_reason(tmp_path):
    """分析检查读取真实状态文件后，保留插件初始化的具体错误。"""
    status_path = tmp_path / "status.json"
    status_path.write_text(json.dumps({
        "attempt_id": "test-attempt", "plugin_version": PLUGIN_VERSION, "policy_version": 1,
        "ready": False, "error": "analysis_plugin_initialization_failed",
        "error_detail": "analysis_plugin_initialize:HTTP_503:ServiceUnavailable",
    }))
    monitor = AnalysisMonitor(
        attempt_id="test-attempt", policy={"version": 1}, database=tmp_path / "unused.db",
        directory=tmp_path, status_path=status_path, startup_deadline=time.monotonic() + 10,
    )
    with pytest.raises(AnalysisControlError):
        monitor.check(None)
    assert monitor.reminder_error == "analysis_plugin_initialization_failed"
    assert monitor.reminder_error_detail == "analysis_plugin_initialize:HTTP_503:ServiceUnavailable"
