# Executor attempt 的有限输出采集与失败日志，供进程执行循环使用。

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
import json
import os
from pathlib import Path
import tempfile
from typing import Any, Callable, Iterator

from loguru import logger


def diagnostics_rotation() -> str:
    """返回部署配置的 Loguru 轮转容量；格式由 Loguru 在配置时校验。"""
    return os.environ.get("MEMEMEOW_AGENT_DIAGNOSTICS_ROTATION", "200 MB")


def configure_executor_diagnostics(root: Path, *, rotation: str | None = None) -> int:
    """为独立 Executor 添加持久化日志，返回 sink 标识供生命周期管理。"""
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    return logger.add(
        root / "agent-diagnostics.log", rotation=diagnostics_rotation() if rotation is None else rotation,
        retention="14 days", encoding="utf-8", enqueue=False, catch=False,
        format="{time:YYYY-MM-DDTHH:mm:ss.SSSZ} {level} {message}",
    )


def stream_sample(stream: Any, limit: int) -> bytes:
    """读取文件头尾的有限样本，保持子进程共享的文件读取位置不变。"""
    size = os.fstat(stream.fileno()).st_size
    if size <= 2 * limit:
        return os.pread(stream.fileno(), size, 0)
    return os.pread(stream.fileno(), limit, 0) + b"\n" + os.pread(stream.fileno(), limit, size - limit)


@dataclass
class AttemptOutput:
    """保存一个 attempt 的输出样本及字节数，在文件关闭后生成失败诊断。"""

    samples: dict[str, bytes] = field(default_factory=dict)
    sizes: dict[str, int] = field(default_factory=dict)
    phase: str = "prepare"

    @contextmanager
    def capture(self, root: Path, task_id: str) -> Iterator[tuple[Any, Any]]:
        """创建进程输出文件；任何退出路径均在关闭文件前保留头尾样本。"""
        with tempfile.TemporaryFile(dir=root, prefix=f"{task_id}-", mode="w+b") as out, tempfile.TemporaryFile(dir=root, prefix=f"{task_id}-", mode="w+b") as err:
            try:
                yield out, err
            finally:
                for name, stream, limit in (("stdout", out, 256 * 1024), ("stderr", err, 16 * 1024)):
                    stream.flush()
                    self.sizes[name] = os.fstat(stream.fileno()).st_size
                    self.samples[name] = stream_sample(stream, limit)

    def log_failure(self, *, redact: Callable[[str], str], **facts: object) -> None:
        """记录终态事实与脱敏摘要；stdout 仅提取 OpenCode error 事件。"""
        record: dict[str, object] = {"event": "executor_attempt_failure", "phase": self.phase, **facts}
        for name, limit in (("stdout", 256 * 1024), ("stderr", 16 * 1024)):
            sample = self.samples.get(name, b"")
            lines = sample.decode("utf-8", errors="replace").splitlines()
            messages: list[str] = []
            omitted = 0
            for line in lines:
                if name == "stdout":
                    try:
                        event = json.loads(line)
                    except (ValueError, RecursionError):
                        omitted += 1
                        continue
                    if not isinstance(event, dict) or event.get("type") != "error":
                        omitted += 1
                        continue
                    error = event.get("error")
                    if isinstance(error, dict):
                        data = error.get("data")
                        # 不保存 provider 响应正文或请求参数，只保留可诊断字段。
                        detail = {key: error[key] for key in ("name", "code", "message") if key in error}
                        if isinstance(data, dict):
                            detail.update({key: data[key] for key in ("message", "statusCode", "code") if key in data})
                        line = json.dumps(detail, ensure_ascii=False)
                    else:
                        line = str(error)
                messages.append(redact(line))
            summary = "\n".join(messages)
            encoded = summary.encode("utf-8")
            summary_limit = 16 * 1024
            if len(encoded) > summary_limit:
                marker = b"\n[TRUNCATED]\n"
                half = (summary_limit - len(marker)) // 2
                summary = encoded[:half].decode("utf-8", errors="ignore") + marker.decode() + encoded[-half:].decode("utf-8", errors="ignore")
            record[name] = summary
            record[f"{name}_bytes"] = self.sizes.get(name, 0)
            record[f"{name}_truncated"] = self.sizes.get(name, 0) > 2 * limit or len(encoded) > summary_limit or any(len(line) > 500 for line in lines)
            if name == "stdout":
                record["stdout_omitted_lines"] = omitted
        # JSON 转义换行，保证一个 attempt 的诊断始终是一条日志。
        logger.bind(component="executor").error("{}", json.dumps(record, ensure_ascii=False, default=str))
