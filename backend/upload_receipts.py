# 持久保存上传输入并登记普通 Task，文件处理由既有 Worker 执行。

from __future__ import annotations

import hashlib
import os
import shutil
from datetime import timedelta
from pathlib import Path
from typing import BinaryIO, Any, Callable
from uuid import UUID, uuid5

from loguru import logger
from sqlalchemy import BigInteger, cast, func, select

from backend.persistence.engine import DatabaseError
from backend.persistence.file_lock import shared_file_lock
from backend.persistence.models import Scope, ScopeContext, Task, utcnow
from backend.storage_security import validate_controlled_root


UPLOAD_TASK_TYPE = "image_upload"


class UploadReceipts:
    """按可信 scope 保存上传原始输入，Task 是接收状态与恢复的持久依据。"""

    def __init__(self, resources: Any, scope: ScopeContext, tasks: Any):
        """从数据库读取存储 namespace，准备独立于公开图片目录的接收目录。"""
        self.resources = resources
        self.scope = scope
        self.tasks = tasks
        with resources.factory() as session:
            namespace = session.scalar(select(Scope.storage_namespace).where(Scope.id == scope.scope_id))
        if namespace is None:
            raise DatabaseError("scope_not_found")
        self.namespace = namespace
        self.root = validate_controlled_root(resources.data_root / "upload-receipts" / str(namespace), create=True, writable=True)

    def path(self, receipt_id: str) -> Path:
        """按内部 UUID 取得接收文件路径，拒绝非规范标识和符号链接。"""
        identifier = UUID(receipt_id).hex
        path = self.root / f"{identifier}.upload"
        if path.is_symlink():
            raise DatabaseError("symlink_forbidden")
        return path

    def accept(self, source: BinaryIO, *, request_id: str, index: int, filename: str, options: dict[str, Any], size_bytes: int, max_bytes: int, max_pending_bytes: int, reserve: Callable[[dict[str, Any]], None]) -> dict[str, Any]:
        """流式保存单个文件后登记任务；相同请求重传必须具有相同输入。"""
        identifier = uuid5(self.namespace, f"{UUID(request_id).hex}:{index}").hex
        target = self.path(identifier)
        partial = target.with_suffix(".receiving")
        if size_bytes < 1 or size_bytes > max_bytes:
            raise DatabaseError("file_too_large" if size_bytes > max_bytes else "empty_file")
        if shutil.disk_usage(self.root).free < size_bytes + max_bytes:
            raise DatabaseError("upload_disk_capacity_exceeded")
        identity = {"receipt_id": identifier, "batch_id": UUID(request_id).hex, "filename": filename, "size_bytes": size_bytes, "options": options}
        with shared_file_lock(self.root / f"{identifier}.lock"):
            with self.resources.environment(self.scope) as environment:
                # scope 行锁只覆盖容量统计与接收记录登记，不覆盖文件传输。
                environment.uow.session.scalar(select(Scope).where(Scope.id == self.scope.scope_id).with_for_update())
                existing = environment.tasks.get(identifier)
                if existing is not None:
                    if existing.task_type != UPLOAD_TASK_TYPE or any((existing.payload or {}).get(key) != value for key, value in identity.items()):
                        raise DatabaseError("upload_request_conflict")
                    existing_payload = dict(existing.payload)
                    existing_status = existing.status
                    if existing_payload.get("input_removed") and existing_payload.get("phase") == "receiving":
                        raise DatabaseError("upload_input_expired")
                else:
                    pending = environment.uow.session.scalar(select(func.coalesce(func.sum(cast(Task.payload["size_bytes"].astext, BigInteger)), 0)).where(Task.scope_id == self.scope.scope_id, Task.task_type == UPLOAD_TASK_TYPE, Task.payload["receipt_id"].astext == Task.id, Task.payload["input_removed"].astext.is_distinct_from("true")))
                    if pending + size_bytes > max_pending_bytes:
                        raise DatabaseError("upload_pending_capacity_exceeded")
                    existing_payload = {**identity, "phase": "receiving"}
                    existing_status = "queued"
                    environment.uow.session.add(Task(id=identifier, scope_id=self.scope.scope_id, task_type=UPLOAD_TASK_TYPE, lane="upload", dedupe_key="upload:" + identifier, payload=existing_payload, available_at=utcnow() + timedelta(minutes=5), max_attempts=3))
            digest = hashlib.sha256()
            size = 0
            flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC | getattr(os, "O_NOFOLLOW", 0)
            try:
                with os.fdopen(os.open(partial, flags, 0o600), "wb") as output:
                    while chunk := source.read(1024 * 1024):
                        size += len(chunk)
                        if size > max_bytes:
                            raise DatabaseError("file_too_large")
                        digest.update(chunk)
                        output.write(chunk)
                    output.flush()
                    os.fsync(output.fileno())
                if size != size_bytes:
                    raise DatabaseError("upload_input_size_changed")
                payload = {**existing_payload, "input_digest": digest.hexdigest()}
                if existing_payload.get("input_digest") is not None:
                    if existing_payload["input_digest"] != payload["input_digest"]:
                        raise DatabaseError("upload_request_conflict")
                if existing_payload.get("phase") != "receiving":
                    status = existing_status
                else:
                    os.replace(partial, target)
                    directory = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY)
                    try:
                        os.fsync(directory)
                    finally:
                        os.close(directory)
                    with self.resources.environment(self.scope) as environment:
                        record = environment.tasks.get(identifier, for_update=True)
                        record.payload = payload
                    reserve(payload)
                    with self.resources.environment(self.scope) as environment:
                        record = environment.tasks.get(identifier, for_update=True)
                        if (record.error or {}).get("error") == "task_cancelled":
                            raise DatabaseError("task_cancelled")
                        if record.status not in {"queued", "failed"}:
                            raise DatabaseError("upload_receiving_state_changed")
                        record.payload = {**payload, "phase": "accepted"}
                        record.status = "queued"
                        record.available_at = utcnow()
                        record.error = None
                        record.completed_at = None
                    status = "queued"
            finally:
                partial.unlink(missing_ok=True)
        self.tasks.schedule(identifier)
        logger.info("upload_received task_id={} bytes={} status={}", identifier, size, status)
        return {"filename": filename, "ok": True, "upload_task_id": identifier, "status": status, "batch_id": identity["batch_id"]}

    def read(self, payload: dict[str, Any]) -> bytes:
        """在 Worker 中读取已确认保存的输入，大小不符时停止处理。"""
        path = self.path(payload["receipt_id"])
        expected = int(payload["size_bytes"])
        with path.open("rb") as source:
            content = source.read(expected + 1)
        if len(content) != expected:
            raise DatabaseError("upload_input_size_changed")
        return content
