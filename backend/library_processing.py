# 图片库后台扫描：通过普通 Task 保存枚举范围、游标与提交统计。

from __future__ import annotations

from typing import Any, Callable
from datetime import datetime
from uuid import UUID

from loguru import logger
from sqlalchemy import select

from backend.image_processing import ImageProcessingError
from backend.persistence.models import Meme, utcnow


LIBRARY_TASK_TYPE = "image_library_processing"


def submit_library_processing(tasks: Any, options: dict[str, Any]) -> Any:
    """固定当前图片范围后提交扫描任务，返回可查询的 Task 快照。"""
    accepted_at = utcnow()
    with tasks.resources.environment(tasks.scope) as environment:
        upper_id = environment.uow.session.scalar(
            select(Meme.id).where(Meme.scope_id == tasks.scope.scope_id).order_by(Meme.id.desc()).limit(1)
        )
    return tasks.submit(LIBRARY_TASK_TYPE, {"options": options, "accepted_at": accepted_at.isoformat(), "upper_id": str(upper_id) if upper_id else None})


def run_library_processing(tasks: Any, payload: dict[str, Any], process_image: Callable[[Any, dict[str, Any]], str], progress: Callable[..., Any], *, page_size: int = 100) -> dict[str, int]:
    """分页提交图片处理，逐图保存游标；租约失效立即停止当前执行者。"""
    resources = tasks.resources
    counts = dict(payload.get("counts") or {"scanned_count": 0, "submitted_count": 0, "conflict_count": 0, "not_needed_count": 0})
    upper_id = UUID(payload["upper_id"]) if payload.get("upper_id") else None
    cursor = UUID(payload["cursor"]) if payload.get("cursor") else None
    accepted_at = datetime.fromisoformat(payload["accepted_at"])
    if upper_id is None:
        return counts
    while True:
        with resources.environment(tasks.scope) as environment:
            statement = select(Meme).where(Meme.scope_id == tasks.scope.scope_id, Meme.id <= upper_id, Meme.created_at <= accepted_at)
            if cursor is not None:
                statement = statement.where(Meme.id > cursor)
            rows = list(environment.uow.session.scalars(statement.order_by(Meme.id).limit(page_size)))
        if not rows:
            return counts
        for record in rows:
            if tasks.stopping:
                raise RuntimeError("task_interrupted")
            # 检查与保存都使用租约条件；图片操作期间不持有数据库事务。
            with resources.environment(tasks.scope) as environment:
                current = environment.tasks.update_payload_fenced(payload["_claim_task_id"], payload["_claim_generation"], payload["_claim_owner"], {})
            if not current:
                raise RuntimeError("task_claim_expired")
            try:
                category = process_image(record, payload["options"])
            except ImageProcessingError as exc:
                if exc.code == "image_processing_active":
                    category = "conflict_count"
                elif exc.code == "already_ready":
                    category = "not_needed_count"
                else:
                    logger.error("library_processing_error task_id={} meme_id={} error={}", payload["_claim_task_id"], record.id, exc.code)
                    raise
            counts[category] += 1
            counts["scanned_count"] += 1
            cursor = record.id
            with resources.environment(tasks.scope) as environment:
                saved = environment.tasks.update_payload_fenced(payload["_claim_task_id"], payload["_claim_generation"], payload["_claim_owner"], {"cursor": str(cursor), "counts": counts})
            if not saved:
                raise RuntimeError("task_claim_expired")
        progress(None, f"已检查 {counts['scanned_count']} 张图片，已提交 {counts['submitted_count']} 张")
        logger.info("library_processing_page task_id={} scanned={} submitted={}", payload["_claim_task_id"], counts["scanned_count"], counts["submitted_count"])
