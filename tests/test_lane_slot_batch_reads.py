# 使用真实数据库验证调度槽位初始化的查询数量和已有租约保护。

from uuid import uuid4

from dotenv import dotenv_values
from sqlalchemy import delete, event, select
from sqlalchemy.orm import Session

from backend.persistence.engine import create_engine_for_url
from backend.persistence.models import Scope, ScopeContext, TaskLaneSlot, TaskLaneResourceSlot
from backend.persistence.repositories.tasks import TaskRepository


def test_slot_initialization_queries_are_bounded():
    """容量增加到五百时仍批量读取槽位，重复初始化保留现有租约。"""
    engine = create_engine_for_url(dotenv_values(".env")["MEMEMEOW_DATABASE_URL"])
    lane = "test-lane-" + uuid4().hex
    scope_id = "test-slot-" + uuid4().hex
    reads = []

    def record_statement(connection, cursor, statement, parameters, context, executemany):
        """观察实际 SQL 执行，不替换数据库调用。"""
        if context.compiled is not None and context.compiled.statement.is_select:
            reads.append(context.compiled.statement)

    try:
        with engine.begin() as connection:
            connection.execute(Scope.__table__.insert().values(id=scope_id))
        with Session(engine) as session:
            repository = TaskRepository(session, ScopeContext(scope_id))
            repository._lock_lane(lane)
            repository._ensure_lane_slots(lane, 500)
            repository._ensure_lane_resource_slots(lane, "test-resource", 500)
            slot = session.get(TaskLaneSlot, (lane, 0))
            slot.lease_owner = "test-existing-owner"
            session.commit()
        event.listen(engine, "before_cursor_execute", record_statement)
        with Session(engine) as session:
            repository = TaskRepository(session, ScopeContext(scope_id))
            repository._lock_lane(lane)
            repository._ensure_lane_slots(lane, 500)
            repository._ensure_lane_resource_slots(lane, "test-resource", 500)
            session.commit()
        assert len(reads) == 3
        event.remove(engine, "before_cursor_execute", record_statement)
        with Session(engine) as session:
            assert session.get(TaskLaneSlot, (lane, 0)).lease_owner == "test-existing-owner"
            assert len(list(session.scalars(select(TaskLaneSlot).where(TaskLaneSlot.lane == lane)))) == 500
            assert len(list(session.scalars(select(TaskLaneResourceSlot).where(TaskLaneResourceSlot.lane == lane)))) == 500
    finally:
        with engine.begin() as connection:
            connection.execute(delete(TaskLaneResourceSlot).where(TaskLaneResourceSlot.lane == lane))
            connection.execute(delete(TaskLaneSlot).where(TaskLaneSlot.lane == lane))
            connection.execute(delete(Scope).where(Scope.id == scope_id))
        engine.dispose()
