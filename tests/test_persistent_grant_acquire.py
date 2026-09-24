# 使用真实数据库与本地策略验证公共 grant 的幂等登记。

from uuid import uuid4

from dotenv import dotenv_values
from sqlalchemy import delete

from backend.operation_policy import AllowAllOperationPolicy, OperationPolicyGateway, Operations, PersistentGrantRepository
from backend.persistence.engine import create_engine_for_url
from backend.persistence.models import OperationGrant, Scope, ScopeContext
from backend.persistence.resources import DatabaseResources


def test_persistent_local_grant_is_idempotent(tmp_path):
    """真实本地授权重复获取时保留同一 grant，并完成数据库登记。"""
    engine = create_engine_for_url(dotenv_values(".env")["MEMEMEOW_DATABASE_URL"])
    scope = ScopeContext("test-grant-acquire-" + uuid4().hex)
    with engine.begin() as connection:
        connection.execute(Scope.__table__.insert().values(id=scope.scope_id))
    resources = DatabaseResources(engine, image_root=tmp_path / "images", data_root=tmp_path / "data", require_local_scope=False)
    gateway = OperationPolicyGateway(AllowAllOperationPolicy())
    repository = PersistentGrantRepository(resources, scope)
    request = gateway.request(scope, Operations.IMAGE_UPLOAD, "test-upload", resource_id="test.png")
    try:
        first = repository.acquire(request, gateway)
        second = repository.acquire(request, gateway)
        assert first.grant == second.grant
        assert repository.get(request).grant == first.grant
    finally:
        with engine.begin() as connection:
            connection.execute(delete(OperationGrant).where(OperationGrant.scope_id == scope.scope_id))
            connection.execute(delete(Scope).where(Scope.id == scope.scope_id))
        engine.dispose()
