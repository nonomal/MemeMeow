# 图片库排序索引：支持名称忽略大小写和最近更新的分页查询。
from alembic import op
import sqlalchemy as sa


revision = "0028_image_library_sort"
down_revision = "0027_already_exited_termination_signal"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """为现有图片记录增加排序索引，并更新本地安装版本。"""
    op.create_index("ix_memes_scope_name_lower", "memes", ["scope_id", sa.text("lower(display_name)"), "id"])
    op.create_index("ix_memes_scope_updated", "memes", ["scope_id", sa.text("updated_at DESC"), sa.text("id DESC")])
    op.execute("UPDATE installation_state SET schema_revision = '0028_image_library_sort' WHERE key = 'local'")


def downgrade() -> None:
    """项目 schema 只允许前向升级。"""
    raise RuntimeError("本项目 schema 只允许前向升级")
