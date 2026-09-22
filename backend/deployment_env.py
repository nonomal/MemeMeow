# 部署工具共用的 dotenv 读取和宿主端口校验，供适配层及命令行入口使用。

from __future__ import annotations

import os
import re
from collections.abc import Collection, Mapping
from pathlib import Path

from dotenv.main import DotEnv
from dotenv.parser import parse_stream


def deployment_environment(
    env_file: Path, *, unique_keys: Collection[str] = ()
) -> dict[str, str | None]:
    """读取指定 dotenv，并让进程环境覆盖文件；指定字段重复或语法错误时立即报错。"""
    if env_file.is_file():
        seen: set[str] = set()
        with env_file.open(encoding="utf-8") as stream:
            for binding in parse_stream(stream):
                if binding.error:
                    raise ValueError(f"{env_file}: dotenv 语法错误，行号 {binding.original.line}")
                if binding.key in unique_keys and binding.key in seen:
                    raise ValueError(f"{binding.key}: dotenv 重复配置，行号 {binding.original.line}")
                if binding.key is not None:
                    seen.add(binding.key)
        values = dict(DotEnv(env_file, override=False).dict())
    else:
        values = {}
    return {**values, **os.environ}


def host_ports(
    values: Mapping[str, str | None], defaults: Mapping[str, int]
) -> dict[str, int]:
    """解析宿主 TCP 端口；只对缺失字段使用默认值，拒绝非法值和重复端口。"""
    result: dict[str, int] = {}
    owners: dict[int, str] = {}
    for name, default in defaults.items():
        raw = values.get(name, str(default))
        if raw is None or re.fullmatch(r"[1-9][0-9]{0,4}", raw) is None or int(raw) > 65535:
            raise ValueError(f"{name} 必须是 1 到 65535 的十进制端口号")
        port = int(raw)
        if port in owners:
            raise ValueError(f"{name} 与 {owners[port]} 重复使用宿主端口 {port}")
        owners[port] = name
        result[name] = port
    return result
