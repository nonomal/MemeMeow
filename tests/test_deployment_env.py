# 部署配置使用真实 dotenv 文件验证读取优先级和端口错误，无外部服务依赖。

import os

import pytest

from backend.deployment_env import deployment_environment, host_ports


def test_environment_priority_and_dotenv_syntax(tmp_path):
    """真实文件支持 export、引号和注释，进程已有字段拥有最高优先级。"""
    env_file = tmp_path / ".env"
    env_file.write_text('export TEST_PORT="31275" # API\nPATH="file-value"\n', encoding="utf-8")
    values = deployment_environment(env_file)
    assert values["PATH"] == os.environ["PATH"]
    assert host_ports(values, {"TEST_PORT": 8275}) == {"TEST_PORT": 31275}
    assert host_ports({}, {"TEST_PORT": 8275}) == {"TEST_PORT": 8275}


@pytest.mark.parametrize("raw", [None, "", "0", "65536", "-1", "1.2", "abc", "08080", " 8080"])
def test_invalid_explicit_port(raw):
    """显式非法值必须携带字段名报错。"""
    with pytest.raises(ValueError, match="TEST_PORT"):
        host_ports({"TEST_PORT": raw}, {"TEST_PORT": 8275})


def test_duplicate_ports_and_definitions(tmp_path):
    """不同服务占用同一端口与同一字段重复定义都属于配置错误。"""
    with pytest.raises(ValueError, match="重复使用"):
        host_ports({"API": "8080"}, {"API": 8275, "VIEWER": 8080})
    env_file = tmp_path / ".env"
    env_file.write_text("API=8080\nAPI=8081\n", encoding="utf-8")
    with pytest.raises(ValueError, match="API: dotenv 重复配置"):
        deployment_environment(env_file, unique_keys={"API"})


def test_malformed_dotenv(tmp_path):
    """库报告的 dotenv 语法错误包含具体行号。"""
    env_file = tmp_path / ".env"
    env_file.write_text('API="8080\n', encoding="utf-8")
    with pytest.raises(ValueError, match="行号 1"):
        deployment_environment(env_file)
