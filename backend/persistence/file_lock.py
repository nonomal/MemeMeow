# 持久化层共享文件锁；调用方必须在申请数据库连接之前获取锁。

from __future__ import annotations

import fcntl
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


@contextmanager
def shared_file_lock(path: Path, *, blocking: bool = True) -> Iterator[bool]:
    """锁定共享目录中的固定文件；返回获取结果，退出或进程终止时释放锁。

    锁文件永久保留，确保等待者始终使用同一个 inode。非阻塞调用用于周期任务。
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    acquired = False
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB))
            acquired = True
        except BlockingIOError:
            if blocking:
                raise
        yield acquired
    finally:
        try:
            if acquired:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)
