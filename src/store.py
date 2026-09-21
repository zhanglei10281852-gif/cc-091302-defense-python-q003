"""持久化：原子快照存储。

每次已提交的状态变更都通过 :meth:`JsonSnapshotStore.save` 落盘（先写临时
文件再 ``os.replace``，崩溃也不会留下半个状态文件）。重启时 :meth:`load`
读出全部状态——包括未过期租约和全部历史事件——由服务层继续计时。

时间使用墙上时钟（:func:`time.time`），这样租约的 ``valid_until`` 在进程
重启后仍可直接与当前时间相减得到剩余有效期；单调时钟无法跨重启使用。
测试中注入受控 :class:`~src.models.Clock` 以确定性地驱动时间。
"""

from __future__ import annotations

import json
import os
import tempfile
from typing import Protocol


class Store(Protocol):
    def save(self, state: dict) -> None: ...
    def load(self) -> dict | None: ...


class JsonSnapshotStore:
    """单文件 JSON 快照，写穿透（write-through）。"""

    SCHEMA_VERSION = 1

    def __init__(self, path: str):
        self.path = path

    def save(self, state: dict) -> None:
        directory = os.path.dirname(os.path.abspath(self.path)) or "."
        os.makedirs(directory, exist_ok=True)
        # 同目录临时文件 + 原子改名，保证读到的永远是完整快照
        fd, tmp = tempfile.mkstemp(prefix=".state-", suffix=".tmp", dir=directory)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(
                    {"schema_version": self.SCHEMA_VERSION, "state": state},
                    fh,
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, self.path)
        except BaseException:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            raise

    def load(self) -> dict | None:
        if not os.path.exists(self.path):
            return None
        with open(self.path, encoding="utf-8") as fh:
            blob = json.load(fh)
        return blob["state"]


class MemoryStore:
    """不落盘的存储，供单元测试使用。"""

    def __init__(self) -> None:
        self._state: dict | None = None
        self.saves = 0

    def save(self, state: dict) -> None:
        self._state = json.loads(json.dumps(state))  # 深拷贝并校验可 JSON 化
        self.saves += 1

    def load(self) -> dict | None:
        return self._state
