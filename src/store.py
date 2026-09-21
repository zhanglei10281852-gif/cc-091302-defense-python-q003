"""持久化：仅追加的资源变更事件日志 + 原子写入的状态快照。

- 事件日志（*.events.jsonl）：保存每一次资源变更与操作员身份，只追加。
- 状态快照（*.state.json）：设备、租约、延期队列与序列号，原子替换写入。
  租约使用绝对时间戳，重启加载后按当前时间继续计时并清理已过期租约。
"""

from __future__ import annotations

import json
import os
import tempfile
from typing import Iterable, Optional

SNAPSHOT_SCHEMA = 1
_DEFAULT_STATE = {
    "schema": SNAPSHOT_SCHEMA,
    "devices": [],
    "leases": {},
    "deferred": [],
    "event_seq": 0,
    "lease_seq": 0,
    "saved_at": None,
}


class EventStore:
    def __init__(self, state_path: str, events_path: Optional[str] = None):
        self.state_path = state_path
        self.events_path = events_path or state_path + ".events.jsonl"

    # ---- 快照 ----
    def load_state(self) -> dict:
        if not os.path.exists(self.state_path):
            return json.loads(json.dumps(_DEFAULT_STATE))
        with open(self.state_path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        data.setdefault("devices", [])
        data.setdefault("leases", {})
        data.setdefault("deferred", [])
        data.setdefault("event_seq", 0)
        data.setdefault("lease_seq", 0)
        return data

    def save_state(self, state: dict) -> None:
        directory = os.path.dirname(os.path.abspath(self.state_path)) or "."
        os.makedirs(directory, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(
            prefix=".state-", suffix=".tmp", dir=directory
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                json.dump(state, fh, ensure_ascii=False, sort_keys=True)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp_path, self.state_path)
        except BaseException:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

    # ---- 事件日志 ----
    def append_event(self, event: dict) -> None:
        directory = os.path.dirname(os.path.abspath(self.events_path)) or "."
        os.makedirs(directory, exist_ok=True)
        with open(self.events_path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n")
            fh.flush()
            os.fsync(fh.fileno())

    def read_events(self) -> Iterable[dict]:
        if not os.path.exists(self.events_path):
            return []
        with open(self.events_path, "r", encoding="utf-8") as fh:
            return [json.loads(line) for line in fh if line.strip()]
