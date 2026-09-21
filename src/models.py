"""领域模型：设备、救援小组、租约、事件、时钟。

模型本身只承载状态，不做调度决策；所有变更都经过 :mod:`src.service` 统一
加锁、产生事件并持久化。
"""

from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum


class Clock:
    """墙上时钟（epoch 秒，float）。

    使用 :func:`time.time` 而非单调时钟：单调时间无法跨进程重启比较，
    而租约的 ``valid_until`` 必须在重启后直接减去当前时间得到剩余有效期。
    测试中注入固定/步进时钟，使有效期、过期与恢复行为可确定性验证。
    """

    def now(self) -> float:
        return time.time()


# ---------------------------------------------------------------- 设备 / 小组


class Band(Enum):
    """应急车可用频段。"""

    UHF = "UHF"
    VHF = "VHF"
    SAT = "SAT"   # 卫星回程


@dataclass
class Device:
    """一台可分配的通信设备（电台/网关/卫星终端）。

    位置使用平面坐标（公里），用欧氏距离判断覆盖；真实部署可替换为 GIS。
    """

    device_id: str
    name: str
    bands: frozenset[Band]
    total_bandwidth_khz: float          # 本设备可切分的总带宽
    used_bandwidth_khz: float = 0.0
    power_budget_w: float = 0.0          # 0 表示不受电源预算约束
    used_power_w: float = 0.0
    x: float = 0.0
    y: float = 0.0
    range_km: float = 0.0               # 0 表示不限制距离
    online: bool = True

    @property
    def free_bandwidth_khz(self) -> float:
        return self.total_bandwidth_khz - self.used_bandwidth_khz

    @property
    def free_power_w(self) -> float:
        if self.power_budget_w <= 0:
            return float("inf")
        return self.power_budget_w - self.used_power_w

    def covers(self, x: float, y: float) -> bool:
        if self.range_km <= 0:
            return True
        return ((x - self.x) ** 2 + (y - self.y) ** 2) ** 0.5 <= self.range_km

    def to_dict(self, include_usage: bool = True) -> dict:
        data = {
            "device_id": self.device_id,
            "name": self.name,
            "bands": sorted(b.value for b in self.bands),
            "total_bandwidth_khz": self.total_bandwidth_khz,
            "x": self.x,
            "y": self.y,
            "range_km": self.range_km,
            "power_budget_w": self.power_budget_w,
            "online": self.online,
        }
        if include_usage:
            data.update(
                used_bandwidth_khz=self.used_bandwidth_khz,
                free_bandwidth_khz=self.free_bandwidth_khz,
                used_power_w=self.used_power_w,
                free_power_w=(None if self.power_budget_w <= 0 else self.free_power_w),
                active_leases=getattr(self, "_active", None) or [],
            )
        return data


@dataclass
class Team:
    """报到的救援小组。"""

    team_id: str
    name: str
    x: float
    y: float
    operator: str                       # 现场登记操作员
    capabilities: frozenset[Band] = frozenset()

    def to_dict(self) -> dict:
        return {
            "team_id": self.team_id,
            "name": self.name,
            "x": self.x,
            "y": self.y,
            "operator": self.operator,
            "capabilities": sorted(b.value for b in self.capabilities),
        }


# ---------------------------------------------------------------- 租约


class LeaseState(str, Enum):
    ACTIVE = "ACTIVE"                   # 在效：占用资源
    EXPIRED = "EXPIRED"                 # 到有效期结束
    RELEASED = "RELEASED"               # 主动释放
    PREEMPTED = "PREEMPTED"             # 被高优先级抢占
    OFFLINE_TERMINATED = "OFFLINE_TERMINATED"  # 设备掉线被终止
    REJECTED = "REJECTED"               # 申请未获通过（仅存历史）


@dataclass
class Lease:
    """一份资源租约。

    每个租约都带有：

    * ``valid_from`` / ``valid_until``：有效期（单调时钟秒）；
    * ``version``：租约版本，每次续约/改配 +1，延期、释放必须校验版本，
      防止现场两个终端基于过期视图重复操作；
    * ``lease_id``：全局唯一，重新申请若指定原租约则保留血缘。
    """

    lease_id: str
    team_id: str
    device_id: str
    band: Band
    bandwidth_khz: float
    power_w: float
    priority: int                       # 数值越大越优先
    valid_from: float
    valid_until: float
    operator: str
    request_id: str                     # 幂等键：同一调度请求只产生一次结果
    state: LeaseState = LeaseState.ACTIVE
    version: int = 1
    reissued_from: str | None = None    # 由哪份被抢占/过期租约重新申请而来
    preempted_lease_ids: list[str] = field(default_factory=list)
    terminate_reason: str | None = None
    created_at: float = 0.0

    # -- 计时 --------------------------------------------------------------
    def remaining(self, now: float) -> float:
        return max(0.0, self.valid_until - now)

    def is_active(self, now: float) -> bool:
        return self.state == LeaseState.ACTIVE and now < self.valid_until

    # -- 序列化 ------------------------------------------------------------
    def to_dict(self, now: float | None = None) -> dict:
        data = {
            "lease_id": self.lease_id,
            "team_id": self.team_id,
            "device_id": self.device_id,
            "band": self.band.value,
            "bandwidth_khz": self.bandwidth_khz,
            "power_w": self.power_w,
            "priority": self.priority,
            "valid_from": self.valid_from,
            "valid_until": self.valid_until,
            "ttl_seconds": (None if now is None else round(self.remaining(now), 3)),
            "operator": self.operator,
            "request_id": self.request_id,
            "state": self.state.value,
            "version": self.version,
            "reissued_from": self.reissued_from,
            "preempted_leases": list(self.preempted_lease_ids),
            "terminate_reason": self.terminate_reason,
            "created_at": self.created_at,
        }
        return data


# ---------------------------------------------------------------- 事件


class EventType(str, Enum):
    DEVICE_REGISTERED = "DEVICE_REGISTERED"
    DEVICE_ONLINE = "DEVICE_ONLINE"
    DEVICE_OFFLINE = "DEVICE_OFFLINE"
    TEAM_REGISTERED = "TEAM_REGISTERED"
    TEAM_POSITION_UPDATED = "TEAM_POSITION_UPDATED"
    LEASE_GRANTED = "LEASE_GRANTED"
    LEASE_PREEMPTED = "LEASE_PREEMPTED"
    LEASE_EXTENDED = "LEASE_EXTENDED"
    LEASE_RELEASED = "LEASE_RELEASED"
    LEASE_EXPIRED = "LEASE_EXPIRED"
    LEASE_REISSUED = "LEASE_REISSUED"
    LEASE_TERMINATED_OFFLINE = "LEASE_TERMINATED_OFFLINE"
    REQUEST_REJECTED = "REQUEST_REJECTED"
    REQUEST_DEFERRED = "REQUEST_DEFERRED"
    SERVICE_RESTORED = "SERVICE_RESTORED"


@dataclass
class Event:
    """资源变更事件（只增不改，审计留痕）。"""

    event_id: str
    seq: int
    timestamp: float
    event_type: EventType
    operator: str
    details: dict = field(default_factory=dict)

    @staticmethod
    def new_id() -> str:
        return uuid.uuid4().hex

    def to_dict(self) -> dict:
        return {
            "event_id": self.event_id,
            "seq": self.seq,
            "timestamp": self.timestamp,
            "event_type": self.event_type.value,
            "operator": self.operator,
            "details": self.details,
        }


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"
