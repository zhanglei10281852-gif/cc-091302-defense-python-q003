"""应急通信资源编排的领域模型。

约束建模：
- ``Device`` 分两类：BASE（应急车基站/中继台，提供信道与电源预算）和
  TERMINAL（救援小组终端，声明自身支持的频段能力）。
- ``Channel`` 隶属于一台 BASE，有频段、中心频率与带宽；同一信道同一时刻
  只能被一条活跃租约占用（冲突域）。
- ``Lease`` 是一次分配的租约，必须带绝对时间戳的有效期（expires_at）与
  单调递增的版本号（version），重启后依据绝对时间继续计时。
"""

from __future__ import annotations

import uuid
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Optional


def _new_id() -> str:
    return uuid.uuid4().hex


class DeviceType(str, Enum):
    BASE = "BASE"
    TERMINAL = "TERMINAL"


class LeaseStatus(str, Enum):
    ACTIVE = "ACTIVE"
    RELEASED = "RELEASED"
    EXPIRED = "EXPIRED"
    PREEMPTED = "PREEMPTED"
    REVOKED = "REVOKED"


class AllocationStatus(str, Enum):
    """申请结果。CONFLICT 表示与其它调度请求发生了明确的资源竞争。"""

    GRANTED = "GRANTED"
    DEFERRED = "DEFERRED"
    CONFLICT = "CONFLICT"
    REJECTED = "REJECTED"


class RejectReason(str, Enum):
    DEVICE_OFFLINE = "DEVICE_OFFLINE"
    NO_DEVICE_IN_RANGE = "NO_DEVICE_IN_RANGE"
    CAPABILITY_MISMATCH = "CAPABILITY_MISMATCH"
    INSUFFICIENT_BANDWIDTH = "INSUFFICIENT_BANDWIDTH"
    INSUFFICIENT_POWER = "INSUFFICIENT_POWER"
    ALLOCATION_CONFLICT = "ALLOCATION_CONFLICT"
    VERSION_CONFLICT = "VERSION_CONFLICT"
    REQUEST_EXPIRED = "REQUEST_EXPIRED"
    LEASE_EXPIRED = "LEASE_EXPIRED"
    LEASE_NOT_ACTIVE = "LEASE_NOT_ACTIVE"
    NOT_FOUND = "NOT_FOUND"


class EventType:
    DEVICE_REGISTERED = "device.registered"
    DEVICE_STATUS = "device.status"
    LEASE_GRANTED = "lease.granted"
    LEASE_RENEWED = "lease.renewed"
    LEASE_RELEASED = "lease.released"
    LEASE_EXPIRED = "lease.expired"
    LEASE_PREEMPTED = "lease.preempted"
    LEASE_REVOKED = "lease.revoked"
    LEASE_OP_REJECTED = "lease.op_rejected"
    REQUEST_DEFERRED = "request.deferred"
    REQUEST_REJECTED = "request.rejected"
    REQUEST_EXPIRED = "request.expired"


# 所有携带 lease 快照、重放时需写回租约表的事件。
ALLOCATION_EVENTS = frozenset(
    {
        EventType.LEASE_GRANTED,
        EventType.LEASE_RENEWED,
        EventType.LEASE_RELEASED,
        EventType.LEASE_EXPIRED,
        EventType.LEASE_PREEMPTED,
        EventType.LEASE_REVOKED,
    }
)


SYSTEM_OPERATOR = "system"


@dataclass
class Channel:
    """基站提供的一个频段信道。"""

    id: str
    provider_device_id: str
    frequency_mhz: float
    bandwidth_mhz: float
    band: str

    @staticmethod
    def from_dict(d: dict) -> "Channel":
        return Channel(**d)


@dataclass
class Device:
    id: str
    name: str
    type: str  # DeviceType 的值
    x_km: float
    y_km: float
    # BASE 属性
    range_km: float = 0.0
    power_budget_w: float = 0.0
    channels: list[Channel] = field(default_factory=list)
    # TERMINAL 属性
    group_id: Optional[str] = None
    supported_bands: list[str] = field(default_factory=list)
    online: bool = True

    @staticmethod
    def base(
        device_id: str,
        name: str,
        x_km: float,
        y_km: float,
        range_km: float,
        power_budget_w: float,
        channels: list[Channel],
        online: bool = True,
    ) -> "Device":
        return Device(
            id=device_id,
            name=name,
            type=DeviceType.BASE.value,
            x_km=x_km,
            y_km=y_km,
            range_km=range_km,
            power_budget_w=power_budget_w,
            channels=channels,
            online=online,
        )

    @staticmethod
    def terminal(
        device_id: str,
        name: str,
        group_id: str,
        x_km: float,
        y_km: float,
        supported_bands: list[str],
        online: bool = True,
    ) -> "Device":
        return Device(
            id=device_id,
            name=name,
            type=DeviceType.TERMINAL.value,
            x_km=x_km,
            y_km=y_km,
            group_id=group_id,
            supported_bands=list(supported_bands),
            online=online,
        )

    def to_dict(self) -> dict:
        d = asdict(self)
        return d

    @staticmethod
    def from_dict(d: dict) -> "Device":
        channels = [Channel.from_dict(c) for c in d.get("channels", [])]
        return Device(
            id=d["id"],
            name=d["name"],
            type=d["type"],
            x_km=d["x_km"],
            y_km=d["y_km"],
            range_km=d.get("range_km", 0.0),
            power_budget_w=d.get("power_budget_w", 0.0),
            channels=channels,
            group_id=d.get("group_id"),
            supported_bands=list(d.get("supported_bands", [])),
            online=d.get("online", True),
        )


@dataclass
class Request:
    """救援小组的一次链路申请。

    - location 可显式给出，否则取终端当前登记位置。
    - power_w 可显式给出，否则由带宽与距离估算。
    - allow_preempt：允许抢占严格更低优先级的活跃租约。
    - allow_defer：资源暂不可得时进入延期队列，而非直接返回冲突/拒绝。
    """

    group_id: str
    terminal_device_id: str
    bandwidth_mhz: float
    priority: int
    ttl_seconds: float
    x_km: Optional[float] = None
    y_km: Optional[float] = None
    power_w: Optional[float] = None
    allow_preempt: bool = False
    allow_defer: bool = True
    valid_for_seconds: Optional[float] = None
    request_id: str = field(default_factory=_new_id)
    origin_request_id: Optional[str] = None
    # 以下由服务端在落盘时补齐
    submitted_by: Optional[str] = None
    submitted_at: Optional[float] = None
    deferred_at: Optional[float] = None

    @property
    def valid_for(self) -> float:
        """申请在延期队列中的有效等待时长，默认为自身 TTL。"""
        return (
            self.valid_for_seconds
            if self.valid_for_seconds is not None
            else self.ttl_seconds
        )

    def valid_until(self) -> float:
        if self.submitted_at is None:
            raise OrchestratorError("request has not been submitted yet")
        return self.submitted_at + self.valid_for

    def to_dict(self) -> dict:
        return asdict(self)

    @staticmethod
    def from_dict(d: dict) -> "Request":
        return Request(**d)


@dataclass
class Lease:
    lease_id: str
    request_id: str
    group_id: str
    terminal_device_id: str
    provider_device_id: str
    channel_id: str
    bandwidth_mhz: float
    power_w: float
    priority: int
    ttl_seconds: float
    issued_at: float
    expires_at: float
    version: int
    status: str
    granted_by: str

    @staticmethod
    def from_dict(d: dict) -> "Lease":
        return Lease(**d)

    def to_dict(self) -> dict:
        return asdict(self)

    def is_active_at(self, now: float) -> bool:
        return self.status == LeaseStatus.ACTIVE.value and now < self.expires_at


@dataclass
class FailureRecord:
    """失败/延期/异常终止留痕，供失败原因查询。"""

    ts: float
    kind: str  # DEFERRED / REJECTED / PREEMPTED / REVOKED / EXPIRED / VERSION_CONFLICT
    reason: str
    operator: str
    request_id: Optional[str] = None
    lease_id: Optional[str] = None
    group_id: Optional[str] = None
    detail: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class AllocationResult:
    status: str
    request_id: str
    lease: Optional[Lease] = None
    reason: Optional[str] = None
    blocking_leases: list[str] = field(default_factory=list)
    deferred_at: Optional[float] = None

    @property
    def granted(self) -> bool:
        return self.status == AllocationStatus.GRANTED.value

    def to_dict(self) -> dict:
        return {
            "status": self.status,
            "request_id": self.request_id,
            "lease": self.lease.to_dict() if self.lease else None,
            "reason": self.reason,
            "blocking_leases": list(self.blocking_leases),
            "deferred_at": self.deferred_at,
        }


class OrchestratorError(Exception):
    """编排服务异常基类（编程性/前置条件错误）。"""


class NotFoundError(OrchestratorError):
    pass


class VersionConflictError(OrchestratorError):
    """租约版本与调用方持有的版本不一致（乐观锁冲突）。"""

    def __init__(self, lease_id: str, expected: int, actual: int):
        super().__init__(
            f"lease {lease_id} version conflict: expected {expected}, actual {actual}"
        )
        self.lease_id = lease_id
        self.expected = expected
        self.actual = actual


class LeaseStateError(OrchestratorError):
    def __init__(self, lease_id: str, status: str, reason: str):
        super().__init__(f"lease {lease_id} is {status}: {reason}")
        self.lease_id = lease_id
        self.status = status
        self.reason = reason
