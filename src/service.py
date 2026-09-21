"""应急通信资源编排服务。

职责（对应现场需求）：

* 接收小组位置、任务优先级、所需带宽/电源与设备能力，在有限频段与电源
  容量下计算可行分配，每份分配都是带**有效期**和**租约版本**的租约；
* 处理抢占（高优先级任务可挤走低优先级租约）、延期（租约续约 + 请求
  排队）、释放与重新申请；
* 租约过期或设备掉线后立即停止占用资源；
* 两个调度请求同时竞争同一资源且无法通过抢占化解时，抛出
  :class:`~src.orchestrator_errors.ResourceConflictError`，明确给出冲突租约；
* 保存全部资源变更事件与操作员身份，提供当前拓扑、租约历史、失败原因
  查询；
* 每次变更写穿透到快照存储，重启后恢复未过期租约并继续计时。

线程模型：服务内部持有一把可重入锁，所有公共方法都是原子的；并发的
调度请求被串行化裁决，后来的失败者拿到明确的冲突信息。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Iterable

from .models import (
    Band,
    Clock,
    Device,
    Event,
    EventType,
    Lease,
    LeaseState,
    Team,
    new_id,
)
from .orchestrator_errors import (
    InvalidRequestError,
    LeaseNotActiveError,
    LeaseVersionConflictError,
    OrchestratorError,
    Reason,
    ResourceConflictError,
)
from .store import JsonSnapshotStore, MemoryStore, Store

_EPS = 1e-9
_SYSTEM = "system"


@dataclass
class DeferredRequest:
    """因资源不足被操作员主动延期（排队）的调度请求。"""

    request_id: str
    team_id: str
    bandwidth_khz: float
    band: Band
    priority: int
    ttl_seconds: float
    power_w: float
    operator: str
    note: str
    created_at: float
    state: str = "PENDING"                    # PENDING / FULFILLED / CANCELLED
    fulfilled_lease_id: str | None = None
    last_failure: dict | None = None

    def to_dict(self) -> dict:
        return {
            "request_id": self.request_id,
            "team_id": self.team_id,
            "bandwidth_khz": self.bandwidth_khz,
            "band": self.band.value,
            "priority": self.priority,
            "ttl_seconds": self.ttl_seconds,
            "power_w": self.power_w,
            "operator": self.operator,
            "note": self.note,
            "created_at": self.created_at,
            "state": self.state,
            "fulfilled_lease_id": self.fulfilled_lease_id,
            "last_failure": self.last_failure,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "DeferredRequest":
        return cls(
            request_id=d["request_id"],
            team_id=d["team_id"],
            bandwidth_khz=d["bandwidth_khz"],
            band=Band(d["band"]),
            priority=d["priority"],
            ttl_seconds=d["ttl_seconds"],
            power_w=d.get("power_w", 0.0),
            operator=d["operator"],
            note=d.get("note", ""),
            created_at=d["created_at"],
            state=d.get("state", "PENDING"),
            fulfilled_lease_id=d.get("fulfilled_lease_id"),
            last_failure=d.get("last_failure"),
        )


class OrchestratorService:
    """资源编排领域服务。所有公共方法均为线程安全的原子操作。"""

    def __init__(self, store: Store | str | None = None, clock: Clock | None = None):
        if isinstance(store, str):
            store = JsonSnapshotStore(store)
        self._store: Store = store if store is not None else MemoryStore()
        self._clock = clock or Clock()
        self._lock = threading.RLock()

        self._devices: dict[str, Device] = {}
        self._teams: dict[str, Team] = {}
        self._leases: dict[str, Lease] = {}
        self._events: list[Event] = []
        self._seq = 0
        self._requests: dict[str, dict] = {}   # 幂等键 -> 首次裁决结果
        self._failures: list[dict] = []        # 失败原因台账
        self._deferred: dict[str, DeferredRequest] = {}
        self.ready = False
        self._restore()

    # ============================================================ 内部工具

    def _now(self) -> float:
        return self._clock.now()

    def _require_operator(self, operator: str) -> str:
        if not operator or not str(operator).strip():
            raise InvalidRequestError("所有变更都必须登记操作员身份")
        return str(operator).strip()

    @staticmethod
    def _as_band(band: Band | str) -> Band:
        if isinstance(band, Band):
            return band
        try:
            return Band(band)
        except ValueError:
            raise InvalidRequestError(f"不支持的频段：{band!r}")

    @staticmethod
    def _as_bands(bands: Iterable[Band | str]) -> frozenset[Band]:
        return frozenset(OrchestratorService._as_band(b) for b in bands)

    def _emit(self, event_type: EventType, operator: str, details: dict) -> Event:
        self._seq += 1
        event = Event(
            event_id=Event.new_id(),
            seq=self._seq,
            timestamp=self._now(),
            event_type=event_type,
            operator=operator,
            details=details,
        )
        self._events.append(event)
        return event

    def _persist(self) -> None:
        state = {
            "devices": [d.to_dict(include_usage=False) for d in self._devices.values()],
            "teams": [t.to_dict() for t in self._teams.values()],
            "leases": [l.to_dict() for l in self._leases.values()],
            "events": [e.to_dict() for e in self._events],
            "seq": self._seq,
            "requests": self._requests,
            "failures": self._failures,
            "deferred": [d.to_dict() for d in self._deferred.values()],
            "saved_at": self._now(),
        }
        self._store.save(state)

    def _active_leases(self, device_id: str | None = None) -> list[Lease]:
        now = self._now()
        leases = [
            l
            for l in self._leases.values()
            if l.state == LeaseState.ACTIVE and l.valid_until > now
        ]
        if device_id is not None:
            leases = [l for l in leases if l.device_id == device_id]
        return leases

    def _sweep_expired(self) -> list[str]:
        """把到点租约转为 EXPIRED 并释放资源；返回被清理的租约 id。"""
        now = self._now()
        expired_ids: list[str] = []
        for lease in self._leases.values():
            if lease.state == LeaseState.ACTIVE and now >= lease.valid_until:
                self._terminate(lease, LeaseState.EXPIRED, "EXPIRED")
                self._emit(
                    EventType.LEASE_EXPIRED,
                    _SYSTEM,
                    {"lease_id": lease.lease_id, "team_id": lease.team_id,
                     "device_id": lease.device_id, "valid_until": lease.valid_until},
                )
                expired_ids.append(lease.lease_id)
        return expired_ids

    def _terminate(self, lease: Lease, state: LeaseState, reason: str) -> None:
        lease.state = state
        lease.terminate_reason = reason
        device = self._devices.get(lease.device_id)
        if device is not None:
            device.used_bandwidth_khz = max(0.0, device.used_bandwidth_khz - lease.bandwidth_khz)
            device.used_power_w = max(0.0, device.used_power_w - lease.power_w)
            if hasattr(device, "_active") and lease.lease_id in device._active:
                device._active.remove(lease.lease_id)

    def _record_failure(self, operator: str, err: OrchestratorError, context: dict) -> dict:
        record = {
            "failure_id": new_id("fail"),
            "timestamp": self._now(),
            "operator": operator,
            "reason": err.reason,
            "detail": err.detail,
            "context": context,
        }
        if isinstance(err, ResourceConflictError):
            record["competing_leases"] = err.competing_leases
            record["candidate_devices"] = err.candidate_devices
        self._failures.append(record)
        self._emit(EventType.REQUEST_REJECTED, operator, record)
        return record

    # ============================================================ 注册管理

    def register_device(
        self,
        operator: str,
        device_id: str,
        name: str,
        bands: Iterable[Band | str],
        total_bandwidth_khz: float,
        *,
        power_budget_w: float = 0.0,
        x: float = 0.0,
        y: float = 0.0,
        range_km: float = 0.0,
        online: bool = True,
    ) -> dict:
        operator = self._require_operator(operator)
        if total_bandwidth_khz <= 0:
            raise InvalidRequestError("设备总带宽必须为正数")
        if power_budget_w < 0:
            raise InvalidRequestError("电源预算不能为负")
        band_set = self._as_bands(bands)
        if not band_set:
            raise InvalidRequestError("设备至少要支持一个频段")
        with self._lock:
            if device_id in self._devices:
                raise InvalidRequestError(f"设备 {device_id} 已注册")
            device = Device(
                device_id=device_id,
                name=name,
                bands=band_set,
                total_bandwidth_khz=float(total_bandwidth_khz),
                power_budget_w=float(power_budget_w),
                x=float(x),
                y=float(y),
                range_km=float(range_km),
                online=online,
            )
            device._active = []  # type: ignore[attr-defined]
            self._devices[device_id] = device
            self._emit(EventType.DEVICE_REGISTERED, operator, device.to_dict())
            self._persist()
            return device.to_dict()

    def set_device_online(self, operator: str, device_id: str, online: bool) -> dict:
        """通报设备上线/掉线。掉线时该设备上的在效租约全部强制终止。"""
        operator = self._require_operator(operator)
        with self._lock:
            expired = self._sweep_expired()
            device = self._devices.get(device_id)
            if device is None:
                raise InvalidRequestError(f"未知设备：{device_id}", reason=Reason.DEVICE_UNKNOWN)
            if device.online == online:
                if expired:
                    self._persist()
                return device.to_dict()
            device.online = online
            if online:
                self._emit(EventType.DEVICE_ONLINE, operator, {"device_id": device_id})
            else:
                victims = [
                    l for l in self._active_leases(device_id)
                    if l.state == LeaseState.ACTIVE
                ]
                for lease in victims:
                    self._terminate(lease, LeaseState.OFFLINE_TERMINATED,
                                    Reason.DEVICE_WENT_OFFLINE)
                    self._emit(
                        EventType.LEASE_TERMINATED_OFFLINE,
                        operator,
                        {"lease_id": lease.lease_id, "team_id": lease.team_id,
                         "device_id": device_id},
                    )
                self._emit(
                    EventType.DEVICE_OFFLINE,
                    operator,
                    {"device_id": device_id, "terminated_leases": [l.lease_id for l in victims]},
                )
            self._persist()
            return device.to_dict()

    def register_team(
        self,
        operator: str,
        team_id: str,
        name: str,
        x: float,
        y: float,
        capabilities: Iterable[Band | str] = (),
    ) -> dict:
        operator = self._require_operator(operator)
        caps = self._as_bands(capabilities)
        with self._lock:
            if team_id in self._teams:
                raise InvalidRequestError(f"小组 {team_id} 已报到")
            team = Team(
                team_id=team_id, name=name, x=float(x), y=float(y),
                operator=operator, capabilities=caps,
            )
            self._teams[team_id] = team
            self._emit(EventType.TEAM_REGISTERED, operator, team.to_dict())
            self._persist()
            return team.to_dict()

    def update_team_position(self, operator: str, team_id: str, x: float, y: float) -> dict:
        """更新小组位置（在效租约保留到到期/释放，新申请按新位置裁决）。"""
        operator = self._require_operator(operator)
        with self._lock:
            team = self._teams.get(team_id)
            if team is None:
                raise InvalidRequestError(f"未知小组：{team_id}", reason=Reason.TEAM_UNKNOWN)
            old = {"x": team.x, "y": team.y}
            team.x, team.y = float(x), float(y)
            self._emit(EventType.TEAM_POSITION_UPDATED, operator,
                       {"team_id": team_id, "from": old,
                        "to": {"x": team.x, "y": team.y}})
            self._persist()
            return team.to_dict()

    # ============================================================ 核心分配

    def request_link(
        self,
        operator: str,
        team_id: str,
        *,
        bandwidth_khz: float,
        band: Band | str,
        priority: int,
        ttl_seconds: float,
        power_w: float = 0.0,
        request_id: str | None = None,
        allow_preemption: bool = True,
        reissue_from: str | None = None,
    ) -> dict:
        """为小组申请一条链路。

        成功返回新租约（含有效期与版本号）；资源不足且无法通过抢占化解时
        抛出 :class:`ResourceConflictError`（或更具体的拒绝异常），失败原因
        同时入账可查。``request_id`` 是幂等键：同一调度请求重复提交只裁决
        一次。
        """
        operator = self._require_operator(operator)
        band = self._as_band(band)
        if bandwidth_khz <= 0:
            raise InvalidRequestError("所需带宽必须为正数")
        if ttl_seconds <= 0:
            raise InvalidRequestError("租约有效期必须为正数")
        if power_w < 0:
            raise InvalidRequestError("所需电源不能为负")
        if not isinstance(priority, int):
            raise InvalidRequestError("任务优先级必须为整数")

        with self._lock:
            # 幂等重放：返回首次裁决结果，绝不重复分配
            if request_id and request_id in self._requests:
                return self._replay_outcome(request_id)

            self._sweep_expired()

            team = self._teams.get(team_id)
            if team is None:
                raise self._reject(operator, InvalidRequestError(
                    f"未知小组：{team_id}", reason=Reason.TEAM_UNKNOWN),
                    {"request_id": request_id, "team_id": team_id})
            if team.capabilities and band not in team.capabilities:
                raise self._reject(operator, InvalidRequestError(
                    f"小组 {team_id} 的设备不支持 {band.value} 频段",
                    reason=Reason.UNSUPPORTED_CAPABILITY),
                    {"request_id": request_id, "team_id": team_id, "band": band.value})

            source = None
            if reissue_from is not None:
                source = self._leases.get(reissue_from)
                if source is None:
                    raise InvalidRequestError(f"原租约不存在：{reissue_from}")
                if source.team_id != team_id:
                    raise InvalidRequestError("只能由原承租小组重新申请")
                if source.state == LeaseState.ACTIVE and source.valid_until > self._now():
                    raise InvalidRequestError(
                        f"原租约 {reissue_from} 仍在效；请先释放或使用 extend_lease 续约")

            candidates, reason = self._candidate_devices(team, band)
            if not candidates:
                raise self._reject(
                    operator,
                    ResourceConflictError(
                        self._unavailable_detail(reason, band),
                        candidate_devices=[d.device_id for d in self._devices.values()
                                          if band in d.bands],
                        reason=reason,
                    ),
                    self._request_context(request_id, team_id, band, bandwidth_khz, priority),
                )

            # 1) 直接可分配：最佳适配（剩余带宽最少但够用的设备）
            fitting = [
                d for d in candidates
                if d.free_bandwidth_khz + _EPS >= bandwidth_khz
                and d.free_power_w + _EPS >= power_w
            ]
            if fitting:
                device = min(fitting, key=lambda d: (d.free_bandwidth_khz, d.device_id))
                lease = self._grant(
                    operator, team, device, band, bandwidth_khz, power_w,
                    priority, ttl_seconds, request_id, reissue_from,
                    preempted=[],
                )
                return lease.to_dict(self._now())

            # 2) 尝试抢占低优先级租约
            plan: list[Lease] = []
            target: Device | None = None
            if allow_preemption:
                target, plan = self._preemption_plan(
                    candidates, bandwidth_khz, power_w, priority)

            if target is not None:
                for victim in plan:
                    self._terminate(victim, LeaseState.PREEMPTED, Reason.PREEMPTED)
                    self._emit(
                        EventType.LEASE_PREEMPTED, operator,
                        {"lease_id": victim.lease_id, "team_id": victim.team_id,
                         "device_id": target.device_id, "victim_priority": victim.priority,
                         "new_priority": priority,
                         "preempted_by_request": request_id},
                    )
                lease = self._grant(
                    operator, team, target, band, bandwidth_khz, power_w,
                    priority, ttl_seconds, request_id, reissue_from,
                    preempted=plan,
                )
                return lease.to_dict(self._now())

            # 3) 明确冲突：列出所有候选设备上的在效竞争租约
            competing = sorted({
                l.lease_id
                for d in candidates
                for l in self._active_leases(d.device_id)
            })
            reason = self._shortage_reason(candidates, bandwidth_khz, power_w)
            err = ResourceConflictError(
                f"小组 {team_id} 申请 {bandwidth_khz:g}kHz/{band.value} 无法满足："
                f"{len(candidates)} 台候选设备容量不足，且无更低优先级租约可抢占",
                competing_leases=competing,
                candidate_devices=[d.device_id for d in candidates],
                reason=reason,
            )
            raise self._reject(
                operator, err,
                self._request_context(request_id, team_id, band, bandwidth_khz, priority),
            )

    def _candidate_devices(
        self, team: Team, band: Band
    ) -> tuple[list[Device], str]:
        """返回（覆盖小组位置、在线、支持该频段的设备，不可用原因码）。"""
        support = [d for d in self._devices.values() if band in d.bands]
        if not support:
            return [], Reason.UNSUPPORTED_CAPABILITY
        covering_online = [d for d in support if d.online and d.covers(team.x, team.y)]
        if covering_online:
            # 电源/带宽不足属于竞争而非候选不可用，交给后续裁决
            return covering_online, Reason.RESOURCE_CONFLICT
        # 没有在线且覆盖的设备：优先报告“本可覆盖的设备掉线了”
        offline_covering = [
            d for d in support if not d.online and d.covers(team.x, team.y)]
        if offline_covering:
            return [], Reason.DEVICE_OFFLINE
        if any(d.online for d in support):
            return [], Reason.NO_DEVICE_IN_RANGE
        return [], Reason.DEVICE_OFFLINE

    @staticmethod
    def _shortage_reason(candidates: list[Device], need_bw: float,
                         need_power: float) -> str:
        """抢占不可行时的最终失败原因。

        只要存在一台物理上（总带宽/电源预算）就能容纳该请求的候选设备，
        失败就源于容量被在效租约占用 → 资源冲突；否则按“硬约束”归为
        带宽不足或电源不足。
        """
        physically_fits = any(
            d.total_bandwidth_khz + _EPS >= need_bw
            and (d.power_budget_w <= 0 or d.power_budget_w + _EPS >= need_power)
            for d in candidates
        )
        if physically_fits:
            return Reason.RESOURCE_CONFLICT
        if all(d.total_bandwidth_khz + _EPS < need_bw for d in candidates):
            return Reason.INSUFFICIENT_BANDWIDTH
        return Reason.INSUFFICIENT_POWER

    @staticmethod
    def _unavailable_detail(reason: str, band: Band) -> str:        return {
            Reason.UNSUPPORTED_CAPABILITY: f"现场没有支持 {band.value} 频段的设备",
            Reason.DEVICE_OFFLINE: f"支持 {band.value} 频段的设备全部掉线",
            Reason.NO_DEVICE_IN_RANGE: f"小组位置不在任何 {band.value} 设备的覆盖范围内",
        }.get(reason, "无可用候选设备")

    def _preemption_plan(
        self,
        candidates: list[Device],
        need_bw: float,
        need_power: float,
        priority: int,
    ) -> tuple[Device | None, list[Lease]]:
        """在每台候选设备上计算最小抢占集合，返回代价最低的方案。

        只抢占严格更低优先级的租约；同优先级先到期也不抢占（走冲突）。
        排序：优先级低 → 即将到期 → 占用带宽大（尽量少打扰小组）。
        """
        best: tuple[tuple, Device, list[Lease]] | None = None
        for device in candidates:
            victims = sorted(
                (l for l in self._active_leases(device.device_id) if l.priority < priority),
                key=lambda l: (l.priority, l.valid_until, -l.bandwidth_khz),
            )
            freed_bw = device.free_bandwidth_khz
            freed_power = device.free_power_w
            chosen: list[Lease] = []
            for victim in victims:
                if freed_bw + _EPS >= need_bw and freed_power + _EPS >= need_power:
                    break
                chosen.append(victim)
                freed_bw += victim.bandwidth_khz
                freed_power += victim.power_w
            if freed_bw + _EPS < need_bw or freed_power + _EPS < need_power:
                continue
            cost = (max((v.priority for v in chosen), default=-10**9), len(chosen),
                    device.device_id)
            if best is None or cost < best[0]:
                best = (cost, device, chosen)
        if best is None:
            return None, []
        return best[1], best[2]

    def _grant(
        self,
        operator: str,
        team: Team,
        device: Device,
        band: Band,
        bandwidth_khz: float,
        power_w: float,
        priority: int,
        ttl_seconds: float,
        request_id: str | None,
        reissue_from: str | None,
        *,
        preempted: list[Lease],
    ) -> Lease:
        now = self._now()
        lease = Lease(
            lease_id=new_id("lease"),
            team_id=team.team_id,
            device_id=device.device_id,
            band=band,
            bandwidth_khz=float(bandwidth_khz),
            power_w=float(power_w),
            priority=priority,
            valid_from=now,
            valid_until=now + ttl_seconds,
            operator=operator,
            request_id=request_id or new_id("req"),
            version=1,
            reissued_from=reissue_from,
            preempted_lease_ids=[v.lease_id for v in preempted],
            created_at=now,
        )
        device.used_bandwidth_khz += lease.bandwidth_khz
        device.used_power_w += lease.power_w
        if not hasattr(device, "_active"):
            device._active = []  # type: ignore[attr-defined]
        device._active.append(lease.lease_id)
        self._leases[lease.lease_id] = lease

        event_type = EventType.LEASE_REISSUED if reissue_from else EventType.LEASE_GRANTED
        details = {"lease": lease.to_dict(now)}
        if reissue_from:
            details["reissued_from"] = reissue_from
        if preempted:
            details["preempted_leases"] = [v.lease_id for v in preempted]
        self._emit(event_type, operator, details)

        self._requests[lease.request_id] = {
            "outcome": "granted",
            "lease_id": lease.lease_id,
        }
        self._persist()
        return lease

    def _reject(self, operator: str, err: OrchestratorError, context: dict) -> "OrchestratorError":
        """登记失败原因与事件，返回原异常供调用方 raise。"""
        record = self._record_failure(operator, err, context)
        if context.get("request_id"):
            self._requests[context["request_id"]] = {
                "outcome": "rejected",
                "failure_id": record["failure_id"],
                "reason": err.reason,
                "detail": err.detail,
                "competing_leases": getattr(err, "competing_leases", []),
                "candidate_devices": getattr(err, "candidate_devices", []),
            }
        self._persist()
        return err

    def _replay_outcome(self, request_id: str) -> dict:
        outcome = self._requests[request_id]
        if outcome["outcome"] == "granted":
            lease = self._leases[outcome["lease_id"]]
            return lease.to_dict(self._now())
        if outcome.get("competing_leases"):
            raise ResourceConflictError(
                outcome.get("detail", "请求此前已被裁决为冲突"),
                competing_leases=outcome.get("competing_leases", []),
                candidate_devices=outcome.get("candidate_devices", []),
                reason=outcome["reason"],
            )
        raise OrchestratorError(outcome.get("detail", "请求此前已被拒绝"),
                                reason=outcome["reason"])

    @staticmethod
    def _request_context(request_id, team_id, band, bw, priority) -> dict:
        return {"request_id": request_id, "team_id": team_id, "band": band.value,
                "bandwidth_khz": bw, "priority": priority}

    # ============================================================ 租约生命周期

    def extend_lease(
        self,
        operator: str,
        lease_id: str,
        extra_seconds: float,
        expected_version: int,
    ) -> dict:
        """延期（续约）：延长有效期，租约版本 +1。

        必须携带当前版本号（CAS）；现场另一个终端已经改过租约时，版本不
        匹配会抛出 :class:`LeaseVersionConflictError`。
        """
        operator = self._require_operator(operator)
        if extra_seconds <= 0:
            raise InvalidRequestError("延时时长必须为正数")
        with self._lock:
            self._sweep_expired()
            lease = self._leases.get(lease_id)
            if lease is None or lease.state != LeaseState.ACTIVE:
                raise LeaseNotActiveError(f"租约 {lease_id} 不在效状态，无法延期")
            if lease.version != expected_version:
                raise LeaseVersionConflictError(lease_id, expected_version, lease.version)
            old_until = lease.valid_until
            lease.valid_until = old_until + extra_seconds
            lease.version += 1
            self._emit(
                EventType.LEASE_EXTENDED, operator,
                {"lease_id": lease_id, "team_id": lease.team_id,
                 "device_id": lease.device_id,
                 "old_valid_until": old_until, "new_valid_until": lease.valid_until,
                 "version": lease.version, "expected_version": expected_version},
            )
            self._persist()
            return lease.to_dict(self._now())

    def release(self, operator: str, lease_id: str, expected_version: int | None = None) -> dict:
        """释放租约（小组撤离），带宽与电源立即归还。"""
        operator = self._require_operator(operator)
        with self._lock:
            self._sweep_expired()
            lease = self._leases.get(lease_id)
            if lease is None:
                raise LeaseNotActiveError(f"租约不存在：{lease_id}")
            if lease.state != LeaseState.ACTIVE:
                raise LeaseNotActiveError(
                    f"租约 {lease_id} 已不在效（{lease.state.value}）")
            if expected_version is not None and lease.version != expected_version:
                raise LeaseVersionConflictError(lease_id, expected_version, lease.version)
            self._terminate(lease, LeaseState.RELEASED, "RELEASED")
            self._emit(
                EventType.LEASE_RELEASED, operator,
                {"lease_id": lease_id, "team_id": lease.team_id,
                 "device_id": lease.device_id, "version": lease.version},
            )
            self._persist()
            return lease.to_dict(self._now())

    def reissue(
        self,
        operator: str,
        lease_id: str,
        *,
        ttl_seconds: float | None = None,
        priority: int | None = None,
        bandwidth_khz: float | None = None,
        request_id: str | None = None,
    ) -> dict:
        """重新申请：基于一份被抢占/掉线终止/过期的旧租约新开链路。

        缺省参数沿用旧租约；新租约带 ``reissued_from`` 血缘。资源不足时
        同样返回冲突，可改用 :meth:`defer_request` 延期排队。
        """
        operator = self._require_operator(operator)
        with self._lock:
            old = self._leases.get(lease_id)
            if old is None:
                raise InvalidRequestError(f"原租约不存在：{lease_id}")
            # ttl 缺省时无法从旧租约推断剩余时长，必须显式给出或用旧时长
            return self.request_link(
                operator,
                old.team_id,
                bandwidth_khz=bandwidth_khz if bandwidth_khz is not None else old.bandwidth_khz,
                band=old.band,
                priority=priority if priority is not None else old.priority,
                ttl_seconds=ttl_seconds if ttl_seconds is not None else (old.valid_until - old.valid_from),
                power_w=old.power_w,
                request_id=request_id,
                reissue_from=lease_id,
            )

    # ============================================================ 延期排队

    def defer_request(
        self,
        operator: str,
        team_id: str,
        *,
        bandwidth_khz: float,
        band: Band | str,
        priority: int,
        ttl_seconds: float,
        power_w: float = 0.0,
        note: str = "",
    ) -> dict:
        """把暂时无法满足的请求登记为延期（排队），不占用任何资源。"""
        operator = self._require_operator(operator)
        band = self._as_band(band)
        if bandwidth_khz <= 0 or ttl_seconds <= 0:
            raise InvalidRequestError("带宽与有效期必须为正数")
        with self._lock:
            if team_id not in self._teams:
                raise InvalidRequestError(f"未知小组：{team_id}", reason=Reason.TEAM_UNKNOWN)
            req = DeferredRequest(
                request_id=new_id("defer"),
                team_id=team_id,
                bandwidth_khz=float(bandwidth_khz),
                band=band,
                priority=priority,
                ttl_seconds=float(ttl_seconds),
                power_w=float(power_w),
                operator=operator,
                note=note,
                created_at=self._now(),
            )
            self._deferred[req.request_id] = req
            self._emit(EventType.REQUEST_DEFERRED, operator,
                       {"action": "DEFERRED", **req.to_dict()})
            self._persist()
            return req.to_dict()

    def fulfill_deferred(self, deferred_id: str, operator: str | None = None) -> dict:
        """尝试让一个延期请求立即落地为租约；失败时请求仍保持排队。"""
        with self._lock:
            req = self._deferred.get(deferred_id)
            if req is None:
                raise InvalidRequestError(f"未知延期请求：{deferred_id}")
            if req.state != "PENDING":
                raise InvalidRequestError(f"延期请求 {deferred_id} 已 {req.state}")
            operator = self._require_operator(operator or req.operator)
            try:
                granted = self.request_link(
                    operator, req.team_id,
                    bandwidth_khz=req.bandwidth_khz, band=req.band,
                    priority=req.priority, ttl_seconds=req.ttl_seconds,
                    power_w=req.power_w,
                    request_id=new_id("req"),
                )
            except OrchestratorError as err:
                req.last_failure = {"reason": err.reason, "detail": err.detail,
                                    "timestamp": self._now()}
                self._persist()
                raise
            req.state = "FULFILLED"
            req.fulfilled_lease_id = granted["lease_id"]
            self._emit(EventType.REQUEST_DEFERRED, operator,
                       {"action": "FULFILLED", "deferred_id": deferred_id,
                        "lease_id": granted["lease_id"]})
            self._persist()
            return granted

    def cancel_deferred(self, operator: str, deferred_id: str) -> dict:
        operator = self._require_operator(operator)
        with self._lock:
            req = self._deferred.get(deferred_id)
            if req is None:
                raise InvalidRequestError(f"未知延期请求：{deferred_id}")
            if req.state != "PENDING":
                raise InvalidRequestError(f"延期请求 {deferred_id} 已 {req.state}")
            req.state = "CANCELLED"
            self._emit(EventType.REQUEST_DEFERRED, operator,
                       {"action": "CANCELLED", "deferred_id": deferred_id})
            self._persist()
            return req.to_dict()

    # ============================================================ 查询接口

    def sweep_expired(self, operator: str = _SYSTEM) -> list[str]:
        """主动清理过期租约（查询接口也会自动触发）。"""
        with self._lock:
            ids = self._sweep_expired()
            if ids:
                self._persist()
            return ids

    def topology(self) -> dict:
        """当前拓扑：设备占用、在效租约、小组位置。"""
        with self._lock:
            expired = self._sweep_expired()
            if expired:
                self._persist()
            now = self._now()
            active = self._active_leases()
            return {
                "timestamp": now,
                "devices": {d.device_id: d.to_dict() for d in self._devices.values()},
                "teams": {t.team_id: t.to_dict() for t in self._teams.values()},
                "active_leases": [l.to_dict(now) for l in
                                  sorted(active, key=lambda l: (l.device_id, l.lease_id))],
                "deferred_pending": [d.to_dict() for d in self._deferred.values()
                                     if d.state == "PENDING"],
            }

    def get_lease(self, lease_id: str) -> dict:
        with self._lock:
            expired = self._sweep_expired()
            if expired:
                self._persist()
            lease = self._leases.get(lease_id)
            if lease is None:
                raise LeaseNotActiveError(f"租约不存在：{lease_id}")
            return lease.to_dict(self._now())

    def lease_history(
        self,
        *,
        team_id: str | None = None,
        device_id: str | None = None,
        states: Iterable[str] | None = None,
        limit: int | None = None,
    ) -> list[dict]:
        """租约历史（含已释放/过期/抢占/被拒之外的全部租约），按时间倒序。"""
        with self._lock:
            expired = self._sweep_expired()
            if expired:
                self._persist()
            now = self._now()
            state_set = {s.upper() for s in states} if states else None
            rows = [
                l.to_dict(now) for l in self._leases.values()
                if (team_id is None or l.team_id == team_id)
                and (device_id is None or l.device_id == device_id)
                and (state_set is None or l.state.value in state_set)
            ]
            rows.sort(key=lambda d: d["created_at"], reverse=True)
            return rows[:limit] if limit else rows

    def events(
        self,
        *,
        event_type: str | None = None,
        operator: str | None = None,
        team_id: str | None = None,
        limit: int | None = None,
    ) -> list[dict]:
        """资源变更事件查询（审计留痕，按序号倒序）。"""
        with self._lock:
            expired = self._sweep_expired()
            if expired:
                self._persist()
            rows = [e.to_dict() for e in self._events]
            if event_type:
                et = event_type.upper()
                rows = [r for r in rows if r["event_type"] == et]
            if operator:
                rows = [r for r in rows if r["operator"] == operator]
            if team_id:
                rows = [r for r in rows
                        if r["details"].get("team_id") == team_id
                        or r["details"].get("lease", {}).get("team_id") == team_id]
            rows.sort(key=lambda d: d["seq"], reverse=True)
            return rows[:limit] if limit else rows

    def failures(
        self,
        *,
        reason: str | None = None,
        team_id: str | None = None,
        limit: int | None = None,
    ) -> list[dict]:
        """失败原因台账：每次拒绝/冲突的原因、竞争租约与操作员。"""
        with self._lock:
            rows = list(self._failures)
            if reason:
                rows = [r for r in rows if r["reason"] == reason]
            if team_id:
                rows = [r for r in rows if r["context"].get("team_id") == team_id]
            rows.sort(key=lambda d: d["timestamp"], reverse=True)
            return rows[:limit] if limit else rows

    def deferred_requests(self, *, pending_only: bool = False) -> list[dict]:
        with self._lock:
            rows = [d.to_dict() for d in self._deferred.values()]
            if pending_only:
                rows = [d for d in rows if d["state"] == "PENDING"]
            rows.sort(key=lambda d: d["created_at"])
            return rows

    # ============================================================ 重启恢复

    def _restore(self) -> None:
        """从快照恢复：重建占用量，未过期租约继续计时，已过期的立刻清退。"""
        state = self._store.load()
        if state is None:
            self.ready = True
            return

        for d in state.get("devices", []):
            device = Device(
                device_id=d["device_id"],
                name=d["name"],
                bands=self._as_bands(d["bands"]),
                total_bandwidth_khz=d["total_bandwidth_khz"],
                power_budget_w=d.get("power_budget_w", 0.0),
                x=d.get("x", 0.0),
                y=d.get("y", 0.0),
                range_km=d.get("range_km", 0.0),
                online=d.get("online", True),
            )
            device._active = []  # type: ignore[attr-defined]
            self._devices[device.device_id] = device

        for t in state.get("teams", []):
            self._teams[t["team_id"]] = Team(
                team_id=t["team_id"], name=t["name"], x=t["x"], y=t["y"],
                operator=t["operator"],
                capabilities=self._as_bands(t.get("capabilities", ())),
            )

        for ld in state.get("leases", []):
            lease = Lease(
                lease_id=ld["lease_id"],
                team_id=ld["team_id"],
                device_id=ld["device_id"],
                band=Band(ld["band"]),
                bandwidth_khz=ld["bandwidth_khz"],
                power_w=ld.get("power_w", 0.0),
                priority=ld["priority"],
                valid_from=ld["valid_from"],
                valid_until=ld["valid_until"],
                operator=ld["operator"],
                request_id=ld["request_id"],
                state=LeaseState(ld["state"]),
                version=ld.get("version", 1),
                reissued_from=ld.get("reissued_from"),
                preempted_lease_ids=ld.get("preempted_leases", []),
                terminate_reason=ld.get("terminate_reason"),
                created_at=ld.get("created_at", ld["valid_from"]),
            )
            self._leases[lease.lease_id] = lease

        self._seq = state.get("seq", 0)
        for ed in state.get("events", []):
            self._events.append(Event(
                event_id=ed["event_id"], seq=ed["seq"], timestamp=ed["timestamp"],
                event_type=EventType(ed["event_type"]), operator=ed["operator"],
                details=ed.get("details", {}),
            ))
        self._requests = state.get("requests", {})
        self._failures = state.get("failures", [])
        self._deferred = {
            d["request_id"]: DeferredRequest.from_dict(d)
            for d in state.get("deferred", [])
        }

        # 关键：按当前墙上时钟裁决哪些租约仍在效
        now = self._now()
        recovered = 0
        expired = 0
        offline_devices = {did for did, d in self._devices.items() if not d.online}
        for lease in self._leases.values():
            if lease.state != LeaseState.ACTIVE:
                continue
            if now >= lease.valid_until:
                lease.state = LeaseState.EXPIRED
                lease.terminate_reason = "EXPIRED"
                expired += 1
                self._emit(EventType.LEASE_EXPIRED, _SYSTEM,
                           {"lease_id": lease.lease_id, "team_id": lease.team_id,
                            "device_id": lease.device_id, "during_restore": True})
            elif lease.device_id in offline_devices:
                # 设备在停机期间仍处于掉线状态：租约保持 ACTIVE 无法使用？
                # 按“掉线即不得占用”的原则，恢复时同样强制终止。
                lease.state = LeaseState.OFFLINE_TERMINATED
                lease.terminate_reason = Reason.DEVICE_WENT_OFFLINE
                expired += 1
                self._emit(EventType.LEASE_TERMINATED_OFFLINE, _SYSTEM,
                           {"lease_id": lease.lease_id, "team_id": lease.team_id,
                            "device_id": lease.device_id, "during_restore": True})
            else:
                device = self._devices[lease.device_id]
                device.used_bandwidth_khz += lease.bandwidth_khz
                device.used_power_w += lease.power_w
                device._active.append(lease.lease_id)  # type: ignore[attr-defined]
                recovered += 1

        self._emit(EventType.SERVICE_RESTORED, _SYSTEM,
                   {"recovered_active_leases": recovered,
                    "closed_during_restore": expired,
                    "restored_at": now})
        self._persist()
        self.ready = True


# 兼容原占位入口
Service = OrchestratorService
