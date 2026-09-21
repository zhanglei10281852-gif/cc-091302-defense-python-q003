"""应急通信车资源编排服务。

核心能力：
- 接收救援小组位置、任务优先级、所需带宽与终端频段能力，计算可行分配；
- 处理抢占（高优先级可抢占严格更低优先级租约）、延期（资源紧张时排队，
  资源释放后自动补分配）、释放与重新申请；
- 每条租约带绝对时间有效期 expires_at 与单调 version；过期租约或设备掉线/
  撤离后自动释放，不再占用信道与电源；
- 两个调度请求同时竞争同一信道时，后到者得到明确的 CONFLICT 结果
  （含阻塞租约），或按请求意愿进入延期队列；
- 所有资源变更以仅追加事件留痕并记录操作员身份，提供当前拓扑、租约历史、
  失败原因查询；
- 状态落原子快照，租约使用绝对时间戳，重启后恢复未过期租约并继续计时。

线程安全：所有公共写操作在同一把锁内完成“检查—写入—落盘”，调度请求
串行裁决，竞争结果确定可复现。
"""

from __future__ import annotations

import math
import threading
import time
from typing import Callable, Optional

from .models import (
    ALLOCATION_EVENTS,
    AllocationResult,
    AllocationStatus,
    Channel,
    Device,
    DeviceType,
    EventType,
    FailureRecord,
    Lease,
    LeaseStateError,
    LeaseStatus,
    NotFoundError,
    OrchestratorError,
    RejectReason,
    Request,
    SYSTEM_OPERATOR,
    VersionConflictError,
)
from .store import EventStore

# 缺省功耗估算：2.0 W/MHz 基础射频功耗 + 0.5 W/km 链路距离代价。
DEFAULT_W_PER_MHZ = 2.0
DEFAULT_W_PER_KM = 0.5

# 撤离覆盖范围后自动吊销租约的原因码。
REASON_OUT_OF_RANGE = "OUT_OF_RANGE"


class ResourceOrchestrator:
    def __init__(
        self,
        store: Optional[EventStore] = None,
        state_path: str = "data/orchestrator.state.json",
        time_func: Callable[[], float] = time.time,
        autoload: bool = True,
        watts_per_mhz: float = DEFAULT_W_PER_MHZ,
        watts_per_km: float = DEFAULT_W_PER_KM,
    ):
        self._store = store or EventStore(state_path)
        self._clock = time_func
        self._w_per_mhz = watts_per_mhz
        self._w_per_km = watts_per_km
        self._lock = threading.RLock()

        self._devices: dict[str, Device] = {}
        self._leases: dict[str, Lease] = {}
        self._deferred: list[Request] = []
        self._failures: list[FailureRecord] = []
        self._event_seq = 0
        self._lease_seq = 0

        self._reaper_thread: Optional[threading.Thread] = None
        self._reaper_stop = threading.Event()

        if autoload:
            self.load()

    # ------------------------------------------------------------------ #
    # 持久化与恢复
    # ------------------------------------------------------------------ #
    def load(self) -> None:
        """从快照与事件日志恢复状态，并按当前时间继续计时。"""
        with self._lock:
            state = self._store.load_state()
            self._devices = {
                d["id"]: Device.from_dict(d) for d in state.get("devices", [])
            }
            self._leases = {
                lid: Lease.from_dict(ld)
                for lid, ld in state.get("leases", {}).items()
            }
            self._deferred = [
                Request.from_dict(r) for r in state.get("deferred", [])
            ]
            self._event_seq = state.get("event_seq", 0)
            self._lease_seq = state.get("lease_seq", 0)

            # 重放快照之后的事件，补齐快照与日志之间的缺口；失败记录从
            # 全量事件日志重建（快照不保存，保证留痕唯一来源）。
            all_events = list(self._store.read_events())
            replayed = 0
            for event in all_events:
                if event.get("seq", 0) <= self._event_seq:
                    continue
                self._replay(event)
                replayed += 1
            self._failures = [
                FailureRecord(**event["failure"])
                for event in all_events
                if event.get("failure")
            ]

            # 按当前墙钟继续计时：停机期间到期的租约转为 EXPIRED，
            # 等待超时的延期申请作废；未过期租约原样恢复。
            reaped = self._reap(self._clock())
            if replayed or any(reaped.values()):
                self._save_snapshot()

    def _replay(self, event: dict) -> None:
        etype = event["type"]
        self._event_seq = max(self._event_seq, event.get("seq", 0))
        if etype == EventType.DEVICE_REGISTERED or etype == EventType.DEVICE_STATUS:
            device = Device.from_dict(event["device"])
            self._devices[device.id] = device
        elif etype in ALLOCATION_EVENTS and event.get("lease"):
            lease = Lease.from_dict(event["lease"])
            self._leases[lease.lease_id] = lease
            self._lease_seq = max(self._lease_seq, _lease_number(lease.lease_id))
        elif etype == EventType.REQUEST_DEFERRED:
            req = Request.from_dict(event["request"])
            if not any(r.request_id == req.request_id for r in self._deferred):
                self._deferred.append(req)
        elif etype in (EventType.REQUEST_REJECTED, "request.expired"):
            rid = event.get("request", {}).get("request_id")
            if rid:
                self._deferred = [
                    r for r in self._deferred if r.request_id != rid
                ]

    def _save_snapshot(self) -> None:
        state = {
            "schema": 1,
            "devices": [d.to_dict() for d in self._devices.values()],
            "leases": {lid: l.to_dict() for lid, l in self._leases.items()},
            "deferred": [r.to_dict() for r in self._deferred],
            "event_seq": self._event_seq,
            "lease_seq": self._lease_seq,
            "saved_at": self._clock(),
        }
        self._store.save_state(state)

    def _emit(
        self,
        etype: str,
        operator: str,
        now: float,
        payload: Optional[dict] = None,
        failure: Optional[FailureRecord] = None,
    ) -> dict:
        """记录一条资源变更事件（仅追加，带操作员身份）。"""
        self._event_seq += 1
        event = {
            "seq": self._event_seq,
            "ts": now,
            "type": etype,
            "operator": operator,
        }
        if payload:
            event.update(payload)
        if failure is not None:
            event["failure"] = failure.to_dict()
            self._failures.append(failure)
        self._store.append_event(event)
        return event

    # ------------------------------------------------------------------ #
    # 设备登记与状态
    # ------------------------------------------------------------------ #
    def register_device(self, device: Device, operator: str) -> Device:
        with self._lock:
            now = self._clock()
            if device.id in self._devices:
                raise OrchestratorError(f"device {device.id} already registered")
            self._devices[device.id] = device
            self._emit(
                EventType.DEVICE_REGISTERED,
                operator,
                now,
                {"device": device.to_dict()},
            )
            self._save_snapshot()
            return device

    def set_device_online(
        self, device_id: str, online: bool, operator: str
    ) -> Device:
        """报告设备上线/掉线。掉线设备上的活跃租约立即吊销。"""
        with self._lock:
            now = self._clock()
            device = self._require_device(device_id)
            if device.online == online:
                return device
            device.online = online
            self._emit(
                EventType.DEVICE_STATUS,
                operator,
                now,
                {"device": device.to_dict(), "changes": {"online": online}},
            )
            if not online:
                for lease in self._active_leases():
                    if (
                        lease.provider_device_id == device_id
                        or lease.terminal_device_id == device_id
                    ):
                        self._revoke_lease(
                            lease,
                            RejectReason.DEVICE_OFFLINE.value,
                            {"device_id": device_id},
                            now,
                        )
                self._pump_deferred(now)
            else:
                # 基站恢复上线可能满足排队中的申请。
                self._pump_deferred(now)
            self._save_snapshot()
            return device

    def report_position(
        self, terminal_device_id: str, x_km: float, y_km: float, operator: str
    ) -> Device:
        """上报小组终端位置；撤离出覆盖范围后其租约自动释放。"""
        with self._lock:
            now = self._clock()
            device = self._require_device(terminal_device_id)
            if device.type != DeviceType.TERMINAL.value:
                raise OrchestratorError(f"device {terminal_device_id} is not a terminal")
            device.x_km, device.y_km = x_km, y_km
            self._emit(
                EventType.DEVICE_STATUS,
                operator,
                now,
                {
                    "device": device.to_dict(),
                    "changes": {"x_km": x_km, "y_km": y_km},
                },
            )
            for lease in self._active_leases():
                if lease.terminal_device_id != terminal_device_id:
                    continue
                base = self._devices.get(lease.provider_device_id)
                if base is None or self._distance(device, base) > base.range_km:
                    self._revoke_lease(
                        lease,
                        REASON_OUT_OF_RANGE,
                        {"x_km": x_km, "y_km": y_km},
                        now,
                    )
            self._pump_deferred(now)
            self._save_snapshot()
            return device

    # ------------------------------------------------------------------ #
    # 申请 / 分配
    # ------------------------------------------------------------------ #
    def submit_request(self, req: Request, operator: str) -> AllocationResult:
        """提交一次链路调度申请。

        返回 AllocationResult：
        - GRANTED：分配成功，lease 带有效期与版本；
        - DEFERRED：资源暂不可得且允许延期，已进入排队；
        - CONFLICT：与其它调度请求竞争且不允许延期，blocking_leases 指明对手；
        - REJECTED：硬性前置条件不满足（设备掉线、无覆盖、能力不匹配等）。
        """
        with self._lock:
            now = self._clock()
            self._reap(now)

            if req.bandwidth_mhz <= 0:
                raise ValueError("bandwidth_mhz must be positive")
            if req.ttl_seconds <= 0:
                raise ValueError("ttl_seconds must be positive")

            req.submitted_by = operator
            req.submitted_at = now
            req.deferred_at = None

            terminal = self._devices.get(req.terminal_device_id)
            if terminal is None or terminal.type != DeviceType.TERMINAL.value:
                return self._hard_reject(
                    req, operator, now, RejectReason.NOT_FOUND,
                    {"terminal_device_id": req.terminal_device_id},
                )
            if not terminal.online:
                return self._hard_reject(
                    req, operator, now, RejectReason.DEVICE_OFFLINE,
                    {"terminal_device_id": terminal.id},
                )

            x, y = req.x_km, req.y_km
            if x is None or y is None:
                x, y = terminal.x_km, terminal.y_km
                req.x_km, req.y_km = x, y

            result = self._try_allocate(req, operator, now)
            self._save_snapshot()
            return result

    def _try_allocate(
        self, req: Request, operator: str, now: float
    ) -> AllocationResult:
        plans, blockers = self._build_plans(req)

        if plans:
            plans.sort(key=lambda p: p["score"])
            plan = plans[0]
            victims = plan["victims"]
            for victim in victims:
                self._preempt_lease(victim, req, now)
            lease = self._grant(req, plan, operator, now)
            return AllocationResult(
                status=AllocationStatus.GRANTED.value,
                request_id=req.request_id,
                lease=lease,
            )

        # 无可行方案：区分硬性拒绝与资源竞争。
        hard = self._hard_reason(req, blockers)
        if hard is not None:
            return self._hard_reject(req, operator, now, hard, blockers["detail"])

        blocking_ids = sorted(blockers["blocking_leases"])
        if req.allow_defer:
            req.deferred_at = now
            self._deferred.append(req)
            failure = FailureRecord(
                ts=now,
                kind="DEFERRED",
                reason=RejectReason.ALLOCATION_CONFLICT.value,
                operator=operator,
                request_id=req.request_id,
                group_id=req.group_id,
                detail={"blocking_leases": blocking_ids},
            )
            self._emit(
                EventType.REQUEST_DEFERRED,
                operator,
                now,
                {
                    "request": req.to_dict(),
                    "blocking_leases": blocking_ids,
                },
                failure,
            )
            return AllocationResult(
                status=AllocationStatus.DEFERRED.value,
                request_id=req.request_id,
                reason=RejectReason.ALLOCATION_CONFLICT.value,
                blocking_leases=blocking_ids,
                deferred_at=now,
            )

        failure = FailureRecord(
            ts=now,
            kind="REJECTED",
            reason=RejectReason.ALLOCATION_CONFLICT.value,
            operator=operator,
            request_id=req.request_id,
            group_id=req.group_id,
            detail={"blocking_leases": blocking_ids},
        )
        self._emit(
            EventType.REQUEST_REJECTED,
            operator,
            now,
            {"request": req.to_dict(), "blocking_leases": blocking_ids},
            failure,
        )
        return AllocationResult(
            status=AllocationStatus.CONFLICT.value,
            request_id=req.request_id,
            reason=RejectReason.ALLOCATION_CONFLICT.value,
            blocking_leases=blocking_ids,
        )

    def _build_plans(self, req: Request) -> tuple[list[dict], dict]:
        """枚举可行分配方案。每个方案记录需要抢占的受害租约。"""
        x, y = req.x_km, req.y_km
        terminal = self._terminal(req)
        plans: list[dict] = []
        blockers = {"blocking_leases": set(), "detail": {}}

        in_range_online = False
        in_range_any = False
        band_mismatch_everywhere = True
        all_channels_too_small = True
        power_unfit_everywhere = True
        contention_seen = False

        for base in self._devices.values():
            if base.type != DeviceType.BASE.value:
                continue
            dist = math.hypot(base.x_km - x, base.y_km - y)
            if dist > base.range_km:
                continue
            in_range_any = True
            if not base.online:
                continue
            in_range_online = True

            usable: list[tuple[Channel, float]] = []
            for ch in base.channels:
                if ch.bandwidth_mhz < req.bandwidth_mhz:
                    continue
                all_channels_too_small = False
                if ch.band not in terminal.supported_bands:
                    continue
                band_mismatch_everywhere = False
                usable.append((ch, dist))

            if not usable:
                continue

            active = [
                l
                for l in self._leases.values()
                if l.is_active_at(self._clock())
                and l.provider_device_id == base.id
            ]
            used_power = sum(l.power_w for l in active)

            for ch, dist in usable:
                need_power = (
                    req.power_w
                    if req.power_w is not None
                    else self.estimate_power_w(req.bandwidth_mhz, dist)
                )
                if need_power > base.power_budget_w:
                    # 单这一条链路就超过基站总电源预算，任何抢占都无济于事。
                    continue
                power_unfit_everywhere = False

                occupant = next(
                    (l for l in active if l.channel_id == ch.id), None
                )
                victims: list[Lease] = []
                feasible = True

                if occupant is not None:
                    if req.allow_preempt and occupant.priority < req.priority:
                        victims.append(occupant)
                    else:
                        feasible = False
                        contention_seen = True
                        blockers["blocking_leases"].add(occupant.lease_id)

                # 电源预算：必要时按“最低优先级、最早签发”顺序抢占腾功率。
                remaining = base.power_budget_w - used_power + sum(
                    v.power_w for v in victims
                )
                if remaining < need_power:
                    if not req.allow_preempt:
                        feasible = False
                        contention_seen = True
                        blockers["blocking_leases"].update(
                            l.lease_id for l in active
                        )
                    else:
                        candidates = [
                            l
                            for l in active
                            if l not in victims and l.priority < req.priority
                        ]
                        candidates.sort(
                            key=lambda l: (l.priority, l.issued_at, l.lease_id)
                        )
                        for cand in candidates:
                            if remaining >= need_power:
                                break
                            victims.append(cand)
                            remaining += cand.power_w
                        if remaining < need_power:
                            feasible = False
                            blockers["blocking_leases"].update(
                                l.lease_id for l in active
                            )

                if feasible:
                    worst_victim = max(
                        (v.priority for v in victims), default=-10**9
                    )
                    plans.append(
                        {
                            "base": base,
                            "channel": ch,
                            "distance_km": dist,
                            "power_w": need_power,
                            "victims": victims,
                            # 排序：少抢占 → 受害优先级低 → 优先空闲信道
                            # （抢占只为腾功率时保留受害信道的复用机会）
                            # → 距离近 → 设备/信道 id 稳定兜底。
                            "score": (
                                len(victims),
                                worst_victim,
                                1 if occupant is not None else 0,
                                dist,
                                base.id,
                                ch.id,
                            ),
                        }
                    )

        blockers["detail"] = {
            "in_range_any": in_range_any,
            "in_range_online": in_range_online,
            "band_mismatch_everywhere": band_mismatch_everywhere,
            "all_channels_too_small": all_channels_too_small,
            "power_unfit_everywhere": power_unfit_everywhere,
            "contention_seen": contention_seen,
        }
        return plans, blockers

    def _hard_reason(self, req: Request, blockers: dict) -> Optional[RejectReason]:
        d = blockers["detail"]
        if not d["in_range_any"]:
            return RejectReason.NO_DEVICE_IN_RANGE
        if not d["in_range_online"]:
            return RejectReason.DEVICE_OFFLINE
        if d["all_channels_too_small"]:
            return RejectReason.INSUFFICIENT_BANDWIDTH
        if d["band_mismatch_everywhere"]:
            return RejectReason.CAPABILITY_MISMATCH
        if d["power_unfit_everywhere"]:
            # 单条链路功耗超过所有可达基站的总电源预算，抢占也无法腾出。
            return RejectReason.INSUFFICIENT_POWER
        return None

    def _hard_reject(
        self,
        req: Request,
        operator: str,
        now: float,
        reason: RejectReason,
        detail: Optional[dict] = None,
    ) -> AllocationResult:
        failure = FailureRecord(
            ts=now,
            kind="REJECTED",
            reason=reason.value,
            operator=operator,
            request_id=req.request_id,
            group_id=req.group_id,
            detail=dict(detail or {}),
        )
        self._emit(
            EventType.REQUEST_REJECTED,
            operator,
            now,
            {"request": req.to_dict(), "reason": reason.value},
            failure,
        )
        return AllocationResult(
            status=AllocationStatus.REJECTED.value,
            request_id=req.request_id,
            reason=reason.value,
        )

    def _grant(
        self,
        req: Request,
        plan: dict,
        operator: str,
        now: float,
        automatic: bool = False,
    ) -> Lease:
        self._lease_seq += 1
        lease = Lease(
            lease_id=f"lease-{self._lease_seq}",
            request_id=req.request_id,
            group_id=req.group_id,
            terminal_device_id=req.terminal_device_id,
            provider_device_id=plan["base"].id,
            channel_id=plan["channel"].id,
            bandwidth_mhz=req.bandwidth_mhz,
            power_w=plan["power_w"],
            priority=req.priority,
            ttl_seconds=req.ttl_seconds,
            issued_at=now,
            expires_at=now + req.ttl_seconds,
            version=1,
            status=LeaseStatus.ACTIVE.value,
            granted_by=operator,
        )
        self._leases[lease.lease_id] = lease
        self._emit(
            EventType.LEASE_GRANTED,
            operator,
            now,
            {
                "lease": lease.to_dict(),
                "request_id": req.request_id,
                "automatic": automatic,
                "preempted": [v.lease_id for v in plan.get("victims", [])],
            },
        )
        return lease

    # ------------------------------------------------------------------ #
    # 续租 / 释放 / 抢占 / 吊销
    # ------------------------------------------------------------------ #
    def renew_lease(
        self,
        lease_id: str,
        ttl_seconds: Optional[float] = None,
        operator: str = SYSTEM_OPERATOR,
        expected_version: Optional[int] = None,
        bandwidth_mhz: Optional[float] = None,
    ) -> Lease:
        """续租并刷新有效期，version 自增；expected_version 构成乐观锁。"""
        with self._lock:
            now = self._clock()
            self._reap(now)
            lease = self._leases.get(lease_id)
            if lease is None:
                raise NotFoundError(f"lease {lease_id} not found")

            if expected_version is not None and expected_version != lease.version:
                self._op_rejected(
                    lease,
                    operator,
                    now,
                    RejectReason.VERSION_CONFLICT.value,
                    {"expected_version": expected_version},
                )
                self._save_snapshot()
                raise VersionConflictError(
                    lease_id, expected_version, lease.version
                )

            if lease.status != LeaseStatus.ACTIVE.value:
                self._op_rejected(
                    lease, operator, now,
                    RejectReason.LEASE_NOT_ACTIVE.value,
                    {"status": lease.status},
                )
                self._save_snapshot()
                raise LeaseStateError(
                    lease_id, lease.status, RejectReason.LEASE_NOT_ACTIVE.value
                )

            ttl = ttl_seconds if ttl_seconds is not None else lease.ttl_seconds
            if ttl <= 0:
                raise ValueError("ttl_seconds must be positive")
            new_bw = (
                bandwidth_mhz if bandwidth_mhz is not None else lease.bandwidth_mhz
            )
            if new_bw <= 0:
                raise ValueError("bandwidth_mhz must be positive")

            base = self._devices[lease.provider_device_id]
            terminal = self._devices[lease.terminal_device_id]
            if not base.online or not terminal.online:
                reason = RejectReason.DEVICE_OFFLINE.value
                self._op_rejected(
                    lease, operator, now, reason,
                    {"base_online": base.online, "terminal_online": terminal.online},
                )
                self._save_snapshot()
                raise OrchestratorError(
                    f"cannot renew: device offline (lease {lease_id})"
                )
            dist = math.hypot(base.x_km - terminal.x_km, base.y_km - terminal.y_km)
            if dist > base.range_km:
                self._op_rejected(
                    lease, operator, now, REASON_OUT_OF_RANGE,
                    {"distance_km": dist, "range_km": base.range_km},
                )
                self._save_snapshot()
                raise OrchestratorError(
                    f"cannot renew: terminal out of range (lease {lease_id})"
                )

            ch = next(c for c in base.channels if c.id == lease.channel_id)
            if new_bw > ch.bandwidth_mhz:
                self._op_rejected(
                    lease, operator, now,
                    RejectReason.INSUFFICIENT_BANDWIDTH.value,
                    {"requested_mhz": new_bw, "channel_mhz": ch.bandwidth_mhz},
                )
                self._save_snapshot()
                raise OrchestratorError(
                    f"channel {ch.id} cannot provide {new_bw} MHz"
                )

            new_power = self.estimate_power_w(new_bw, dist)
            used_other = sum(
                l.power_w
                for l in self._active_leases()
                if l.provider_device_id == base.id and l.lease_id != lease_id
            )
            if used_other + new_power > base.power_budget_w:
                self._op_rejected(
                    lease, operator, now,
                    RejectReason.INSUFFICIENT_POWER.value,
                    {"needed_w": new_power, "free_w": base.power_budget_w - used_other},
                )
                self._save_snapshot()
                raise OrchestratorError(
                    f"base {base.id} power budget cannot cover renewal"
                )

            lease.ttl_seconds = ttl
            lease.expires_at = now + ttl
            lease.bandwidth_mhz = new_bw
            lease.power_w = new_power
            lease.version += 1
            self._emit(
                EventType.LEASE_RENEWED,
                operator,
                now,
                {"lease": lease.to_dict()},
            )
            self._save_snapshot()
            return lease

    def release_lease(
        self,
        lease_id: str,
        operator: str,
        expected_version: Optional[int] = None,
    ) -> Lease:
        """主动释放租约。已 RELEASED 的重复释放幂等返回。"""
        with self._lock:
            now = self._clock()
            self._reap(now)
            lease = self._leases.get(lease_id)
            if lease is None:
                raise NotFoundError(f"lease {lease_id} not found")

            if expected_version is not None and expected_version != lease.version:
                self._op_rejected(
                    lease, operator, now,
                    RejectReason.VERSION_CONFLICT.value,
                    {"expected_version": expected_version},
                )
                self._save_snapshot()
                raise VersionConflictError(
                    lease_id, expected_version, lease.version
                )

            if lease.status == LeaseStatus.RELEASED.value:
                return lease
            if lease.status != LeaseStatus.ACTIVE.value:
                raise LeaseStateError(
                    lease_id, lease.status, RejectReason.LEASE_NOT_ACTIVE.value
                )

            lease.status = LeaseStatus.RELEASED.value
            self._emit(
                EventType.LEASE_RELEASED,
                operator,
                now,
                {"lease": lease.to_dict()},
            )
            self._pump_deferred(now)
            self._save_snapshot()
            return lease

    def retry_deferred(self, request_id: str, operator: str) -> AllocationResult:
        """重新申请：将一条延期申请立刻重新裁决一次。"""
        with self._lock:
            now = self._clock()
            self._reap(now)
            req = next(
                (r for r in self._deferred if r.request_id == request_id), None
            )
            if req is None:
                raise NotFoundError(f"deferred request {request_id} not found")
            self._deferred = [
                r for r in self._deferred if r.request_id != request_id
            ]
            terminal = self._require_device(req.terminal_device_id)
            req.x_km, req.y_km = terminal.x_km, terminal.y_km
            result = self._try_allocate(req, operator, now)
            self._save_snapshot()
            return result

    def _preempt_lease(self, victim: Lease, req: Request, now: float) -> None:
        victim.status = LeaseStatus.PREEMPTED.value
        failure = FailureRecord(
            ts=now,
            kind="PREEMPTED",
            reason="PREEMPTED",
            operator=req.submitted_by or SYSTEM_OPERATOR,
            lease_id=victim.lease_id,
            group_id=victim.group_id,
            request_id=victim.request_id,
            detail={
                "by_request_id": req.request_id,
                "by_group_id": req.group_id,
            },
        )
        self._emit(
            EventType.LEASE_PREEMPTED,
            req.submitted_by or SYSTEM_OPERATOR,
            now,
            {"lease": victim.to_dict(), "by_request_id": req.request_id},
            failure,
        )

    def _revoke_lease(
        self, lease: Lease, reason: str, detail: dict, now: float
    ) -> None:
        lease.status = LeaseStatus.REVOKED.value
        failure = FailureRecord(
            ts=now,
            kind="REVOKED",
            reason=reason,
            operator=SYSTEM_OPERATOR,
            lease_id=lease.lease_id,
            group_id=lease.group_id,
            request_id=lease.request_id,
            detail=dict(detail),
        )
        self._emit(
            EventType.LEASE_REVOKED,
            SYSTEM_OPERATOR,
            now,
            {"lease": lease.to_dict(), "reason": reason, "detail": detail},
            failure,
        )

    def _op_rejected(
        self,
        lease: Lease,
        operator: str,
        now: float,
        reason: str,
        detail: dict,
    ) -> None:
        failure = FailureRecord(
            ts=now,
            kind="VERSION_CONFLICT"
            if reason == RejectReason.VERSION_CONFLICT.value
            else "OP_REJECTED",
            reason=reason,
            operator=operator,
            lease_id=lease.lease_id,
            group_id=lease.group_id,
            detail=dict(detail),
        )
        self._emit(
            EventType.LEASE_OP_REJECTED,
            operator,
            now,
            {"lease_id": lease.lease_id, "reason": reason, "detail": detail},
            failure,
        )

    # ------------------------------------------------------------------ #
    # 过期与延期队列驱动
    # ------------------------------------------------------------------ #
    def reap(self) -> dict:
        """显式执行一次到期扫描。"""
        with self._lock:
            result = self._reap(self._clock())
            self._save_snapshot()
            return result

    def _reap(self, now: float) -> dict:
        expired_leases: list[str] = []
        for lease in list(self._leases.values()):
            if (
                lease.status == LeaseStatus.ACTIVE.value
                and lease.expires_at <= now
            ):
                lease.status = LeaseStatus.EXPIRED.value
                expired_leases.append(lease.lease_id)
                failure = FailureRecord(
                    ts=now,
                    kind="EXPIRED",
                    reason=RejectReason.LEASE_EXPIRED.value,
                    operator=SYSTEM_OPERATOR,
                    lease_id=lease.lease_id,
                    group_id=lease.group_id,
                    request_id=lease.request_id,
                    detail={"expires_at": lease.expires_at},
                )
                self._emit(
                    EventType.LEASE_EXPIRED,
                    SYSTEM_OPERATOR,
                    now,
                    {"lease": lease.to_dict()},
                    failure,
                )

        stale = [
            r for r in self._deferred
            if (r.submitted_at is not None and r.valid_until() <= now)
        ]
        for req in stale:
            self._deferred.remove(req)
            failure = FailureRecord(
                ts=now,
                kind="EXPIRED",
                reason=RejectReason.REQUEST_EXPIRED.value,
                operator=req.submitted_by or SYSTEM_OPERATOR,
                request_id=req.request_id,
                group_id=req.group_id,
                detail={"submitted_at": req.submitted_at},
            )
            self._emit(
                "request.expired",
                req.submitted_by or SYSTEM_OPERATOR,
                now,
                {"request": req.to_dict()},
                failure,
            )

        if expired_leases or stale:
            self._pump_deferred(now)

        return {
            "expired_leases": expired_leases,
            "expired_requests": [r.request_id for r in stale],
        }

    def _pump_deferred(self, now: float) -> list[str]:
        """资源变化后按“优先级高者先得、同优先级先到先得”补分配。"""
        granted: list[str] = []
        if not self._deferred:
            return granted
        ordered = sorted(
            self._deferred,
            key=lambda r: (-r.priority, r.deferred_at or r.submitted_at or now),
        )
        for req in ordered:
            if req not in self._deferred:
                continue  # 本轮已被处理（硬性拒绝出队）
            terminal = self._devices.get(req.terminal_device_id)
            if terminal is None or not terminal.online:
                self._deferred.remove(req)
                self._hard_reject(
                    req, req.submitted_by or SYSTEM_OPERATOR, now,
                    RejectReason.DEVICE_OFFLINE
                    if terminal is not None
                    else RejectReason.NOT_FOUND,
                    {"terminal_device_id": req.terminal_device_id},
                )
                continue
            req.x_km, req.y_km = terminal.x_km, terminal.y_km
            plans, _ = self._build_plans(req)
            if not plans:
                continue  # 仍不满足，继续排队
            self._deferred.remove(req)
            plans.sort(key=lambda p: p["score"])
            plan = plans[0]
            for victim in plan["victims"]:
                self._preempt_lease(victim, req, now)
            self._grant(
                req, plan, req.submitted_by or SYSTEM_OPERATOR, now,
                automatic=True,
            )
            granted.append(req.request_id)
        return granted

    def start_reaper(self, interval_seconds: float = 5.0) -> None:
        """启动后台守护线程，周期性清理到期租约/申请。"""
        if self._reaper_thread and self._reaper_thread.is_alive():
            return
        self._reaper_stop.clear()

        def _run() -> None:
            while not self._reaper_stop.wait(interval_seconds):
                with self._lock:
                    reaped = self._reap(self._clock())
                    if any(reaped.values()):
                        self._save_snapshot()

        self._reaper_thread = threading.Thread(
            target=_run, name="orchestrator-reaper", daemon=True
        )
        self._reaper_thread.start()

    def stop_reaper(self) -> None:
        self._reaper_stop.set()
        if self._reaper_thread:
            self._reaper_thread.join(timeout=2.0)

    # ------------------------------------------------------------------ #
    # 查询：拓扑 / 租约历史 / 失败原因 / 事件
    # ------------------------------------------------------------------ #
    def topology(self) -> dict:
        """返回当前拓扑：基站、信道占用、电源预算、终端与活跃租约。"""
        with self._lock:
            now = self._clock()
            self._reap(now)
            active = self._active_leases()

            bases = []
            for dev in self._devices.values():
                if dev.type != DeviceType.BASE.value:
                    continue
                base_leases = [
                    l for l in active if l.provider_device_id == dev.id
                ]
                used_power = sum(l.power_w for l in base_leases)
                occupant = {l.channel_id: l for l in base_leases}
                in_range_terminals = []
                for term in self._devices.values():
                    if term.type != DeviceType.TERMINAL.value:
                        continue
                    if self._distance(dev, term) <= dev.range_km:
                        in_range_terminals.append(term.id)
                bases.append(
                    {
                        "device_id": dev.id,
                        "name": dev.name,
                        "x_km": dev.x_km,
                        "y_km": dev.y_km,
                        "online": dev.online,
                        "range_km": dev.range_km,
                        "power_budget_w": dev.power_budget_w,
                        "power_used_w": round(used_power, 6),
                        "power_free_w": round(
                            dev.power_budget_w - used_power, 6
                        ),
                        "terminals_in_range": in_range_terminals,
                        "channels": [
                            {
                                "channel_id": c.id,
                                "band": c.band,
                                "frequency_mhz": c.frequency_mhz,
                                "bandwidth_mhz": c.bandwidth_mhz,
                                "occupied_by": occupant[c.id].lease_id
                                if c.id in occupant
                                else None,
                                "group_id": occupant[c.id].group_id
                                if c.id in occupant
                                else None,
                            }
                            for c in dev.channels
                        ],
                        "active_lease_ids": [l.lease_id for l in base_leases],
                    }
                )

            terminals = []
            for dev in self._devices.values():
                if dev.type != DeviceType.TERMINAL.value:
                    continue
                served = [
                    l for l in active if l.terminal_device_id == dev.id
                ]
                terminals.append(
                    {
                        "device_id": dev.id,
                        "name": dev.name,
                        "group_id": dev.group_id,
                        "x_km": dev.x_km,
                        "y_km": dev.y_km,
                        "online": dev.online,
                        "supported_bands": list(dev.supported_bands),
                        "serving_lease_ids": [l.lease_id for l in served],
                        "bases_in_range": [
                            b.id
                            for b in self._devices.values()
                            if b.type == DeviceType.BASE.value
                            and self._distance(dev, b) <= b.range_km
                        ],
                    }
                )

            return {
                "ts": now,
                "bases": sorted(bases, key=lambda b: b["device_id"]),
                "terminals": sorted(terminals, key=lambda t: t["device_id"]),
                "active_leases": [
                    self._lease_view(l, now)
                    for l in sorted(active, key=lambda l: l.issued_at)
                ],
                "deferred_requests": [r.request_id for r in self._deferred],
            }

    def active_leases(self, group_id: Optional[str] = None) -> list[dict]:
        with self._lock:
            now = self._clock()
            self._reap(now)
            leases = self._active_leases()
            if group_id is not None:
                leases = [l for l in leases if l.group_id == group_id]
            return [self._lease_view(l, now) for l in leases]

    def lease_history(self, group_id: Optional[str] = None) -> list[dict]:
        """租约全生命周期历史（含签发/续租版本/抢占/释放等事件）。"""
        with self._lock:
            events_by_lease: dict[str, list[dict]] = {}
            for ev in self._store.read_events():
                lid = ev.get("lease", {}).get("lease_id") or ev.get("lease_id")
                if lid:
                    events_by_lease.setdefault(lid, []).append(
                        {
                            "seq": ev["seq"],
                            "ts": ev["ts"],
                            "type": ev["type"],
                            "operator": ev["operator"],
                            "version": (ev.get("lease") or {}).get("version"),
                        }
                    )
            now = self._clock()
            rows = []
            for lease in self._leases.values():
                if group_id is not None and lease.group_id != group_id:
                    continue
                view = self._lease_view(lease, now)
                view["events"] = sorted(
                    events_by_lease.get(lease.lease_id, []),
                    key=lambda e: e["seq"],
                )
                rows.append(view)
            rows.sort(key=lambda l: l["issued_at"], reverse=True)
            return rows

    def failures(
        self,
        group_id: Optional[str] = None,
        request_id: Optional[str] = None,
        kind: Optional[str] = None,
    ) -> list[dict]:
        """失败原因查询：REJECTED/DEFERRED/PREEMPTED/REVOKED/EXPIRED/VERSION_CONFLICT。"""
        with self._lock:
            out = []
            for f in self._failures:
                if group_id is not None and f.group_id != group_id:
                    continue
                if request_id is not None and f.request_id != request_id:
                    continue
                if kind is not None and f.kind != kind:
                    continue
                out.append(f.to_dict())
            return out

    def events(self, event_type: Optional[str] = None) -> list[dict]:
        """查询资源变更事件原始日志。"""
        with self._lock:
            rows = self._store.read_events()
            if event_type is not None:
                rows = [e for e in rows if e["type"] == event_type]
            return rows

    def request_status(self, request_id: str) -> dict:
        with self._lock:
            now = self._clock()
            self._reap(now)
            leases = [
                self._lease_view(l, now)
                for l in self._leases.values()
                if l.request_id == request_id
            ]
            deferred = next(
                (r for r in self._deferred if r.request_id == request_id), None
            )
            return {
                "request_id": request_id,
                "deferred": deferred.to_dict() if deferred else None,
                "leases": leases,
                "failures": self.failures(request_id=request_id),
            }

    def get_lease(self, lease_id: str) -> dict:
        with self._lock:
            lease = self._leases.get(lease_id)
            if lease is None:
                raise NotFoundError(f"lease {lease_id} not found")
            return self._lease_view(lease, self._clock())

    # ------------------------------------------------------------------ #
    # 辅助
    # ------------------------------------------------------------------ #
    def estimate_power_w(self, bandwidth_mhz: float, distance_km: float) -> float:
        return round(
            self._w_per_mhz * bandwidth_mhz + self._w_per_km * distance_km, 6
        )

    def _active_leases(self) -> list[Lease]:
        now = self._clock()
        return [l for l in self._leases.values() if l.is_active_at(now)]

    def _lease_view(self, lease: Lease, now: float) -> dict:
        view = lease.to_dict()
        view["time_left_seconds"] = max(0.0, round(lease.expires_at - now, 6))
        view["active"] = lease.is_active_at(now)
        return view

    def _require_device(self, device_id: str) -> Device:
        device = self._devices.get(device_id)
        if device is None:
            raise NotFoundError(f"device {device_id} not found")
        return device

    def _terminal(self, req: Request) -> Device:
        return self._devices[req.terminal_device_id]

    @staticmethod
    def _distance(a: Device, b: Device) -> float:
        return math.hypot(a.x_km - b.x_km, a.y_km - b.y_km)


def _lease_number(lease_id: str) -> int:
    try:
        return int(lease_id.rsplit("-", 1)[1])
    except (IndexError, ValueError):
        return 0


# 兼容包入口骨架中的 Service 命名。
Service = ResourceOrchestrator
