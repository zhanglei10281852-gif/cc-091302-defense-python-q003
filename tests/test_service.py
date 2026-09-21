"""资源编排服务测试。

覆盖需求点：
1. 基础分配（位置覆盖、频段能力、带宽/电源约束）；
2. 容量不足返回明确冲突，且竞争租约可查；
3. 抢占（高优先级挤走低优先级，同优先级不抢占）；
4. 延期排队、释放、重新申请；
5. 租约带有效期与版本，延期走 CAS 版本校验；
6. 过期自动释放、设备掉线强制终止；
7. 请求幂等；
8. 并发竞争串行裁决（明确冲突）；
9. 事件审计与操作员留痕、失败原因查询；
10. 重启恢复未过期租约并继续计时、清退已过期租约。
"""

from __future__ import annotations

import threading
import unittest

from src.models import Band, LeaseState
from src.orchestrator_errors import (
    InvalidRequestError,
    LeaseNotActiveError,
    LeaseVersionConflictError,
    OrchestratorError,
    Reason,
    ResourceConflictError,
)
from src.service import OrchestratorService
from src.store import JsonSnapshotStore, MemoryStore
from tests.clock_helper import ManualClock


def bootstrap_service(store, clock: ManualClock | None = None):
    """首次建服：注册示范现场设备与小组。"""
    clock = clock or ManualClock()
    svc = OrchestratorService(store, clock)
    # 两台 UHF 设备：D1 容量 100kHz/电源 60W，覆盖原点；
    # D2 容量 100kHz，覆盖 (10, 0)；另注册一台 SAT 设备。
    svc.register_device("调度员甲", "D1", "山前中继",
                        ["UHF"], 100, power_budget_w=60, range_km=5)
    svc.register_device("调度员甲", "D2", "山后中继",
                        ["UHF"], 100, x=10, range_km=5)
    svc.register_device("调度员乙", "D3", "卫星终端",
                        ["SAT"], 200, x=0, range_km=99)
    svc.register_team("现场员丙", "T1", "搜救一队", 0, 0, capabilities=["UHF"])
    svc.register_team("现场员丙", "T2", "搜救二队", 0, 0, capabilities=["UHF"])
    svc.register_team("现场员丙", "T3", "搜救三队", 0, 0, capabilities=["UHF", "SAT"])
    return svc, clock


def make_service(tmp_path: str | None = None, clock: ManualClock | None = None):
    store = JsonSnapshotStore(f"{tmp_path}/state.json") if tmp_path else MemoryStore()
    return bootstrap_service(store, clock)


class BasicAllocationTests(unittest.TestCase):
    def setUp(self):
        self.svc, self.clock = make_service()

    def test_grant_carries_ttl_and_version(self):
        lease = self.svc.request_link(
            "调度员甲", "T1", bandwidth_khz=40, band="UHF", priority=5, ttl_seconds=300)
        self.assertEqual(lease["state"], "ACTIVE")
        self.assertEqual(lease["version"], 1)
        self.assertAlmostEqual(lease["ttl_seconds"], 300, places=3)
        self.assertEqual(lease["operator"], "调度员甲")

    def test_device_capacity_tracking(self):
        self.svc.request_link("甲", "T1", bandwidth_khz=40, band="UHF", priority=5, ttl_seconds=300)
        topo = self.svc.topology()
        d1 = topo["devices"]["D1"]
        self.assertAlmostEqual(d1["used_bandwidth_khz"], 40)
        self.assertAlmostEqual(d1["free_bandwidth_khz"], 60)
        self.assertEqual(d1["active_leases"], [l for l in [
            self.svc.lease_history(team_id="T1")[0]["lease_id"]]])

    def test_out_of_range_position(self):
        self.svc.register_team("丙", "TF", "远方队", 50, 50, capabilities=["UHF"])
        with self.assertRaises(ResourceConflictError) as cm:
            self.svc.request_link("甲", "TF", bandwidth_khz=10, band="UHF",
                                  priority=5, ttl_seconds=120)
        self.assertEqual(cm.exception.reason, Reason.NO_DEVICE_IN_RANGE)

    def test_unsupported_band(self):
        # T1 仅有 UHF 能力，申请 SAT 被能力校验拦截
        with self.assertRaises(OrchestratorError) as cm:
            self.svc.request_link("甲", "T1", bandwidth_khz=10, band="SAT",
                                  priority=5, ttl_seconds=120)
        self.assertEqual(cm.exception.reason, Reason.UNSUPPORTED_CAPABILITY)

    def test_power_budget_blocks(self):
        with self.assertRaises(ResourceConflictError) as cm:
            self.svc.request_link("甲", "T3", bandwidth_khz=10, band="UHF",
                                  priority=5, ttl_seconds=120, power_w=80)
        self.assertEqual(cm.exception.reason, Reason.INSUFFICIENT_POWER)
        # D1 候选但电源 60W < 80W；D2 距离 10 > 5 覆盖不到
        self.assertEqual(cm.exception.candidate_devices, ["D1"])
        self.assertEqual(cm.exception.competing_leases, [])


class ConflictAndPreemptionTests(unittest.TestCase):
    def setUp(self):
        self.svc, self.clock = make_service()

    def test_conflict_names_competing_lease(self):
        l1 = self.svc.request_link("甲", "T1", bandwidth_khz=80, band="UHF",
                                   priority=5, ttl_seconds=300)
        with self.assertRaises(ResourceConflictError) as cm:
            self.svc.request_link("甲", "T2", bandwidth_khz=40, band="UHF",
                                  priority=5, ttl_seconds=300)
        self.assertEqual(cm.exception.reason, Reason.RESOURCE_CONFLICT)
        self.assertEqual(cm.exception.competing_leases, [l1["lease_id"]])
        self.assertEqual(cm.exception.candidate_devices, ["D1"])

    def test_higher_priority_preempts(self):
        low = self.svc.request_link("甲", "T1", bandwidth_khz=80, band="UHF",
                                    priority=2, ttl_seconds=300)
        high = self.svc.request_link("调度员乙", "T2", bandwidth_khz=70, band="UHF",
                                     priority=9, ttl_seconds=300)
        self.assertEqual(high["device_id"], "D1")
        self.assertEqual(high["preempted_leases"], [low["lease_id"]])
        victim = self.svc.get_lease(low["lease_id"])
        self.assertEqual(victim["state"], "PREEMPTED")
        self.assertEqual(victim["terminate_reason"], "PREEMPTED")
        # 被抢占方资源已释放，容量归新高优先级租约
        d1 = self.svc.topology()["devices"]["D1"]
        self.assertAlmostEqual(d1["used_bandwidth_khz"], 70)

    def test_same_priority_does_not_preempt(self):
        self.svc.request_link("甲", "T1", bandwidth_khz=80, band="UHF",
                              priority=5, ttl_seconds=300)
        with self.assertRaises(ResourceConflictError):
            self.svc.request_link("乙", "T2", bandwidth_khz=70, band="UHF",
                                  priority=5, ttl_seconds=300)

    def test_lower_priority_cannot_preempt(self):
        self.svc.request_link("甲", "T1", bandwidth_khz=80, band="UHF",
                              priority=9, ttl_seconds=300)
        with self.assertRaises(ResourceConflictError) as cm:
            self.svc.request_link("乙", "T2", bandwidth_khz=70, band="UHF",
                                  priority=1, ttl_seconds=300)
        self.assertEqual(cm.exception.competing_leases != [], True)

    def test_preemption_chooses_minimal_victim_set(self):
        # D1 上三条低优先级租约 30/30/30，高优先级需要 55：
        # 按优先级相同、即将到期（这里都是同时刻，以带宽大的优先）→ 选两条
        self.svc.register_team("丙", "T4", "搜救四队", 0, 0, capabilities=["UHF"])
        l1 = self.svc.request_link("甲", "T1", bandwidth_khz=30, band="UHF",
                                   priority=1, ttl_seconds=100)
        self.clock.advance(1)
        l2 = self.svc.request_link("甲", "T2", bandwidth_khz=30, band="UHF",
                                   priority=1, ttl_seconds=100)
        self.clock.advance(1)
        self.svc.request_link("甲", "T4", bandwidth_khz=30, band="UHF",
                              priority=1, ttl_seconds=100)
        high = self.svc.request_link("乙", "T3", bandwidth_khz=55, band="UHF",
                                     priority=9, ttl_seconds=100)
        # 100 - 90 = 10 空闲，需要再释放 45；30+30 两条足够，且按规则选
        # 最先到期（valid_until 最小）的两条
        self.assertEqual(len(high["preempted_leases"]), 2)
        self.assertIn(l1["lease_id"], high["preempted_leases"])
        self.assertIn(l2["lease_id"], high["preempted_leases"])

    def test_best_fit_device_selection(self):
        # 在 (5,0) 处同时被 D1(range 5) 与 D2(中心10,range5) 覆盖
        self.svc.register_team("丙", "TM", "中段队", 5, 0, capabilities=["UHF"])
        self.svc.request_link("甲", "T1", bandwidth_khz=60, band="UHF",
                              priority=5, ttl_seconds=300)  # D1 剩 40
        lease = self.svc.request_link("甲", "TM", bandwidth_khz=30, band="UHF",
                                      priority=5, ttl_seconds=300)
        # 最佳适配：D1 剩 40 能放下 → D1
        self.assertEqual(lease["device_id"], "D1")

    def test_all_devices_full_conflict(self):
        self.svc.request_link("甲", "T1", bandwidth_khz=100, band="UHF",
                              priority=5, ttl_seconds=300)
        with self.assertRaises(ResourceConflictError) as cm:
            self.svc.request_link("甲", "T2", bandwidth_khz=10, band="UHF",
                                  priority=5, ttl_seconds=300)
        # 原点只能由 D1 覆盖（D2 距 10km > 5km）
        self.assertEqual(cm.exception.candidate_devices, ["D1"])


class LifecycleTests(unittest.TestCase):
    def setUp(self):
        self.svc, self.clock = make_service()

    def test_expiry_frees_capacity(self):
        lease = self.svc.request_link("甲", "T1", bandwidth_khz=90, band="UHF",
                                      priority=5, ttl_seconds=120)
        self.clock.advance(121)
        self.svc.sweep_expired()
        self.assertEqual(self.svc.get_lease(lease["lease_id"])["state"], "EXPIRED")
        # 容量回来了，新申请可以通过
        again = self.svc.request_link("甲", "T2", bandwidth_khz=100, band="UHF",
                                      priority=5, ttl_seconds=120)
        self.assertEqual(again["device_id"], "D1")

    def test_expired_lease_cannot_be_extended(self):
        lease = self.svc.request_link("甲", "T1", bandwidth_khz=10, band="UHF",
                                      priority=5, ttl_seconds=60)
        self.clock.advance(61)
        with self.assertRaises(LeaseNotActiveError) as cm:
            self.svc.extend_lease("甲", lease["lease_id"], 60, expected_version=1)
        self.assertEqual(cm.exception.reason, Reason.LEASE_NOT_ACTIVE)

    def test_extend_bumps_ttl_and_version(self):
        lease = self.svc.request_link("甲", "T1", bandwidth_khz=10, band="UHF",
                                      priority=5, ttl_seconds=60)
        extended = self.svc.extend_lease("甲", lease["lease_id"], 60,
                                         expected_version=1)
        self.assertEqual(extended["version"], 2)
        self.assertAlmostEqual(extended["ttl_seconds"], 120, places=3)
        # 旧版本号再操作 → 版本冲突
        with self.assertRaises(LeaseVersionConflictError):
            self.svc.extend_lease("甲", lease["lease_id"], 30, expected_version=1)
        v2 = self.svc.extend_lease("甲", lease["lease_id"], 30, expected_version=2)
        self.assertEqual(v2["version"], 3)

    def test_release_frees_resources(self):
        lease = self.svc.request_link("甲", "T1", bandwidth_khz=90, band="UHF",
                                      priority=5, ttl_seconds=300)
        self.svc.release("调度员丁", lease["lease_id"])
        self.assertEqual(self.svc.get_lease(lease["lease_id"])["state"], "RELEASED")
        d1 = self.svc.topology()["devices"]["D1"]
        self.assertAlmostEqual(d1["used_bandwidth_khz"], 0)
        # 重复释放报“不在效”
        with self.assertRaises(LeaseNotActiveError):
            self.svc.release("甲", lease["lease_id"])

    def test_release_version_check(self):
        lease = self.svc.request_link("甲", "T1", bandwidth_khz=10, band="UHF",
                                      priority=5, ttl_seconds=60)
        self.svc.extend_lease("甲", lease["lease_id"], 10, 1)
        with self.assertRaises(LeaseVersionConflictError):
            self.svc.release("甲", lease["lease_id"], expected_version=1)

    def test_device_offline_terminates_leases(self):
        l1 = self.svc.request_link("甲", "T1", bandwidth_khz=40, band="UHF",
                                   priority=5, ttl_seconds=300)
        self.svc.set_device_online("值班长", "D1", False)
        self.assertEqual(self.svc.get_lease(l1["lease_id"])["state"],
                         "OFFLINE_TERMINATED")
        d1 = self.svc.topology()["devices"]["D1"]
        self.assertAlmostEqual(d1["used_bandwidth_khz"], 0)
        # 掉线期间无法新分配
        with self.assertRaises(ResourceConflictError) as cm:
            self.svc.request_link("甲", "T2", bandwidth_khz=10, band="UHF",
                                  priority=5, ttl_seconds=300)
        self.assertEqual(cm.exception.reason, Reason.DEVICE_OFFLINE)
        # 恢复上线后可重新申请
        self.svc.set_device_online("值班长", "D1", True)
        new = self.svc.reissue("甲", l1["lease_id"], ttl_seconds=300)
        self.assertEqual(new["state"], "ACTIVE")
        self.assertEqual(new["reissued_from"], l1["lease_id"])

    def test_reissue_from_active_lease_rejected(self):
        l1 = self.svc.request_link("甲", "T1", bandwidth_khz=10, band="UHF",
                                   priority=5, ttl_seconds=300)
        with self.assertRaises(InvalidRequestError):
            self.svc.reissue("甲", l1["lease_id"], ttl_seconds=300)

    def test_reissue_after_preemption(self):
        low = self.svc.request_link("甲", "T1", bandwidth_khz=80, band="UHF",
                                    priority=1, ttl_seconds=300)
        self.svc.request_link("乙", "T2", bandwidth_khz=80, band="UHF",
                              priority=9, ttl_seconds=300)
        # T1 被抢占后立刻重新申请同样规格会再次冲突；SAT 频段 T1 不支持，
        # 故改为小带宽（D1 还剩 20）
        new = self.svc.reissue("甲", low["lease_id"], bandwidth_khz=15,
                               ttl_seconds=300)
        self.assertEqual(new["team_id"], "T1")
        self.assertEqual(new["reissued_from"], low["lease_id"])

    def test_defer_then_fulfill_after_release(self):
        l1 = self.svc.request_link("甲", "T1", bandwidth_khz=90, band="UHF",
                                   priority=5, ttl_seconds=300)
        defer = self.svc.defer_request("甲", "T2", bandwidth_khz=30, band="UHF",
                                       priority=5, ttl_seconds=300,
                                       note="等 T1 撤离")
        self.assertEqual(defer["state"], "PENDING")
        # 容量仍不足 → 尝试落地失败但请求保留
        with self.assertRaises(ResourceConflictError):
            self.svc.fulfill_deferred(defer["request_id"])
        self.assertEqual(
            self.svc.deferred_requests(pending_only=True)[0]["last_failure"]["reason"],
            Reason.RESOURCE_CONFLICT)
        # T1 撤离释放 → 落地成功
        self.svc.release("甲", l1["lease_id"])
        granted = self.svc.fulfill_deferred(defer["request_id"])
        self.assertEqual(granted["team_id"], "T2")
        self.assertEqual(
            self.svc.deferred_requests()[0]["state"], "FULFILLED")

    def test_cancel_deferred(self):
        defer = self.svc.defer_request("甲", "T2", bandwidth_khz=10, band="UHF",
                                       priority=5, ttl_seconds=300)
        self.svc.cancel_deferred("甲", defer["request_id"])
        self.assertEqual(self.svc.deferred_requests()[0]["state"], "CANCELLED")


class ConcurrencyTests(unittest.TestCase):
    def setUp(self):
        self.svc, self.clock = make_service()

    def test_concurrent_requests_are_arbitrated(self):
        """两个调度请求同时竞争：恰好一个成功，另一个拿到明确冲突。"""
        barrier = threading.Barrier(2)
        results: list = []

        def ask(team):
            barrier.wait()
            try:
                lease = self.svc.request_link(
                    "甲", team, bandwidth_khz=80, band="UHF",
                    priority=5, ttl_seconds=300)
                results.append(("ok", lease["team_id"]))
            except ResourceConflictError as err:
                results.append(("conflict", team, err.competing_leases))
            except OrchestratorError as err:
                results.append(("error", team, err.reason))

        t1 = threading.Thread(target=ask, args=("T1",))
        t2 = threading.Thread(target=ask, args=("T2",))
        t1.start(); t2.start(); t1.join(); t2.join()

        self.assertEqual(len(results), 2)
        oks = [r for r in results if r[0] == "ok"]
        conflicts = [r for r in results if r[0] == "conflict"]
        self.assertEqual(len(oks), 1)
        self.assertEqual(len(conflicts), 1)
        winner_id = oks[0][1]
        loser = conflicts[0]
        self.assertNotEqual(loser[1], winner_id)
        # 失败方拿到的竞争租约正是获胜方的租约
        winner_lease = self.svc.lease_history(team_id=winner_id, states=["ACTIVE"])[0]
        self.assertEqual(loser[2], [winner_lease["lease_id"]])

    def test_idempotent_request_id(self):
        lease = self.svc.request_link("甲", "T1", bandwidth_khz=40, band="UHF",
                                      priority=5, ttl_seconds=300,
                                      request_id="cmd-001")
        replay = self.svc.request_link("甲", "T1", bandwidth_khz=40, band="UHF",
                                       priority=5, ttl_seconds=300,
                                       request_id="cmd-001")
        self.assertEqual(replay["lease_id"], lease["lease_id"])
        self.assertEqual(len(self.svc.lease_history(team_id="T1")), 1)

    def test_rejected_request_is_idempotent_too(self):
        self.svc.request_link("甲", "T1", bandwidth_khz=90, band="UHF",
                              priority=5, ttl_seconds=300, request_id="cmd-a")
        with self.assertRaises(ResourceConflictError):
            self.svc.request_link("甲", "T2", bandwidth_khz=90, band="UHF",
                                  priority=5, ttl_seconds=300, request_id="cmd-b")
        with self.assertRaises(ResourceConflictError) as cm:
            self.svc.request_link("甲", "T2", bandwidth_khz=90, band="UHF",
                                  priority=5, ttl_seconds=300, request_id="cmd-b")
        self.assertEqual(cm.exception.reason, Reason.RESOURCE_CONFLICT)
        # 失败台账只有一条（重放不重复记账）
        self.assertEqual(len(self.svc.failures(team_id="T2")), 1)


class AuditAndQueryTests(unittest.TestCase):
    def setUp(self):
        self.svc, self.clock = make_service()

    def test_events_record_operator_and_changes(self):
        lease = self.svc.request_link("调度员甲", "T1", bandwidth_khz=40, band="UHF",
                                      priority=5, ttl_seconds=300)
        self.svc.release("调度员戊", lease["lease_id"])
        granted = self.svc.events(event_type="LEASE_GRANTED")
        self.assertEqual(len(granted), 1)
        self.assertEqual(granted[0]["operator"], "调度员甲")
        released = self.svc.events(event_type="LEASE_RELEASED")
        self.assertEqual(released[0]["operator"], "调度员戊")

    def test_failures_ledger(self):
        self.svc.register_team("丙", "TF", "远方", 50, 50, capabilities=["UHF"])
        for _ in range(2):
            try:
                self.svc.request_link("甲", "TF", bandwidth_khz=10, band="UHF",
                                      priority=5, ttl_seconds=120)
            except ResourceConflictError:
                pass
        fails = self.svc.failures(reason=Reason.NO_DEVICE_IN_RANGE)
        self.assertEqual(len(fails), 2)
        self.assertEqual(fails[0]["operator"], "甲")
        self.assertIn("context", fails[0])

    def test_lease_history_filter(self):
        l1 = self.svc.request_link("甲", "T1", bandwidth_khz=40, band="UHF",
                                   priority=5, ttl_seconds=300)
        self.svc.request_link("甲", "T2", bandwidth_khz=20, band="UHF",
                              priority=5, ttl_seconds=300)
        self.svc.release("甲", l1["lease_id"])
        hist = self.svc.lease_history(team_id="T1")
        self.assertEqual(len(hist), 1)
        released = self.svc.lease_history(states=["RELEASED"])
        self.assertEqual([h["lease_id"] for h in released], [l1["lease_id"]])

    def test_operator_required(self):
        with self.assertRaises(InvalidRequestError):
            self.svc.request_link("", "T1", bandwidth_khz=10, band="UHF",
                                  priority=5, ttl_seconds=120)

    def test_topology_snapshot(self):
        self.svc.request_link("甲", "T1", bandwidth_khz=40, band="UHF",
                              priority=5, ttl_seconds=300)
        topo = self.svc.topology()
        self.assertEqual(len(topo["active_leases"]), 1)
        self.assertEqual(set(topo["devices"]), {"D1", "D2", "D3"})
        self.assertEqual(topo["teams"]["T1"]["name"], "搜救一队")


class PersistenceTests(unittest.TestCase):
    def setUp(self):
        import tempfile
        self.tmp = tempfile.mkdtemp()
        self.path = f"{self.tmp}/state.json"
        self.clock = ManualClock()
        # 首次建服并注册现场设备/小组（后续“重启”只 new 服务，不再注册）
        self.svc = bootstrap_service(JsonSnapshotStore(self.path), self.clock)[0]

    def _restart(self, at: float | None = None):
        """模拟进程重启：同一快照路径、新的服务实例与时钟对象。"""
        if at is not None:
            self.clock.advance(at - self.clock.now())
        return OrchestratorService(
            JsonSnapshotStore(self.path), ManualClock(self.clock.now()))

    def test_restore_active_lease_keeps_timing(self):
        svc = self.svc
        lease = svc.request_link("甲", "T1", bandwidth_khz=40, band="UHF",
                                 priority=5, ttl_seconds=300)
        # “重启”：新服务实例 + 新时钟对象（100 秒后）
        svc2 = self._restart(at=self.clock.now() + 100)
        restored = svc2.get_lease(lease["lease_id"])
        self.assertEqual(restored["state"], "ACTIVE")
        self.assertAlmostEqual(restored["ttl_seconds"], 200, places=3)  # 继续计时
        self.assertEqual(restored["version"], 1)
        # 占用量已重建
        self.assertAlmostEqual(
            svc2.topology()["devices"]["D1"]["used_bandwidth_khz"], 40)

        # 再过 201 秒租约自然过期
        svc3 = self._restart(at=self.clock.now() + 201)
        self.assertEqual(
            svc3.get_lease(lease["lease_id"])["state"], "EXPIRED")

    def test_restore_closes_expired_and_frees_capacity(self):
        svc = self.svc
        lease = svc.request_link("甲", "T1", bandwidth_khz=90, band="UHF",
                                 priority=5, ttl_seconds=120)
        svc2 = self._restart(at=self.clock.now() + 200)  # 停机期间超过有效期
        self.assertEqual(svc2.get_lease(lease["lease_id"])["state"], "EXPIRED")
        self.assertAlmostEqual(
            svc2.topology()["devices"]["D1"]["used_bandwidth_khz"], 0)
        # 历史与事件仍可查
        self.assertEqual(len(svc2.lease_history()), 1)
        restore_events = svc2.events(event_type="SERVICE_RESTORED")
        self.assertEqual(len(restore_events), 1)
        self.assertEqual(restore_events[0]["details"]["recovered_active_leases"], 0)
        self.assertEqual(restore_events[0]["details"]["closed_during_restore"], 1)

    def test_restore_after_offline_does_not_hold_resources(self):
        svc = self.svc
        lease = svc.request_link("甲", "T1", bandwidth_khz=40, band="UHF",
                                 priority=5, ttl_seconds=300)
        svc.set_device_online("乙", "D1", False)
        svc2 = self._restart()
        self.assertEqual(svc2.get_lease(lease["lease_id"])["state"],
                         "OFFLINE_TERMINATED")

    def test_restore_preserves_deferred_and_idempotency(self):
        svc = self.svc
        svc.request_link("甲", "T1", bandwidth_khz=90, band="UHF",
                         priority=5, ttl_seconds=300, request_id="cmd-1")
        defer = svc.defer_request("甲", "T2", bandwidth_khz=30, band="UHF",
                                  priority=5, ttl_seconds=300)
        svc2 = self._restart()
        # 幂等表仍生效：重放返回原租约，不新开
        replay = svc2.request_link("甲", "T1", bandwidth_khz=90, band="UHF",
                                   priority=5, ttl_seconds=300, request_id="cmd-1")
        self.assertEqual(replay["team_id"], "T1")
        self.assertEqual(len(svc2.lease_history(team_id="T1")), 1)
        # 延期队列恢复
        pending = svc2.deferred_requests(pending_only=True)
        self.assertEqual([d["request_id"] for d in pending], [defer["request_id"]])


if __name__ == "__main__":
    unittest.main()
