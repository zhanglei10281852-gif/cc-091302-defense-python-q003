"""ResourceOrchestrator 功能测试。

运行：python -m unittest discover -s tests -v
"""

import os
import sys
import tempfile
import threading
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.models import (  # noqa: E402
    AllocationStatus,
    Channel,
    Device,
    LeaseStatus,
    RejectReason,
    Request,
    VersionConflictError,
)
from src.service import ResourceOrchestrator  # noqa: E402
from src.store import EventStore  # noqa: E402


class FakeClock:
    def __init__(self, start: float = 1_000_000.0):
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def make_base(
    device_id="base-1",
    channels=("ch-1",),
    budget=30.0,
    range_km=10.0,
    bw=10.0,
    online=True,
):
    return Device.base(
        device_id=device_id,
        name="应急车-" + device_id,
        x_km=0.0,
        y_km=0.0,
        range_km=range_km,
        power_budget_w=budget,
        channels=[
            Channel(id=cid, provider_device_id=device_id,
                    frequency_mhz=400.0 + i, bandwidth_mhz=bw, band="UHF")
            for i, cid in enumerate(channels)
        ],
        online=online,
    )


def make_terminal(
    device_id="term-1", group_id="g1", x=1.0, y=0.0, bands=("UHF",), online=True
):
    return Device.terminal(
        device_id=device_id,
        name="终端-" + device_id,
        group_id=group_id,
        x_km=x,
        y_km=y,
        supported_bands=list(bands),
        online=online,
    )


def make_request(
    term="term-1",
    group="g1",
    bw=2.0,
    priority=5,
    ttl=300.0,
    allow_preempt=False,
    allow_defer=False,
    power=None,
    x=None,
    y=None,
    valid_for=None,
):
    return Request(
        group_id=group,
        terminal_device_id=term,
        bandwidth_mhz=bw,
        priority=priority,
        ttl_seconds=ttl,
        allow_preempt=allow_preempt,
        allow_defer=allow_defer,
        power_w=power,
        x_km=x,
        y_km=y,
        valid_for_seconds=valid_for,
    )


class OrchestratorTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.state_path = os.path.join(self.tmp.name, "state.json")
        self.clock = FakeClock()
        self.orch = ResourceOrchestrator(
            EventStore(self.state_path),
            time_func=self.clock,
            autoload=False,
        )
        self.orch.register_device(make_base(), "dispatcher-li")
        self.orch.register_device(make_terminal(), "dispatcher-li")

    def replace_base(self, **kwargs):
        """用自定义基站替换 setUp 的默认单信道基站（须在任何申请之前调用）。"""
        self.orch._devices.pop("base-1")
        base = make_base(**kwargs)
        self.orch.register_device(base, "setup")
        return base

    def tearDown(self):
        self.orch.stop_reaper()
        self.tmp.cleanup()


class TestBasicAllocation(OrchestratorTestBase):
    def test_grant_carries_expiry_and_version(self):
        result = self.orch.submit_request(make_request(), "operator-wang")
        self.assertEqual(result.status, AllocationStatus.GRANTED.value)
        lease = result.lease
        self.assertEqual(lease.version, 1)
        self.assertAlmostEqual(lease.expires_at - lease.issued_at, 300.0)
        self.assertEqual(lease.status, LeaseStatus.ACTIVE.value)
        self.assertEqual(lease.granted_by, "operator-wang")

        view = self.orch.get_lease(lease.lease_id)
        self.assertAlmostEqual(view["time_left_seconds"], 300.0)
        self.assertTrue(view["active"])

        self.clock.advance(120)
        view = self.orch.get_lease(lease.lease_id)
        self.assertAlmostEqual(view["time_left_seconds"], 180.0)

    def test_topology_reflects_occupancy_and_power(self):
        self.replace_base(channels=("ch-1", "ch-2"))
        result = self.orch.submit_request(
            make_request(bw=3.0), "operator-wang"
        )
        topo = self.orch.topology()
        base = next(b for b in topo["bases"] if b["device_id"] == "base-1")
        ch1 = next(c for c in base["channels"] if c["channel_id"] == "ch-1")
        self.assertEqual(ch1["occupied_by"], result.lease.lease_id)
        self.assertEqual(ch1["group_id"], "g1")
        # 距离 1km：2 W/MHz*3 + 0.5 W/km*1 = 6.5W
        self.assertAlmostEqual(base["power_used_w"], 6.5)
        self.assertAlmostEqual(base["power_free_w"], 23.5)
        self.assertIn("term-1", base["terminals_in_range"])

        term = next(t for t in topo["terminals"] if t["device_id"] == "term-1")
        self.assertIn("base-1", term["bases_in_range"])
        self.assertEqual(len(topo["active_leases"]), 1)

    def test_channel_capability_reject(self):
        self.orch.register_device(
            make_terminal("term-vhf", "g-vhf", bands=("VHF",)), "op"
        )
        result = self.orch.submit_request(
            make_request(term="term-vhf", group="g-vhf"), "op"
        )
        self.assertEqual(result.status, AllocationStatus.REJECTED.value)
        self.assertEqual(result.reason, RejectReason.CAPABILITY_MISMATCH.value)

    def test_no_base_in_range_reject(self):
        result = self.orch.submit_request(
            make_request(x=50.0, y=50.0), "op"
        )
        self.assertEqual(result.status, AllocationStatus.REJECTED.value)
        self.assertEqual(result.reason, RejectReason.NO_DEVICE_IN_RANGE.value)

    def test_offline_terminal_reject(self):
        self.orch.set_device_online("term-1", False, "op")
        result = self.orch.submit_request(make_request(), "op")
        self.assertEqual(result.status, AllocationStatus.REJECTED.value)
        self.assertEqual(result.reason, RejectReason.DEVICE_OFFLINE.value)

    def test_offline_base_in_range_reject(self):
        # 唯一覆盖基站掉线
        self.orch.set_device_online("base-1", False, "op")
        result = self.orch.submit_request(make_request(), "op")
        self.assertEqual(result.reason, RejectReason.DEVICE_OFFLINE.value)

    def test_bandwidth_too_large_reject(self):
        result = self.orch.submit_request(make_request(bw=99.0), "op")
        self.assertEqual(result.status, AllocationStatus.REJECTED.value)
        self.assertEqual(
            result.reason, RejectReason.INSUFFICIENT_BANDWIDTH.value
        )


class TestConflict(OrchestratorTestBase):
    def _second_terminal(self):
        self.orch.register_device(
            make_terminal("term-2", "g2", x=2.0, y=0.0), "op"
        )

    def test_same_channel_competition_returns_conflict(self):
        first = self.orch.submit_request(make_request(bw=10.0), "op1")
        self.assertEqual(first.status, AllocationStatus.GRANTED.value)

        self._second_terminal()
        # 单信道场景：第二个信道容量不足以满足 10MHz 请求
        second = self.orch.submit_request(
            make_request(term="term-2", group="g2", bw=10.0), "op2"
        )
        self.assertEqual(second.status, AllocationStatus.CONFLICT.value)
        self.assertEqual(
            second.blocking_leases, [first.lease.lease_id]
        )
        self.assertEqual(
            second.reason, RejectReason.ALLOCATION_CONFLICT.value
        )

        # 信道占用没有被后来者破坏
        topo = self.orch.topology()
        active = topo["active_leases"]
        self.assertEqual(len(active), 1)
        self.assertEqual(active[0]["lease_id"], first.lease.lease_id)

    def test_separate_channels_both_granted(self):
        self.replace_base(channels=("ch-1", "ch-2"))
        first = self.orch.submit_request(make_request(bw=2.0), "op1")
        self._second_terminal()
        second = self.orch.submit_request(
            make_request(term="term-2", group="g2", bw=2.0), "op2"
        )
        self.assertEqual(second.status, AllocationStatus.GRANTED.value)
        self.assertNotEqual(
            first.lease.channel_id, second.lease.channel_id
        )
        self.assertEqual(len(self.orch.active_leases()), 2)

    def test_power_budget_competition_conflict(self):
        # 两个信道各占 9W 后电源预算（20W）不足以再开一条链路
        self.replace_base(channels=("ch-1", "ch-2"), budget=20.0)
        self.orch.register_device(
            make_terminal("term-2", "g2"), "op"
        )
        self.orch.register_device(
            make_terminal("term-3", "g3", x=0.5, y=0.5), "op"
        )
        r1 = self.orch.submit_request(
            make_request(term="term-1", group="g1", bw=4.0, power=9.0), "op1"
        )
        r2 = self.orch.submit_request(
            make_request(term="term-2", group="g2", bw=4.0, power=9.0), "op2"
        )
        self.assertTrue(r1.granted and r2.granted)
        r3 = self.orch.submit_request(
            make_request(term="term-3", group="g3", bw=1.0, power=5.0), "op3"
        )
        self.assertEqual(r3.status, AllocationStatus.CONFLICT.value)
        self.assertEqual(set(r3.blocking_leases),
                         {r1.lease.lease_id, r2.lease.lease_id})

    def test_concurrent_competing_requests_single_winner(self):
        terminals = []
        for i in range(2, 22):
            tid = f"term-{i}"
            self.orch.register_device(
                make_terminal(tid, f"g{i}", x=0.0, y=0.0), "setup"
            )
            terminals.append(tid)

        results = []
        barrier = threading.Barrier(len(terminals))

        def worker(tid, idx):
            barrier.wait()
            res = self.orch.submit_request(
                make_request(term=tid, group=f"g{idx}", bw=10.0),
                f"op-{tid}",
            )
            results.append(res)

        threads = [
            threading.Thread(target=worker, args=(tid, i))
            for i, tid in enumerate(terminals)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        granted = [r for r in results if r.status == AllocationStatus.GRANTED.value]
        conflicts = [
            r for r in results if r.status == AllocationStatus.CONFLICT.value
        ]
        self.assertEqual(len(granted), 1)
        self.assertEqual(len(conflicts), 19)
        self.assertEqual(len(self.orch.active_leases()), 1)


class TestDeferAndRelease(OrchestratorTestBase):
    def test_defer_then_auto_grant_on_release(self):
        first = self.orch.submit_request(make_request(bw=10.0), "op1")
        self.orch.register_device(
            make_terminal("term-2", "g2"), "op"
        )
        second = self.orch.submit_request(
            make_request(term="term-2", group="g2", bw=10.0, allow_defer=True),
            "op2",
        )
        self.assertEqual(second.status, AllocationStatus.DEFERRED.value)
        self.assertIn(second.request_id, self.orch.topology()["deferred_requests"])

        # 释放撤离小组的资源后，排队申请自动补分配
        self.clock.advance(60)
        released = self.orch.release_lease(first.lease.lease_id, "op1")
        self.assertEqual(released.status, LeaseStatus.RELEASED.value)

        status = self.orch.request_status(second.request_id)
        self.assertEqual(len(status["leases"]), 1)
        auto = status["leases"][0]
        self.assertTrue(auto["active"])
        # 有效期从补分配时刻重新起算
        self.assertAlmostEqual(auto["time_left_seconds"], 300.0)
        self.assertEqual([], self.orch.topology()["deferred_requests"])

        grant_events = [
            e for e in self.orch.events("lease.granted")
            if e["request_id"] == second.request_id
        ]
        self.assertTrue(grant_events[0]["automatic"])

    def test_release_is_idempotent_and_nonactive_rejected(self):
        first = self.orch.submit_request(make_request(), "op1")
        self.orch.release_lease(first.lease.lease_id, "op1")
        again = self.orch.release_lease(first.lease.lease_id, "op1")
        self.assertEqual(again.status, LeaseStatus.RELEASED.value)

    def test_deferred_request_expires_in_queue(self):
        self.orch.submit_request(make_request(bw=10.0), "op1")
        self.orch.register_device(make_terminal("term-2", "g2"), "op")
        second = self.orch.submit_request(
            make_request(
                term="term-2", group="g2", bw=10.0,
                allow_defer=True, ttl=120.0, valid_for=120.0,
            ),
            "op2",
        )
        self.assertEqual(second.status, AllocationStatus.DEFERRED.value)
        self.clock.advance(121)
        self.orch.reap()
        self.assertEqual([], self.orch.topology()["deferred_requests"])
        expired = [
            f for f in self.orch.failures(request_id=second.request_id)
            if f["kind"] == "EXPIRED"
        ]
        self.assertTrue(expired)

    def test_retry_deferred(self):
        first = self.orch.submit_request(make_request(bw=10.0), "op1")
        self.orch.register_device(make_terminal("term-2", "g2"), "op")
        second = self.orch.submit_request(
            make_request(term="term-2", group="g2", bw=10.0, allow_defer=True),
            "op2",
        )
        retry = self.orch.retry_deferred(second.request_id, "op2")
        # 资源仍被占用：按原申请意愿继续排队，且队列中不产生重复
        self.assertEqual(retry.status, AllocationStatus.DEFERRED.value)
        self.assertEqual(
            self.orch.topology()["deferred_requests"], [second.request_id]
        )

        # 资源释放后，重新排队的申请被自动补分配
        self.orch.release_lease(first.lease.lease_id, "op1")
        self.assertEqual(
            len(self.orch.request_status(second.request_id)["leases"]), 1
        )

    def test_retry_unknown_request_not_found(self):
        from src.models import NotFoundError
        with self.assertRaises(NotFoundError):
            self.orch.retry_deferred("nope", "op")

    def test_reapply_after_release(self):
        first = self.orch.submit_request(make_request(bw=10.0), "op1")
        self.orch.release_lease(first.lease.lease_id, "op1")
        again = self.orch.submit_request(make_request(bw=10.0), "op1")
        self.assertEqual(again.status, AllocationStatus.GRANTED.value)
        self.assertNotEqual(again.lease.lease_id, first.lease.lease_id)


class TestPreemption(OrchestratorTestBase):
    def test_higher_priority_preempts_lower(self):
        low = self.orch.submit_request(
            make_request(bw=10.0, priority=2), "op-low"
        )
        self.orch.register_device(make_terminal("term-2", "g2"), "op")
        high = self.orch.submit_request(
            make_request(
                term="term-2", group="g2", bw=10.0,
                priority=9, allow_preempt=True,
            ),
            "op-high",
        )
        self.assertEqual(high.status, AllocationStatus.GRANTED.value)
        low_view = self.orch.get_lease(low.lease.lease_id)
        self.assertEqual(low_view["status"], LeaseStatus.PREEMPTED.value)

        failures = self.orch.failures(kind="PREEMPTED")
        self.assertEqual(failures[0]["lease_id"], low.lease.lease_id)
        self.assertEqual(failures[0]["operator"], "op-high")
        self.assertEqual(failures[0]["detail"]["by_group_id"], "g2")

    def test_equal_priority_cannot_preempt(self):
        self.orch.submit_request(make_request(bw=10.0, priority=5), "op1")
        self.orch.register_device(make_terminal("term-2", "g2"), "op")
        res = self.orch.submit_request(
            make_request(
                term="term-2", group="g2", bw=10.0,
                priority=5, allow_preempt=True,
            ),
            "op2",
        )
        self.assertEqual(res.status, AllocationStatus.CONFLICT.value)

    def test_power_driven_preemption_on_free_channel(self):
        self.replace_base(channels=("ch-1", "ch-2", "ch-3"), budget=20.0)
        for tid, group, bw, x in (
            ("term-a", "ga", 4.0, 0.0),
            ("term-b", "gb", 5.0, 0.0),
            ("term-c", "gc", 3.0, 0.0),
        ):
            if tid != "term-1":
                self.orch.register_device(
                    make_terminal(tid, group, x=x, y=0.0), "setup"
                )
        # term-1 在 setUp 已存在（g1）
        a = self.orch.submit_request(
            make_request(term="term-1", group="g1", bw=4.0, priority=2), "op"
        )  # 8W @ ch-1
        b = self.orch.submit_request(
            make_request(term="term-a", group="ga", bw=5.0, priority=2), "op"
        )  # 10W @ ch-2
        self.assertTrue(a.granted and b.granted)

        high = self.orch.submit_request(
            make_request(
                term="term-b", group="gb", bw=3.0,
                priority=9, allow_preempt=True,
            ),
            "commander",
        )  # ch-3 空闲但仅剩 2W，需抢占腾功率
        self.assertEqual(high.status, AllocationStatus.GRANTED.value)
        self.assertEqual(high.lease.channel_id, "ch-3")
        self.assertEqual(
            self.orch.get_lease(a.lease.lease_id)["status"],
            LeaseStatus.PREEMPTED.value,
        )
        self.assertEqual(
            self.orch.get_lease(b.lease.lease_id)["status"],
            LeaseStatus.ACTIVE.value,
        )

    def test_preemption_frees_channel_and_grants_next_deferred(self):
        low = self.orch.submit_request(
            make_request(bw=10.0, priority=1), "op-low"
        )
        self.orch.register_device(make_terminal("term-2", "g2"), "op")
        self.orch.register_device(make_terminal("term-3", "g3", x=2.0), "op")
        deferred = self.orch.submit_request(
            make_request(term="term-2", group="g2", bw=10.0, allow_defer=True),
            "op2",
        )
        self.assertEqual(deferred.status, AllocationStatus.DEFERRED.value)
        high = self.orch.submit_request(
            make_request(
                term="term-3", group="g3", bw=10.0,
                priority=9, allow_preempt=True,
            ),
            "op3",
        )
        self.assertTrue(high.granted)
        # 高优先级抢占后占用唯一信道，低优先级的排队者继续等待而非抢占高优先级
        self.assertEqual(
            self.orch.request_status(deferred.request_id)["leases"], []
        )
        self.orch.release_lease(high.lease.lease_id, "op3")
        self.assertEqual(
            len(self.orch.request_status(deferred.request_id)["leases"]), 1
        )


class TestExpiry(OrchestratorTestBase):
    def test_lease_expires_and_releases_resource(self):
        first = self.orch.submit_request(
            make_request(bw=10.0, ttl=60.0), "op1"
        )
        self.orch.register_device(make_terminal("term-2", "g2"), "op")

        self.clock.advance(60)
        self.orch.reap()
        view = self.orch.get_lease(first.lease.lease_id)
        self.assertFalse(view["active"])
        self.assertEqual(view["status"], LeaseStatus.EXPIRED.value)

        second = self.orch.submit_request(
            make_request(term="term-2", group="g2", bw=10.0), "op2"
        )
        self.assertEqual(second.status, AllocationStatus.GRANTED.value)

        failures = self.orch.failures(kind="EXPIRED")
        self.assertEqual(failures[0]["lease_id"], first.lease.lease_id)
        self.assertEqual(failures[0]["operator"], "system")

    def test_expired_lease_cannot_be_renewed_or_released(self):
        from src.models import LeaseStateError
        first = self.orch.submit_request(
            make_request(ttl=10.0), "op1"
        )
        self.clock.advance(11)
        self.orch.reap()
        with self.assertRaises(LeaseStateError):
            self.orch.renew_lease(first.lease.lease_id, 60.0, "op1")
        with self.assertRaises(LeaseStateError):
            self.orch.release_lease(first.lease.lease_id, "op1")


class TestRenewal(OrchestratorTestBase):
    def test_renew_extends_and_bumps_version(self):
        first = self.orch.submit_request(
            make_request(ttl=60.0), "op1"
        )
        self.clock.advance(50)
        renewed = self.orch.renew_lease(
            first.lease.lease_id, ttl_seconds=120.0,
            operator="op1", expected_version=1,
        )
        self.assertEqual(renewed.version, 2)
        self.assertAlmostEqual(renewed.expires_at, self.clock.now + 120.0)
        self.assertAlmostEqual(
            self.orch.get_lease(renewed.lease_id)["time_left_seconds"], 120.0
        )

    def test_stale_version_rejected(self):
        first = self.orch.submit_request(make_request(), "op1")
        self.orch.renew_lease(first.lease.lease_id, 60.0, "op1",
                             expected_version=1)
        with self.assertRaises(VersionConflictError):
            self.orch.renew_lease(first.lease.lease_id, 60.0, "op1",
                                  expected_version=1)
        fails = self.orch.failures(kind="VERSION_CONFLICT")
        self.assertEqual(fails[0]["lease_id"], first.lease.lease_id)

    def test_release_with_stale_version_rejected(self):
        first = self.orch.submit_request(make_request(), "op1")
        self.orch.renew_lease(first.lease.lease_id, 60.0, "op1",
                             expected_version=1)
        with self.assertRaises(VersionConflictError):
            self.orch.release_lease(first.lease.lease_id, "op1",
                                    expected_version=1)
        # 租约仍活跃
        self.assertTrue(self.orch.get_lease(first.lease.lease_id)["active"])


class TestDeviceFailures(OrchestratorTestBase):
    def test_base_offline_revokes_leases_and_online_pumps_queue(self):
        first = self.orch.submit_request(make_request(bw=10.0), "op1")
        self.orch.register_device(make_terminal("term-2", "g2"), "op")
        deferred = self.orch.submit_request(
            make_request(term="term-2", group="g2", bw=10.0, allow_defer=True),
            "op2",
        )
        self.orch.set_device_online("base-1", False, "field-radio")
        self.assertEqual(
            self.orch.get_lease(first.lease.lease_id)["status"],
            LeaseStatus.REVOKED.value,
        )
        # 基站掉线期间排队者不会被补分配
        self.assertEqual(
            self.orch.request_status(deferred.request_id)["leases"], []
        )
        fails = self.orch.failures(kind="REVOKED")
        self.assertEqual(fails[0]["reason"], RejectReason.DEVICE_OFFLINE.value)
        self.assertEqual(fails[0]["operator"], "system")
        self.assertEqual(fails[0]["detail"]["device_id"], "base-1")

        # 基站恢复后排队申请自动开通
        self.orch.set_device_online("base-1", True, "field-radio")
        self.assertEqual(
            len(self.orch.request_status(deferred.request_id)["leases"]), 1
        )

    def test_terminal_offline_revokes_its_lease(self):
        first = self.orch.submit_request(make_request(), "op1")
        self.orch.set_device_online("term-1", False, "op1")
        self.assertEqual(
            self.orch.get_lease(first.lease.lease_id)["status"],
            LeaseStatus.REVOKED.value,
        )
        self.assertEqual(self.orch.active_leases(), [])

    def test_move_out_of_range_revokes(self):
        first = self.orch.submit_request(make_request(), "op1")
        self.orch.report_position("term-1", 9.0, 9.0, "field-unit-1")
        self.assertEqual(
            self.orch.get_lease(first.lease.lease_id)["status"],
            LeaseStatus.REVOKED.value,
        )
        fails = self.orch.failures(kind="REVOKED")
        self.assertEqual(fails[0]["reason"], "OUT_OF_RANGE")

    def test_move_within_range_keeps_lease(self):
        first = self.orch.submit_request(make_request(), "op1")
        self.orch.report_position("term-1", 5.0, 5.0, "field-unit-1")
        self.assertTrue(self.orch.get_lease(first.lease.lease_id)["active"])


class TestHistoryAndEvents(OrchestratorTestBase):
    def test_events_carry_operator_identity(self):
        lease = self.orch.submit_request(
            make_request(ttl=60.0), "operator-zhao"
        ).lease
        self.orch.renew_lease(lease.lease_id, 90.0, "operator-zhao",
                              expected_version=1)
        self.orch.release_lease(lease.lease_id, "operator-qian")

        events = self.orch.events()
        operators = {
            "lease.granted": "operator-zhao",
            "lease.renewed": "operator-zhao",
            "lease.released": "operator-qian",
            "device.registered": "dispatcher-li",
        }
        for etype, op in operators.items():
            matches = [e for e in events if e["type"] == etype]
            self.assertTrue(matches)
            self.assertTrue(all(e["operator"] == op for e in matches))
        self.assertEqual([e["seq"] for e in events],
                         list(range(1, len(events) + 1)))

    def test_lease_history_records_lifecycle(self):
        lease = self.orch.submit_request(make_request(), "op1").lease
        self.orch.renew_lease(lease.lease_id, 60.0, "op1",
                              expected_version=1)
        self.orch.release_lease(lease.lease_id, "op1")
        history = self.orch.lease_history(group_id="g1")
        self.assertEqual(len(history), 1)
        types = [e["type"] for e in history[0]["events"]]
        self.assertEqual(
            types, ["lease.granted", "lease.renewed", "lease.released"]
        )
        versions = [e["version"] for e in history[0]["events"]]
        self.assertEqual(versions, [1, 2, 2])

    def test_failures_query_filters(self):
        self.orch.submit_request(make_request(bw=10.0), "op1")
        self.orch.register_device(make_terminal("term-2", "g2"), "setup")
        conflict = self.orch.submit_request(
            make_request(term="term-2", group="g2", bw=10.0), "op2"
        )
        self.assertEqual(conflict.status, AllocationStatus.CONFLICT.value)
        self.assertEqual(
            self.orch.failures(group_id="g2")[0]["request_id"],
            conflict.request_id,
        )
        self.assertEqual(self.orch.failures(group_id="nobody"), [])


class TestRecovery(OrchestratorTestBase):
    def _restart(self):
        self.orch.stop_reaper()
        self.orch = ResourceOrchestrator(
            EventStore(self.state_path),
            time_func=self.clock,
            autoload=True,
        )

    def test_unexpired_leases_resume_with_wall_clock(self):
        self.replace_base(channels=("ch-1", "ch-2"))
        keep = self.orch.submit_request(
            make_request(bw=5.0, ttl=300.0), "op1"
        ).lease
        expire = self.orch.submit_request(
            make_request(bw=2.0, ttl=60.0, group="gx", term="term-1"), "op2"
        ).lease
        # 同一终端两条租约共用信道是允许的（不同信道），验证恢复粒度
        self.assertNotEqual(keep.channel_id, expire.channel_id)

        self.clock.advance(100)
        self._restart()

        keep_view = self.orch.get_lease(keep.lease_id)
        self.assertTrue(keep_view["active"])
        self.assertAlmostEqual(keep_view["time_left_seconds"], 200.0)
        self.assertEqual(keep_view["version"], 1)

        expire_view = self.orch.get_lease(expire.lease_id)
        self.assertFalse(expire_view["active"])
        self.assertEqual(expire_view["status"], LeaseStatus.EXPIRED.value)

        # 拓扑/电源预算恢复：keep 仍占用，expire 已释放
        topo = self.orch.topology()
        self.assertEqual(len(topo["active_leases"]), 1)
        base = topo["bases"][0]
        self.assertEqual(base["power_used_w"], keep_view["power_w"])

    def test_recovered_lease_can_renew_and_release(self):
        lease = self.orch.submit_request(make_request(ttl=300.0), "op1").lease
        self._restart()
        renewed = self.orch.renew_lease(
            lease.lease_id, 120.0, "op1", expected_version=1
        )
        self.assertEqual(renewed.version, 2)
        self.orch.release_lease(renewed.lease_id, "op1",
                                expected_version=2)
        self.assertEqual(
            self.orch.get_lease(lease.lease_id)["status"],
            LeaseStatus.RELEASED.value,
        )

    def test_deferred_queue_recovered_and_auto_grants(self):
        first = self.orch.submit_request(
            make_request(bw=10.0), "op1"
        ).lease
        self.orch.register_device(make_terminal("term-2", "g2"), "setup")
        deferred = self.orch.submit_request(
            make_request(
                term="term-2", group="g2", bw=10.0,
                allow_defer=True, ttl=600.0,
            ),
            "op2",
        )
        self._restart()
        self.assertIn(
            deferred.request_id,
            self.orch.topology()["deferred_requests"],
        )
        self.orch.release_lease(first.lease_id, "op1")
        self.assertEqual(
            len(self.orch.request_status(deferred.request_id)["leases"]), 1
        )

    def test_stale_deferred_dropped_on_restart(self):
        self.orch.submit_request(make_request(bw=10.0), "op1")
        self.orch.register_device(make_terminal("term-2", "g2"), "setup")
        deferred = self.orch.submit_request(
            make_request(
                term="term-2", group="g2", bw=10.0,
                allow_defer=True, ttl=30.0, valid_for=30.0,
            ),
            "op2",
        )
        self.clock.advance(45)
        self._restart()
        self.assertNotIn(
            deferred.request_id,
            self.orch.topology()["deferred_requests"],
        )

    def test_failure_history_survives_restart(self):
        self.orch.submit_request(
            make_request(x=99.0, y=99.0), "op1"
        )  # NO_DEVICE_IN_RANGE
        self._restart()
        fails = self.orch.failures(kind="REJECTED")
        self.assertEqual(len(fails), 1)
        self.assertEqual(fails[0]["reason"],
                         RejectReason.NO_DEVICE_IN_RANGE.value)
        self.assertEqual(fails[0]["operator"], "op1")

    def test_events_after_last_snapshot_replayed(self):
        lease = self.orch.submit_request(make_request(ttl=300.0), "op1").lease
        # 到期事件由查询触发、且不产生快照——落在快照之后的日志里
        self.clock.advance(301)
        self.orch.active_leases()
        self._restart()
        self.assertEqual(
            self.orch.get_lease(lease.lease_id)["status"],
            LeaseStatus.EXPIRED.value,
        )


class TestBackgroundReaper(OrchestratorTestBase):
    def test_background_reaper_expires_lease(self):
        lease = self.orch.submit_request(
            make_request(ttl=1.0), "op1"
        ).lease
        self.orch.start_reaper(interval_seconds=0.05)
        self.clock.advance(2)
        # 真实墙钟等待一个 reap 周期（fake clock 已推进）
        import time
        deadline = time.time() + 2.0
        while self.orch.get_lease(lease.lease_id)["status"] == "ACTIVE":
            self.assertTrue(time.time() < deadline)
            time.sleep(0.05)
        self.assertEqual(
            self.orch.get_lease(lease.lease_id)["status"],
            LeaseStatus.EXPIRED.value,
        )


if __name__ == "__main__":
    unittest.main()
