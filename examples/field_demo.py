"""应急通信车现场调度端到端演示（确定性手动时钟，可直接运行）。

运行：python3 -m examples.field_demo

场景串演：
  1. 登记设备与三个救援小组；
  2. 普通优先级链路占满山前中继；
  3. 高优先级生命搜救任务到达 → 自动抢占；
  4. 被抢占小组改走延期排队，待资源释放后落地；
  5. 租约到期自动释放、设备掉线强制终止；
  6. 查询拓扑、租约历史、失败原因与审计事件；
  7. “重启进程”后未过期租约恢复并继续计时。
"""

from __future__ import annotations

import tempfile

from src.service import OrchestratorService
from src.orchestrator_errors import ResourceConflictError
from src.store import JsonSnapshotStore
from tests.clock_helper import ManualClock


def line(title: str) -> None:
    print(f"\n===== {title} =====")


def main() -> None:
    clock = ManualClock(start=1_700_000_000.0)  # 固定起点，便于复算有效期
    path = tempfile.mktemp(prefix="eco-state-", suffix=".json")
    svc = OrchestratorService(JsonSnapshotStore(path), clock)

    line("1. 设备与小组报到")
    svc.register_device("车长-雷", "D1", "山前UHF中继", ["UHF"],
                        total_bandwidth_khz=100, power_budget_w=60, range_km=8)
    svc.register_device("车长-雷", "D2", "卫星便携站", ["SAT"],
                        total_bandwidth_khz=512, power_budget_w=0, range_km=99)
    svc.register_team("前指-岚", "T1", "道路抢修组", 1.0, 0.0, capabilities=["UHF"])
    svc.register_team("前指-岚", "T2", "生命搜救组", 2.0, 0.0,
                      capabilities=["UHF", "SAT"])
    svc.register_team("前指-岚", "T3", "医疗后送组", 3.0, 0.0, capabilities=["UHF"])

    line("2. 抢修组申请 90kHz/300 秒")
    low = svc.request_link("调度员-岚", "T1", bandwidth_khz=90, band="UHF",
                           priority=2, ttl_seconds=300)
    print("租约:", low["lease_id"], "版本:", low["version"],
          "有效期至:", low["valid_until"])

    line("3. 医疗组同刻竞争 50kHz → 明确冲突（列出竞争租约）")
    try:
        svc.request_link("调度员-岚", "T3", bandwidth_khz=50, band="UHF",
                         priority=2, ttl_seconds=300, request_id="cmd-med-1")
    except ResourceConflictError as err:
        print(f"冲突原因码: {err.reason}；竞争租约: {err.competing_leases}；"
              f"候选设备: {err.candidate_devices}")

    line("4. 生命搜救组高优先级（9）申请 80kHz → 抢占抢修组")
    high = svc.request_link("总指挥-峥", "T2", bandwidth_khz=80, band="UHF",
                            priority=9, ttl_seconds=600)
    print("新租约:", high["lease_id"], "抢占:", high["preempted_leases"])

    line("5. 抢修组改为延期排队；搜救组 100 秒后撤离，延期请求落地")
    defer = svc.defer_request("调度员-岚", "T1", bandwidth_khz=40, band="UHF",
                              priority=2, ttl_seconds=300, note="等待信道空出")
    print("排队:", defer["request_id"])
    clock.advance(100)
    svc.release("搜救组-联络员", high["lease_id"], expected_version=1)
    granted = svc.fulfill_deferred(defer["request_id"])
    print("延期落地租约:", granted["lease_id"], "TTL:", granted["ttl_seconds"])

    line("6. 时间快进 301 秒 → 租约过期、容量归还")
    clock.advance(301)
    svc.sweep_expired()
    topo = svc.topology()
    print("在效租约数:", len(topo["active_leases"]),
          " D1 已用带宽:", topo["devices"]["D1"]["used_bandwidth_khz"])

    line("7. 设备掉线演练：新开链路后 D1 掉线，租约被强制终止")
    hold = svc.request_link("调度员-岚", "T3", bandwidth_khz=30, band="UHF",
                            priority=3, ttl_seconds=300)
    svc.set_device_online("车长-雷", "D1", online=False)
    print("租约状态:", svc.get_lease(hold["lease_id"])["state"],
          " 终止原因:", svc.get_lease(hold["lease_id"])["terminate_reason"])

    line("8. 查询：失败原因台账 / 租约历史 / 审计事件")
    for fail in svc.failures():
        print(f"  [{fail['reason']}] 操作员={fail['operator']} {fail['detail']}")
    print("历史租约数:", len(svc.lease_history()),
          " 抢占事件数:", len(svc.events(event_type="LEASE_PREEMPTED")))

    line("9. 模拟进程重启：恢复未过期租约并继续计时")
    svc.set_device_online("车长-雷", "D1", online=True)
    live = svc.request_link("调度员-岚", "T2", bandwidth_khz=50, band="UHF",
                            priority=5, ttl_seconds=300)
    clock.advance(120)
    restarted = OrchestratorService(JsonSnapshotStore(path),
                                    ManualClock(clock.now()))
    got = restarted.get_lease(live["lease_id"])
    print("恢复后状态:", got["state"], " 剩余TTL:", got["ttl_seconds"],
          " 版本仍为:", got["version"])
    print("快照文件:", path)


if __name__ == "__main__":
    main()
