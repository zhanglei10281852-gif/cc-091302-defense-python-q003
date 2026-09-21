# 灾害通信资源编排

应急通信车进入山区后，调度员在有限频段与电源容量下为多个救援小组临时
开通链路的资源编排服务。服务接收小组位置、任务优先级、所需带宽与终端
频段能力，计算可行分配，并处理抢占、延期、释放与重新申请。

运行环境：Python 3.11（无第三方依赖）。代码位于 `src` 目录。

## 核心语义

- **带有效期与租约版本的分配**：每条租约携带绝对时间戳 `expires_at`
  与单调递增 `version`；续租刷新有效期并自增版本。
- **过期/掉线不占资源**：租约到期、基站或终端掉线、终端撤离出覆盖范围
  后，租约自动转为 `EXPIRED`/`REVOKED`，信道与电源预算立即释放。
- **明确冲突**：两个调度请求竞争同一信道（或电源预算不足）时，后到者
  得到 `CONFLICT`，结果中给出阻塞租约列表；也可选择进入延期队列，
  资源释放后按"优先级高者先得、同优先级先到先得"自动补分配。
- **抢占**：高优先级申请（`allow_preempt=True`）可抢占严格更低优先级
  的活跃租约，电源不足时按"最低优先级、最早签发"顺序腾出功率。
- **乐观锁**：续租/释放可携带 `expected_version`，版本不一致返回
  `VERSION_CONFLICT`，防止过期指令覆盖现场最新状态。
- **留痕**：所有资源变更以仅追加事件日志保存，并记录操作员身份；
  提供当前拓扑、租约全生命周期历史、失败原因查询。
- **重启恢复**：原子快照 + 事件重放恢复状态；租约使用绝对时间戳，
  重启后未过期租约继续计时，停机期间到期的租约自动过期。

## 快速开始

```python
from src import (
    ResourceOrchestrator, EventStore,
    Device, Channel, Request, AllocationStatus,
)

orch = ResourceOrchestrator(EventStore("data/orchestrator.state.json"))

# 1. 登记应急车基站（信道 + 电源预算 + 覆盖半径）
orch.register_device(Device.base(
    device_id="base-1", name="1号应急车",
    x_km=0.0, y_km=0.0, range_km=10.0, power_budget_w=30.0,
    channels=[Channel("ch-1", "base-1", 400.0, 10.0, "UHF")],
), operator="dispatcher-li")

# 2. 登记救援小组终端（频段能力 + 当前位置）
orch.register_device(Device.terminal(
    device_id="term-1", name="甲组终端", group_id="group-a",
    x_km=1.0, y_km=0.0, supported_bands=["UHF"],
), operator="dispatcher-li")

# 3. 提交调度申请：位置/优先级/带宽/有效期
result = orch.submit_request(Request(
    group_id="group-a", terminal_device_id="term-1",
    bandwidth_mhz=3.0, priority=5, ttl_seconds=300,
    allow_preempt=True, allow_defer=True,
), operator="operator-wang")

if result.status == AllocationStatus.GRANTED.value:
    lease = result.lease          # 带 expires_at 与 version=1
    orch.renew_lease(             # 续租（乐观锁校验版本）
        lease.lease_id, ttl_seconds=300,
        operator="operator-wang", expected_version=lease.version,
    )
    orch.release_lease(lease.lease_id, "operator-wang")
elif result.status == AllocationStatus.CONFLICT.value:
    print(result.blocking_leases) # 明确的竞争对手
# DEFERRED：已排队，资源释放后自动开通；也可 retry_deferred() 重新申请

# 4. 查询
orch.topology()                   # 当前拓扑：信道占用/电源/覆盖/活跃租约
orch.lease_history("group-a")     # 租约全生命周期与版本事件
orch.failures(group_id="group-a") # 失败/延期/抢占/掉线/过期原因
orch.events()                     # 带操作员身份的资源变更事件
```

设备掉线、撤离与后台到期清理：

```python
orch.set_device_online("base-1", False, operator="field-radio")  # 吊销租约
orch.report_position("term-1", 9.0, 9.0, operator="field-unit")  # 出覆盖→释放
orch.start_reaper(interval_seconds=5)                            # 后台扫期
```

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 文件说明

| 文件 | 职责 |
| --- | --- |
| `src/models.py` | 设备/信道/申请/租约/事件等领域模型与原因码 |
| `src/store.py` | 原子状态快照 + 仅追加事件日志 |
| `src/service.py` | `ResourceOrchestrator`：分配裁决、抢占、延期、到期、恢复与查询 |
| `tests/test_orchestrator.py` | 40 个场景测试（含并发竞争与重启恢复） |
