# 灾害通信资源编排

应急通信车进入山区后，调度员在有限频段与电源容量下为多个救援小组临时开通
链路的资源编排服务（Python 3.11，仅标准库）。

## 能力清单

- **可行分配计算**：输入小组位置、任务优先级、所需带宽/电源与设备能力
  （频段），按覆盖范围、在线状态、带宽与电源预算筛选候选设备，最佳适配
  选点。
- **租约模型**：任何分配都是带 `valid_from / valid_until`（有效期）和
  `version`（租约版本）的租约；延期/释放支持乐观锁版本校验（CAS），
  防止两个调度终端基于过期视图重复操作。
- **抢占**：高优先级任务自动挤走低优先级租约（选代价最小的 victim 集合），
  同优先级不抢占；被抢占租约记录 `PREEMPTED` 状态与事件。
- **延期排队**：暂时放不下的请求可登记 `defer_request` 排队（不占资源），
  资源空出后 `fulfill_deferred` 落地。
- **释放与重新申请**：小组撤离 `release` 立即归还带宽/电源；被抢占、掉线
  终止或过期的旧租约可用 `reissue` 带血缘新开。
- **过期与掉线**：租约到期自动释放容量；设备 `set_device_online(False)`
  时其上在效租约全部强制终止，掉线/超距导致的失败返回明确原因码。
- **明确冲突**：两个请求同时竞争同一资源且无法通过抢占化解时，抛出
  `ResourceConflictError`，内含竞争租约 ID 与候选设备列表；服务内部加锁
  串行裁决，并发请求一胜一负。
- **审计留痕**：只增事件流记录每次资源变更与操作员身份；失败原因台账
  可按原因/小组检索；另提供当前拓扑与租约历史查询。
- **重启恢复**：每次变更原子写穿透到 JSON 快照；重启后未过期租约恢复
  占用并按墙上时钟继续计时，停机期间到期的租约自动清退。

## 目录结构

```
src/
  models.py              # 设备、小组、租约（有效期+版本）、事件、时钟
  orchestrator_errors.py # 异常与失败原因码
  store.py               # 原子快照存储 / 内存存储（测试）
  service.py             # OrchestratorService：分配、抢占、延期、释放、查询、恢复
tests/                   # 34 个单元/并发/持久化测试
examples/field_demo.py   # 现场全流程演示脚本
```

## 运行方式

```bash
python3 -m unittest discover -s tests -v   # 测试
python3 -m examples.field_demo             # 端到端场景演示
```

## 主要接口

| 操作 | 方法 |
|---|---|
| 设备/小组登记 | `register_device` / `register_team` |
| 设备上下线 | `set_device_online` |
| 申请链路 | `request_link(...)`（支持 `request_id` 幂等、`allow_preemption`） |
| 抢占后重开 | `reissue` |
| 延期（续约） | `extend_lease(..., expected_version=)` |
| 释放 | `release(..., expected_version=)` |
| 延期排队 | `defer_request` / `fulfill_deferred` / `cancel_deferred` |
| 当前拓扑 | `topology()` |
| 租约历史 | `lease_history(team_id=, device_id=, states=)` |
| 事件审计 | `events(event_type=, operator=, team_id=)` |
| 失败原因 | `failures(reason=, team_id=)` |
| 主动清退 | `sweep_expired()` |

失败原因码：`NO_DEVICE_IN_RANGE`、`DEVICE_OFFLINE`、`UNSUPPORTED_CAPABILITY`、
`INSUFFICIENT_BANDWIDTH`、`INSUFFICIENT_POWER`、`RESOURCE_CONFLICT`、
`VERSION_CONFLICT`、`LEASE_NOT_ACTIVE`、`PREEMPTED`、`DEVICE_WENT_OFFLINE`。

运行环境：Python 3.11，无第三方依赖。代码位于 `src` 目录，持久化文件路径
由部署环境以构造参数提供（如 `OrchestratorService("/var/lib/eco/state.json")`）。
