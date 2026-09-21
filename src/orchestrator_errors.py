"""领域异常与原因码。

所有编排失败都抛出或记录这里定义的异常，原因码（Reason）同时写入
审计事件，供调度员事后检索失败原因。
"""

from __future__ import annotations


class Reason:
    """失败原因码（人类可读常量，同时作为查询标签）。"""

    DEVICE_UNKNOWN = "DEVICE_UNKNOWN"                # 设备未注册
    DEVICE_OFFLINE = "DEVICE_OFFLINE"                # 设备掉线/无在线设备
    TEAM_UNKNOWN = "TEAM_UNKNOWN"                    # 小组未报到
    NO_DEVICE_IN_RANGE = "NO_DEVICE_IN_RANGE"        # 位置超出所有设备覆盖
    UNSUPPORTED_CAPABILITY = "UNSUPPORTED_CAPABILITY"  # 频段/设备能力不匹配
    INSUFFICIENT_BANDWIDTH = "INSUFFICIENT_BANDWIDTH"
    INSUFFICIENT_POWER = "INSUFFICIENT_POWER"
    RESOURCE_CONFLICT = "RESOURCE_CONFLICT"          # 与在效租约竞争同一资源
    VERSION_CONFLICT = "VERSION_CONFLICT"            # 租约版本不匹配（CAS 失败）
    LEASE_NOT_ACTIVE = "LEASE_NOT_ACTIVE"            # 租约已终止或过期
    INVALID_REQUEST = "INVALID_REQUEST"              # 参数非法
    PREEMPTED = "PREEMPTED"                          # 被高优先级任务抢占
    DEVICE_WENT_OFFLINE = "DEVICE_WENT_OFFLINE"      # 设备掉线导致租约终止


class OrchestratorError(Exception):
    """编排服务异常基类。"""

    reason = Reason.INVALID_REQUEST

    def __init__(self, detail: str = "", *, reason: str | None = None):
        super().__init__(detail)
        self.detail = detail
        if reason is not None:
            self.reason = reason

    def to_dict(self) -> dict:
        return {"error": self.__class__.__name__, "reason": self.reason, "detail": self.detail}


class InvalidRequestError(OrchestratorError):
    reason = Reason.INVALID_REQUEST


class DeviceOfflineError(OrchestratorError):
    reason = Reason.DEVICE_OFFLINE


class ResourceConflictError(OrchestratorError):
    """两个调度请求竞争同一份有限资源且无法同时满足。

    ``competing_leases`` 给出当前占用资源、导致本次请求失败的在效租约，
    调度员可据此判断该抢占、改点还是延期。
    """

    reason = Reason.RESOURCE_CONFLICT

    def __init__(
        self,
        detail: str,
        *,
        competing_leases: list[str] | None = None,
        candidate_devices: list[str] | None = None,
        reason: str = Reason.RESOURCE_CONFLICT,
    ):
        super().__init__(detail, reason=reason)
        self.competing_leases = competing_leases or []
        self.candidate_devices = candidate_devices or []

    def to_dict(self) -> dict:
        data = super().to_dict()
        data["competing_leases"] = self.competing_leases
        data["candidate_devices"] = self.candidate_devices
        return data


class LeaseVersionConflictError(ResourceConflictError):
    """租约版本号过期（乐观锁失败）。"""

    reason = Reason.VERSION_CONFLICT

    def __init__(self, lease_id: str, expected: int, actual: int):
        super().__init__(
            f"租约 {lease_id} 版本冲突：期望 {expected}，当前为 {actual}",
            competing_leases=[lease_id],
            reason=Reason.VERSION_CONFLICT,
        )
        self.expected = expected
        self.actual = actual


class LeaseNotActiveError(OrchestratorError):
    reason = Reason.LEASE_NOT_ACTIVE
