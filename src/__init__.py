"""灾害通信资源编排领域包。

快速上手::

    from src.service import OrchestratorService

    svc = OrchestratorService("/var/lib/eco/state.json")
    svc.register_device("车长-雷", "D1", "山前中继", ["UHF"], 100)
    svc.register_team("前指-岚", "T1", "搜救组", 0, 0, capabilities=["UHF"])
    lease = svc.request_link("调度员-岚", "T1", bandwidth_khz=40,
                             band="UHF", priority=5, ttl_seconds=300)
"""

from .models import Band, Clock, Device, Event, EventType, Lease, LeaseState, Team
from .orchestrator_errors import (
    InvalidRequestError,
    LeaseNotActiveError,
    LeaseVersionConflictError,
    OrchestratorError,
    Reason,
    ResourceConflictError,
)
from .service import OrchestratorService, Service
from .store import JsonSnapshotStore, MemoryStore, Store

__all__ = [
    "OrchestratorService",
    "Service",
    "Store",
    "JsonSnapshotStore",
    "MemoryStore",
    "Clock",
    "Band",
    "Device",
    "Team",
    "Lease",
    "LeaseState",
    "Event",
    "EventType",
    "Reason",
    "OrchestratorError",
    "InvalidRequestError",
    "ResourceConflictError",
    "LeaseVersionConflictError",
    "LeaseNotActiveError",
]
