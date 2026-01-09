# state_builder.py
from typing import Dict, Any


def build_state(payload: Dict[str, Any], pod_status: Dict[str, Any]) -> Dict[str, Any]:
    """
    构建统一状态快照（Control Plane State）
    """
    state = {}

    # Pod 状态
    state["pod_ready"] = pod_status.get("ready", 0)
    state["pod_desired"] = pod_status.get("desired", 0)

    # DB 状态（来自 errors）
    db_errors = [
        e for e in payload.get("errors", [])
        if "DB" in e.get("exception_type", "")
    ]
    state["db_status"] = "error" if db_errors else "ok"

    return state
