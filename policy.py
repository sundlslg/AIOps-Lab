# policy.py
from typing import Dict, Any


class PolicyEngine:
    def __init__(self, config: Dict[str, Any]):
        self.config = config

    def allow_auto_scale(self, service: str, anomalies: list) -> bool:
        auto = self.config.get("auto_scale", {})
        if not auto.get("enabled", False):
            return False

        svc_cfg = auto.get("services", {}).get(service)
        if not svc_cfg:
            return False

        required = set(svc_cfg.get("require_conditions", []))
        detected = {a["type"] for a in anomalies}

        return required.issubset(detected)
