# detectors/pod_detector.py
from typing import Dict, Any, List


class PodInsufficientDetector:
    def detect(self, state: Dict[str, Any]) -> List[Dict[str, Any]]:
        anomalies = []
        ready = state.get("pod_ready", 0)
        desired = state.get("pod_desired", 0)

        if ready < desired:
            anomalies.append({
                "type": "POD_INSUFFICIENT",
                "current": ready,
                "desired": desired,
                "severity": "HIGH"
            })
        return anomalies
