# decision.py
import uuid
from datetime import datetime, timezone
from typing import Dict, Any, List


def now_utc():
    return datetime.now(timezone.utc).isoformat()


class Decision:
    def __init__(
        self,
        service: str,
        state: Dict[str, Any],
        anomalies: List[Dict[str, Any]],
        rca_v1v2: List[Dict[str, Any]],
        rca_v3: str
    ):
        self.decision_id = str(uuid.uuid4())
        self.timestamp = now_utc()
        self.service = service
        self.state = state
        self.anomalies = anomalies
        self.rca = {
            "v1_v2": rca_v1v2,
            "v3_flashrag": rca_v3
        }
        self.recommendations = []
        self.execution = {
            "auto_enabled": False,
            "executed": False,
            "reason": "policy_not_evaluated"
        }

    def add_recommendation(self, rec: Dict[str, Any]):
        self.recommendations.append(rec)

    def set_execution(self, auto_enabled: bool, executed: bool, reason: str):
        self.execution = {
            "auto_enabled": auto_enabled,
            "executed": executed,
            "reason": reason
        }

    def to_dict(self) -> Dict[str, Any]:
        return {
            "decision_id": self.decision_id,
            "timestamp": self.timestamp,
            "service": self.service,
            "state": self.state,
            "anomalies": self.anomalies,
            "rca": self.rca,
            "recommendations": self.recommendations,
            "execution": self.execution
        }
