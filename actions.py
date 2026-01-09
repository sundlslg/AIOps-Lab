# actions.py
class ActionPlanner:
    def plan(self, rca_results):
        actions = []
        for r in rca_results:
            if "Scale" in r["suggestion"]:
                actions.append({
                    "action": "SCALE",
                    "target": "deployment/newbee-mall",
                    "replicas": "+1",
                    "auto": False
                })
        return {
            "summary": "AIOps recommendation",
            "actions": actions,
            "rca": rca_results
        }
