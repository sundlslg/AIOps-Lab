# rca.py
class RCAEngine:
    def analyze(self, ctx, detections):
        results = []

        for d in detections:
            if d["type"] == "ERROR_SPIKE":
                results.append({
                    "root_cause": "Application exception burst",
                    "confidence": d["score"],
                    "suggestion": "Check recent deployment, rollback if needed"
                })
            elif d["type"] == "HIGH_LATENCY":
                results.append({
                    "root_cause": "Downstream dependency latency",
                    "confidence": d["score"],
                    "suggestion": "Inspect slow spans and DB latency"
                })
            elif d["type"] == "CPU_SATURATION":
                results.append({
                    "root_cause": "CPU saturation",
                    "confidence": d["score"],
                    "suggestion": "Scale replicas or increase CPU limits"
                })

        return results
