# detectors/saturation.py
class SaturationDetector:
    def detect(self, ctx):
        metrics = ctx.get("metrics", [])
        cpu = [m for m in metrics if "cpu" in m["metric_name"].lower()]

        if not cpu:
            return []

        high = [m for m in cpu if (m.get("avg_last") or 0) > 0.8]
        if high:
            return [{
                "type": "CPU_SATURATION",
                "score": 0.8,
                "evidence": {"samples": len(high)}
            }]
        return []
