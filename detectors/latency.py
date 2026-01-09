# detectors/latency.py
import statistics

class LatencyDetector:
    def detect(self, ctx):
        traces = ctx.get("traces", [])
        durs = [t["duration_ms"] for t in traces if t.get("duration_ms")]

        if len(durs) < 10:
            return []

        p95 = statistics.quantiles(durs, n=20)[18]
        if p95 > 1000:
            return [{
                "type": "HIGH_LATENCY",
                "score": min(1.0, p95 / 5000),
                "evidence": {"p95_ms": p95}
            }]
        return []
