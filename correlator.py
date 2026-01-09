# correlator.py
class Correlator:
    def run(self, ctx, detections):
        traces = ctx.get("traces", [])
        logs = ctx.get("logs", [])

        trace_index = {t["trace_id"]: t for t in traces if t.get("trace_id")}

        for d in detections:
            related = []
            for l in logs:
                tid = l.get("trace_id")
                if tid and tid in trace_index:
                    related.append({
                        "trace": trace_index[tid],
                        "log": l
                    })
            d["correlations"] = related[:20]
        return detections
