# detectors/error_spike.py
class ErrorSpikeDetector:
    def detect(self, ctx):
        errors = ctx.get("errors", [])
        logs = ctx.get("logs", [])

        error_count = len(errors) + sum(1 for l in logs if l.get("level") == "ERROR")

        if error_count >= 10:
            return [{
                "type": "ERROR_SPIKE",
                "score": min(1.0, error_count / 50),
                "evidence": {"error_count": error_count}
            }]
        return []
