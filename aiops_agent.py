#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import gzip
import json
import os
import time
import subprocess
import requests
import socket
from datetime import datetime, timezone
from typing import Dict, Any, List, Tuple

from detectors import (
    ErrorSpikeDetector,
    LatencyDetector,
    SaturationDetector
)
from correlator import Correlator
from rca import RCAEngine
from actions import ActionPlanner

# =========================
# 基础配置
# =========================
INPUT_DIR = "/root/aiops-data-prepare/out_json"
POLL_INTERVAL = 10
CONFIG_FILE = "./agent_config.json"

FLASHRAG_URL = "http://192.168.137.103:8000/rag_query"

POD_CHECK_LIST = [
    {"name": "newbee-mall", "namespace": "newbee-mall", "desired_replicas": 3},
]

# 数据库检测配置
DB_HOST = os.getenv("MYSQL_HOST", "192.168.137.108")
DB_PORT = int(os.getenv("MYSQL_PORT", 3306))

# =========================
# 配置加载
# =========================
def load_agent_config() -> Dict[str, Any]:
    if not os.path.exists(CONFIG_FILE):
        return {"auto_scale": {"enabled": False, "max_scale": 10}}
    with open(CONFIG_FILE, "r") as f:
        return json.load(f)

AGENT_CONFIG = load_agent_config()
AUTO_SCALE_ENABLED = AGENT_CONFIG.get("auto_scale", {}).get("enabled", False)
MAX_SCALE = AGENT_CONFIG.get("auto_scale", {}).get("max_scale", 10)

# =========================
# 工具函数
# =========================
def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()

def load_payload(path: str) -> Dict[str, Any]:
    with gzip.open(path, "rt", encoding="utf-8") as f:
        return json.load(f)

# =========================
# 数据库检测
# =========================
def check_db_connection() -> List[Dict[str, Any]]:
    """
    检查 MySQL TCP 3306 是否可达
    """
    errors = []
    try:
        s = socket.create_connection((DB_HOST, DB_PORT), timeout=3)
        s.close()
    except Exception as e:
        errors.append({
            "type": "DB_CONNECTION_ERROR",
            "service": "database",
            "timestamp": utc_now(),
            "exception_type": "DBConnectionError",
            "exception_message": f"Cannot connect to MySQL at {DB_HOST}:{DB_PORT} - {e}"
        })
    return errors

# =========================
# FlashRAG V3
# =========================
def run_flashrag_rag(payload: Dict[str, Any], pod_anomalies: List[Dict[str, Any]], db_anomalies: List[Dict[str, Any]]) -> str:
    """
    把 DB + Pod 异常一起送入 FlashRAG
    """
    lines = []
    meta = payload.get("meta", {})
    window = meta.get("window", {})

    lines.append(f"服务: {meta.get('service_hint', 'unknown')}")
    lines.append(f"时间窗口: {window.get('start')} ~ {window.get('end')}")

    # DB 异常
    for db in db_anomalies:
        lines.append(f"数据库错误: {db.get('exception_message')}")

    # Pod 异常
    for pa in pod_anomalies:
        lines.append(
            f"Pod 异常: {pa['service']} 就绪 {pa['ready']}/{pa['desired']}"
        )

    query_text = "\n".join(lines)

    try:
        resp = requests.post(
            FLASHRAG_URL,
            json={"query_text": query_text},
            timeout=15
        )
        resp.raise_for_status()
        return resp.json().get("answer", "[FlashRAG 无返回]")
    except Exception as e:
        return f"[FlashRAG ERROR] {e}"

# =========================
# Pod 检测（只检测，不执行）
# =========================
def check_pods() -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    pod_anomalies = []
    pod_recommendations = []

    for pod_cfg in POD_CHECK_LIST:
        name = pod_cfg["name"]
        ns = pod_cfg["namespace"]
        desired = pod_cfg["desired_replicas"]

        try:
            cmd = [
                "kubectl", "get", "deploy", name, "-n", ns,
                "-o", "jsonpath={.status.readyReplicas}"
            ]
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
            ready = int(r.stdout.strip() or "0")
        except Exception as e:
            pod_anomalies.append({
                "type": "POD_CHECK_FAILED",
                "service": name,
                "timestamp": utc_now(),
                "message": str(e)
            })
            continue

        if ready < desired:
            pod_anomalies.append({
                "type": "POD_INSUFFICIENT",
                "service": name,
                "timestamp": utc_now(),
                "ready": ready,
                "desired": desired,
                "severity": "HIGH"
            })

            pod_recommendations.append({
                "action": "SCALE",
                "target": f"deployment/{name}",
                "namespace": ns,
                "from": ready,
                "to": min(desired, MAX_SCALE),
                "auto_allowed": AUTO_SCALE_ENABLED,
                "reason": "Pod 就绪数量不足"
            })

    return pod_anomalies, pod_recommendations

# =========================
# 主循环（Control Plane）
# =========================
def main_loop():
    print("[AIOps-Agent] started (Control Plane mode)")
    seen = set()

    while True:
        for fn in sorted(os.listdir(INPUT_DIR)):
            if not fn.endswith(".json.gz") or fn in seen:
                continue
            seen.add(fn)

            payload = load_payload(os.path.join(INPUT_DIR, fn))
            detections: List[Dict[str, Any]] = []

            # 1. 常规异常
            detections += ErrorSpikeDetector().detect(payload)
            detections += LatencyDetector().detect(payload)
            detections += SaturationDetector().detect(payload)

            # 2. Pod 异常
            pod_anomalies, pod_recos = check_pods()
            detections += pod_anomalies
            payload.setdefault("errors", []).extend([
                {
                    "timestamp": a["timestamp"],
                    "service": a["service"],
                    "exception_type": a["type"],
                    "exception_message": f"Pod {a.get('ready')}/{a.get('desired')}"
                }
                for a in pod_anomalies if a["type"] == "POD_INSUFFICIENT"
            ])

            # 3. DB 异常
            db_anomalies = check_db_connection()
            detections += db_anomalies
            payload.setdefault("errors", []).extend(db_anomalies)

            if not detections:
                print(f"[OK] {fn} no anomaly")
                continue

            # 4. 关联分析
            correlated = Correlator().run(payload, detections)

            # 5. RCA V1/V2
            rca = RCAEngine().analyze(payload, correlated)

            # 6. Action Recommendation（不执行）
            plan = ActionPlanner().plan(rca)
            plan.setdefault("actions", []).extend(pod_recos)

            # DB 异常也可以生成 Action 告警
            for db_err in db_anomalies:
                plan.setdefault("actions", []).append({
                    "action": "ALERT",
                    "target": "database",
                    "auto_allowed": False,
                    "reason": db_err.get("exception_message")
                })

            # 7. RCA V3（FlashRAG）
            rca_v3 = run_flashrag_rag(payload, pod_anomalies, db_anomalies)

            # 8. 输出
            print("\n=== AIOps Decision (V1/V2) ===")
            print(json.dumps(plan, indent=2, ensure_ascii=False))

            print("\n=== FlashRAG V3 RCA ===")
            print(rca_v3)

        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main_loop()
