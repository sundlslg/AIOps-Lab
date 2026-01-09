#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys, json, gzip, time, uuid, socket
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List
import clickhouse_connect
from concurrent.futures import ThreadPoolExecutor, as_completed

# ================== 基本配置（低延迟档） ==================
HOST = 'clickhouse.sun.com'
PORT = 80
USERNAME = 'admin'
PASSWORD = '27ff0399-0d3a-4bd8-919d-17c2181e6fb9'

SERVICE_HINT = 'newbee-mall'
OUTPUT_DIR = './out_json'
STATE_FILE = './state.json'

ROLL_INTERVAL_SEC = 30
WINDOW_SEC = 900  # 15 分钟窗口，保证慢数据也能抓到

MAX_RETRIES = 3
RETRY_BASE_SEC = 1

METRIC_WHITELIST_PATTERNS = [
    "node_%", "http.%", "signoz_%",
]

LOG_DB = 'signoz_logs'
TRACE_DB = 'signoz_traces'
METRIC_DB = 'signoz_metrics'

# ================== 工具函数 ==================
def utc_now():
    return datetime.now(timezone.utc)

def isoformat(dt: datetime):
    return dt.astimezone(timezone.utc).isoformat()

def load_state() -> Dict[str, Any]:
    if not os.path.exists(STATE_FILE):
        return {}
    with open(STATE_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)

def save_state(state: Dict[str, Any]):
    tmp = STATE_FILE + '.partial'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    os.replace(tmp, STATE_FILE)

def mk_seq(ts_end: datetime) -> str:
    return ts_end.strftime('%Y%m%dT%H%M') + '-' + uuid.uuid4().hex[:6]

def safe_json_value(v):
    if v is None or isinstance(v, (str, int, float, bool)):
        return v
    if isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, (bytes, bytearray)):
        try: return v.decode('utf-8')
        except Exception: return v.hex()
    try:
        if isinstance(v, memoryview):
            b = v.tobytes()
            try: return b.decode('utf-8')
            except Exception: return b.hex()
    except Exception: pass
    if isinstance(v, dict):
        return {str(k): safe_json_value(vv) for k, vv in v.items()}
    if isinstance(v, (list, tuple)):
        return [safe_json_value(vv) for vv in v]
    return str(v)

def rows_to_objs(keys, rows):
    return [{k: safe_json_value(v) for k, v in zip(keys, r)} for r in rows]

def atomic_gzip_write(path: str, payload: Dict[str, Any]):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + '.partial'
    with gzip.open(tmp, 'wt', encoding='utf-8') as g:
        json.dump(payload, g, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

def run_ch_query(sql: str) -> List[List[Any]]:
    client = clickhouse_connect.get_client(host=HOST, port=PORT, username=USERNAME, password=PASSWORD)
    try:
        return client.query(sql).result_rows
    finally:
        try: client.close()
        except Exception: pass

# ================== SQL 模板 ==================
def sql_logs_by_ns(start_ns: int, end_ns: int):
    return f"""
    SELECT
        toDateTime64(timestamp / 1e9, 9) AS time,
        resources_string['service.name'] AS service,
        resources_string['service.instance.id'] AS service_instance_id,
        resources_string['deployment.environment'] AS environment,
        body AS message,
        severity_text AS level,
        resources_string['host.name'] AS host,
        resources_string['service.version'] AS service_version,
        scope_name AS logger_name,
        attributes_string['exception.type'] AS exception_type,
        attributes_string['exception.message'] AS exception_message,
        attributes_string['thread.name'] AS thread_name,
        trace_id,
        span_id
    FROM {LOG_DB}.logs_v2
    WHERE timestamp >= {start_ns} AND timestamp < {end_ns}
      AND (
            resources_string['service.name'] = '{SERVICE_HINT}'
         OR attributes_string['exception.type'] != ''
         OR attributes_string['exception.message'] != ''
         OR lower(body) LIKE '%error%'
         OR lower(body) LIKE '%exception%'
         OR lower(body) LIKE '%failed%'
      )
    ORDER BY time DESC
    LIMIT 5000
    """

def sql_traces_best_effort(start_iso: str, end_iso: str):
    return f"""
    SELECT
        timestamp AS ts,
        serviceName AS service,
        name AS operation,
        traceID AS trace_id,
        spanID AS span_id,
        parentSpanID AS parent_id,
        durationNano / 1e6 AS duration_ms,
        hasError AS error,
        statusCode AS status_code,
        responseStatusCode AS http_status,
        httpMethod AS http_method,
        httpRoute AS http_route,
        httpUrl AS http_url,
        dbSystem AS db_system,
        dbName AS db_name,
        dbOperation AS db_operation,
        peerService AS peer_service
    FROM {TRACE_DB}.distributed_signoz_index_v3
    WHERE timestamp >= toDateTime64(parseDateTimeBestEffort('{start_iso}'), 9)
      AND timestamp <  toDateTime64(parseDateTimeBestEffort('{end_iso}'),   9)
      AND (serviceName = '{SERVICE_HINT}' OR hasError = 1)
    ORDER BY timestamp DESC
    LIMIT 5000
    """

def _metric_whitelist_where(alias="a"):
    likes = [f"{alias}.metric_name LIKE '{p}'" if '%' in p or '.' in p else f"{alias}.metric_name='{p}'"
             for p in METRIC_WHITELIST_PATTERNS]
    return "(" + " OR ".join(likes) + ")"

def sql_metrics_main(window_start_ms: int, window_end_ms: int):
    where_like = _metric_whitelist_where("a")
    return f"""
    SELECT
        ts.metric_name,
        any(md.unit) AS unit,
        any(md.type) AS type,
        ifNull(ts.resource_attrs['service.name'], '') AS service_name,
        ifNull(ts.resource_attrs['service.namespace'], '') AS service_namespace,
        ifNull(ts.resource_attrs['deployment.environment'], '') AS environment,
        ifNull(ts.attrs['operation'], '') AS operation,
        ifNull(ts.attrs['http.status_code'], '') AS http_status,
        ifNull(ts.attrs['span.kind'], '') AS span_kind,
        sum(a.count) AS sample_count,
        min(a.min) AS min_value,
        max(a.max) AS max_value,
        avg(a.last) AS avg_last,
        sum(a.sum) AS sum_value,
        min(fromUnixTimestamp64Milli(a.unix_milli)) AS first_seen,
        max(fromUnixTimestamp64Milli(a.unix_milli)) AS last_seen
    FROM {METRIC_DB}.distributed_samples_v4_agg_5m AS a
    INNER JOIN {METRIC_DB}.distributed_time_series_v4 AS ts
        ON a.fingerprint = ts.fingerprint AND a.unix_milli = ts.unix_milli
    LEFT JOIN {METRIC_DB}.distributed_metadata AS md
        ON md.metric_name = ts.metric_name AND md.temporality = ts.temporality
    WHERE a.unix_milli >= {window_start_ms} AND a.unix_milli < {window_end_ms} AND {where_like}
    GROUP BY ts.metric_name, service_name, service_namespace, environment, operation, http_status, span_kind
    ORDER BY sample_count DESC, ts.metric_name ASC
    LIMIT 2000
    """

def sql_metrics_fallback(window_start_ms: int, window_end_ms: int):
    where_like = _metric_whitelist_where("a")
    return f"""
    SELECT
        a.metric_name,
        any(a.temporality) AS temporality,
        '' AS unit,
        '' AS type,
        '' AS service_name,
        '' AS service_namespace,
        '' AS environment,
        '' AS operation,
        '' AS http_status,
        '' AS span_kind,
        sum(a.count) AS sample_count,
        min(a.min) AS min_value,
        max(a.max) AS max_value,
        avg(a.last) AS avg_last,
        sum(a.sum) AS sum_value,
        min(fromUnixTimestamp64Milli(a.unix_milli)) AS first_seen,
        max(fromUnixTimestamp64Milli(a.unix_milli)) AS last_seen
    FROM {METRIC_DB}.distributed_samples_v4_agg_5m AS a
    WHERE a.unix_milli >= {window_start_ms} AND a.unix_milli < {window_end_ms} AND {where_like}
    GROUP BY a.metric_name
    ORDER BY sample_count DESC, a.metric_name ASC
    LIMIT 2000
    """

def sql_errors_best_effort(start_iso: str, end_iso: str):
    return f"""
    SELECT
        timestamp,
        serviceName AS service,
        traceID AS trace_id,
        spanID AS span_id,
        exceptionType AS exception_type,
        exceptionMessage AS exception_message,
        exceptionStacktrace AS exception_stacktrace
    FROM {TRACE_DB}.distributed_signoz_error_index_v2
    WHERE timestamp >= toDateTime64(parseDateTimeBestEffort('{start_iso}'), 9)
      AND timestamp <  toDateTime64(parseDateTimeBestEffort('{end_iso}'),   9)
      AND (serviceName='{SERVICE_HINT}' OR exceptionType != '')
    ORDER BY timestamp DESC
    LIMIT 3000
    """

# ================== DB 健康检查 ==================
def check_db_connection() -> List[Dict[str, Any]]:
    """
    测试 MySQL TCP 3306 是否可达，失败则生成 errors 条目
    """
    errors = []
    db_host = os.getenv("MYSQL_HOST", "192.168.137.108")
    db_port = int(os.getenv("MYSQL_PORT", 3306))
    try:
        s = socket.create_connection((db_host, db_port), timeout=3)
        s.close()
    except Exception as e:
        errors.append({
            "timestamp": isoformat(utc_now()),
            "service": SERVICE_HINT,
            "trace_id": "",
            "span_id": "",
            "exception_type": "DBConnectionError",
            "exception_message": f"Cannot connect to MySQL at {db_host}:{db_port} - {e}",
            "exception_stacktrace": ""
        })
    return errors

# ================== 主执行 ==================
LOG_KEYS = ['time','service','service_instance_id','environment','message','level','host','service_version','logger_name','exception_type','exception_message','thread_name','trace_id','span_id']
TRACE_KEYS = ['timestamp','service','operation','trace_id','span_id','parent_id','duration_ms','error','status_code','http_status','http_method','http_route','http_url','db_system','db_name','db_operation','peer_service']
METRIC_KEYS_MAIN = ['metric_name','unit','type','service_name','service_namespace','environment','operation','http_status','span_kind','sample_count','min_value','max_value','avg_last','sum_value','first_seen','last_seen']
METRIC_KEYS_FALLBACK = ['metric_name','temporality','unit','type','service_name','service_namespace','environment','operation','http_status','span_kind','sample_count','min_value','max_value','avg_last','sum_value','first_seen','last_seen']
ERROR_KEYS = ['timestamp','service','trace_id','span_id','exception_type','exception_message','exception_stacktrace']

def run_once():
    state = load_state()
    last_ts = state.get('last_success_ts_utc')
    now_utc = utc_now()
    window_end = now_utc
    default_start = now_utc - timedelta(seconds=WINDOW_SEC)
    if last_ts:
        try: window_start = datetime.fromisoformat(last_ts)
        except: window_start = default_start
        if window_start.tzinfo is None: window_start = window_start.replace(tzinfo=timezone.utc)
        window_start = max(window_start, default_start)
    else:
        window_start = default_start

    window_start_iso = isoformat(window_start)
    window_end_iso   = isoformat(window_end)
    window_start_ns  = int(window_start.timestamp() * 1_000_000_000)
    window_end_ns    = int(window_end.timestamp()   * 1_000_000_000)
    window_start_ms  = int(window_start.timestamp() * 1000)
    window_end_ms    = int(window_end.timestamp()   * 1000)

    seq = mk_seq(window_end)
    outfile = os.path.join(OUTPUT_DIR, window_end.astimezone(timezone.utc).strftime('aiops_payload_%Y%m%d_%H%M.json.gz'))

    queries = {
        'logs':   (sql_logs_by_ns(window_start_ns, window_end_ns), LOG_KEYS),
        'traces': (sql_traces_best_effort(window_start_iso, window_end_iso), TRACE_KEYS),
        'metrics_main': (sql_metrics_main(window_start_ms, window_end_ms), METRIC_KEYS_MAIN),
        'errors': (sql_errors_best_effort(window_start_iso, window_end_iso), ERROR_KEYS),
    }

    results: Dict[str, List[List[Any]]] = {}
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = {ex.submit(run_ch_query, q): name for name, (q, _) in queries.items()}
        for fut in as_completed(futs):
            name = futs[fut]
            rows = []
            retry = 0
            while True:
                try: rows = fut.result(); break
                except Exception as e:
                    retry += 1
                    if retry >= MAX_RETRIES:
                        print(f"[ERROR] Query {name} failed after retries: {e}", file=sys.stderr)
                        break
                    sleep_s = RETRY_BASE_SEC * (2 ** (retry-1))
                    print(f"[WARN] Query {name} failed retry {retry} in {sleep_s}s: {e}")
                    time.sleep(sleep_s)
            results[name] = rows

    # metrics fallback
    metrics_rows = results.get('metrics_main', [])
    use_fallback = False
    if not metrics_rows:
        for attempt in range(MAX_RETRIES):
            try:
                metrics_rows = run_ch_query(sql_metrics_fallback(window_start_ms, window_end_ms))
                use_fallback = True
                break
            except Exception as e:
                sleep_s = RETRY_BASE_SEC * (2 ** attempt)
                print(f"[WARN] metrics_fallback retry {attempt+1} in {sleep_s}s: {e}")
                time.sleep(sleep_s)

    # ================== DB 健康检查 ==================
    db_errors = check_db_connection()

    payload = {
        "meta": {
            "generated_at": isoformat(now_utc),
            "window": {"start": window_start_iso, "end": window_end_iso, "duration_sec": int((window_end-window_start).total_seconds())},
            "source": f"{HOST}:{PORT}",
            "service_hint": SERVICE_HINT,
            "metrics_source": "agg_5m_with_labels" if not use_fallback else "agg_5m_fallback_no_labels",
            "seq": seq,
            "profile": "low-latency"
        },
        "logs":    rows_to_objs(LOG_KEYS, results.get('logs', [])),
        "traces":  rows_to_objs(TRACE_KEYS, results.get('traces', [])),
        "metrics": rows_to_objs(METRIC_KEYS_MAIN if not use_fallback else METRIC_KEYS_FALLBACK, metrics_rows),
        "errors":  rows_to_objs(ERROR_KEYS, results.get('errors', [])) + db_errors,
    }

    atomic_gzip_write(outfile, payload)
    save_state({"last_success_ts_utc": window_end_iso, "last_seq": seq})
    print(f"[OK] wrote {outfile} logs={len(payload['logs'])} traces={len(payload['traces'])} metrics={len(payload['metrics'])} errors={len(payload['errors'])}")
    return True

def main_loop():
    print("[START] AIOps data prepare (low-latency profile, thread-safe)")
    while True:
        try:
            ok = run_once()
        except Exception as e:
            print(f"[FATAL] run_once exception: {e}", file=sys.stderr)
            ok = False
        time.sleep(ROLL_INTERVAL_SEC if ok else ROLL_INTERVAL_SEC * 2)

if __name__ == '__main__':
    main_loop()
