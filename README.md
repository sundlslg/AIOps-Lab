# AIOps Control Plane for Kubernetes (newbee-mall)

## Project Overview

This project implements an **AIOps Control Plane** for monitoring and analyzing Kubernetes workloads. It automatically detects anomalies, correlates metrics/logs/traces, performs root cause analysis (RCA), and generates action recommendations.  
The focus application is **newbee-mall**, an e-commerce platform deployed on a K8s cluster.

---

## Architecture Overview
- newbee-mall (K8s workloads)
  - Metrics (CPU, latency, saturation)
  - Logs (application + container logs)
  - Traces (request-level traces)
- SigNoz (OpenTelemetry-based Observability)
- AIOps Data Preparation (low-latency export)
- AIOps Agent (Control Plane)
  - Detectors (metrics / pod / DB)
  - Correlator
  - RCA Engine (V1 / V2)
  - Policy & Decision Layer
  - FlashRAG V3 (LLM-based RCA)

## Layer Details

### newbee-mall (K8s workloads)
- Collects metrics (CPU, latency, saturation)
- Logs (application + container logs)
- Request-level traces

### SigNoz (OpenTelemetry-based Observability)
- Receives metrics, logs, traces from newbee-mall
- Exposes Prometheus-compatible metrics for AIOps agent

### AIOps Data Preparation
- Low-latency export of collected observability data
- Generates gzipped JSON payloads for the agent

### AIOps Agent (Control Plane)
- **Detectors**: Metrics, Pod status, Database health
- **Correlator**: Links anomalies across services
- **RCA Engine (V1/V2)**: Rule-based root cause analysis
- **Policy & Decision Layer**: Generates actionable recommendations
- **FlashRAG V3**: LLM-based RCA for contextual insights

## Monitoring Data Sources

- **Metrics**: Collected via OpenTelemetry, exported to SigNoz. Includes CPU usage, request latency, pod saturation.
- **Logs**: Application logs (newbee-mall) and container logs collected via SigNoz agent.
- **Traces**: Distributed traces at request level collected via OpenTelemetry SDK instrumented in newbee-mall.
- **Database Health**: TCP check to MySQL; optionally include query latency metrics.

The agent reads these observability payloads from a low-latency export directory and performs anomaly detection and RCA.

## AIOps Agent Features

- Detects anomalies in metrics, pods, and databases
- Correlates anomalies across services
- Generates RCA (V1/V2) and LLM-assisted RCA (FlashRAG V3)
- Generates action recommendations (such as scale pods, alert DB issues etc.)
- Supports configurable auto-scaling via agent_config.json
- Runs in Control Plane mode: analyzes all collected data without performing destructive actions
