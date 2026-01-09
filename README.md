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
