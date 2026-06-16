# SRAO Workflow Orchestrator

> **SRAO Stage 4-5**: Workflow orchestration, execution, and monitoring

Part of the [SRAO Framework](https://github.com/beixuan577?tab=repositories&q=srao) — a systematic methodology for building multi-agent workflow orchestration systems across industries.

## What It Does

Converts agent graphs into executable workflow DAGs with full operational support:

- **Stage 4 — Orchestration Execution**: Select engine, generate DAG code, deploy with fault tolerance
- **Stage 5 — Feedback Optimization**: Log execution, monitor SLAs, close the optimization loop

## Key Features

- ⚙️ 3 orchestration engines: [Temporal](https://temporal.io/) (TypeScript), [Prefect](https://prefect.io/) (Python), [Airflow](https://airflow.apache.org/) (Python)
- 🛡️ Fault tolerance: exponential backoff retry, circuit breaker, human escalation
- 📊 Full monitoring stack: Prometheus metrics + Grafana dashboards + alert rules
- 🔄 Feedback loop: execution logs → analysis → update SRM/Agent Graph → re-deploy
- 🐳 Docker Compose monitoring stack ready to deploy

## Engine Selection Guide

`
Need human-in-the-loop / long-running tasks?
  ├─ Yes → Temporal
  └─ No → Mainly Python data pipelines?
        ├─ Yes → Prefect
        └─ No → Already have Airflow infra?
              ├─ Yes → Airflow
              └─ No → Temporal (recommended default)
`

## Templates

| Engine | Template File | Language |
|--------|---------------|----------|
| Temporal | eferences/temporal-template.ts | TypeScript |
| Prefect | eferences/prefect-template.py | Python |
| Airflow | eferences/airflow-template.py | Python |
| Monitoring | eferences/monitoring-setup.md | Prometheus + Grafana |

## Quick Start

1. Feed agent graph output from [SRAO-Agent-Graph](https://github.com/beixuan577/SRAO-Agent-Graph)
2. Select an orchestration engine
3. Generate DAG code from templates
4. Deploy with docker-compose up
5. Monitor via Grafana dashboard at http://localhost:3000

## SRAO Framework

`
SRAO-Domain-Modeler ──SRM──> SRAO-Agent-Graph ──DAG+Graph──> SRAO-Workflow-Orchestrator
   (Model & Structure)         (Decompose & Map)              (Orchestrate & Monitor)
`

## License

MIT
