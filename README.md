# SRAO Workflow Orchestrator

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![SRAO Framework](https://img.shields.io/badge/SRAO-Framework-blueviolet)](https://github.com/beixuan577?tab=repositories&q=srao)
[![Stage](https://img.shields.io/badge/Stage-4--5-red)]()

> **From agent graph to running system in minutes, not months.**
> Generate executable DAG code, deploy with fault tolerance, monitor with Prometheus + Grafana.

Part of the **SRAO Framework**. Takes agent graph output from [SRAO-Agent-Graph](https://github.com/beixuan577/SRAO-Agent-Graph) and turns it into production-ready workflow code.

---

## The Problem

You've designed your multi-agent system. Now what?

- Which orchestration engine to use? Temporal? Prefect? Airflow?
- How do you handle retries, timeouts, circuit breakers?
- How do you monitor 20+ agents in real time?
- How do you close the feedback loop for continuous improvement?

SRAO Workflow Orchestrator answers all of these with ready-to-use templates and configurations.

## What It Does

**Stage 4 - Orchestration Execution**
- Select the right engine with a decision tree
- Generate DAG code from agent graph definitions
- Configure fault tolerance (retry, circuit breaker, human escalation)
- Standardize agent interfaces (HTTP/gRPC)

**Stage 5 - Feedback Optimization**
- Record execution logs with metrics (latency, token usage, cost)
- Monitor agent SLAs with Prometheus + Grafana
- Alert on failures, high latency, circuit breaker triggers
- Close the loop: analyze -> update SRM/Agent Graph -> re-deploy

## Engine Selection Decision Tree

`
Need human-in-the-loop or long-running tasks?
  +-- Yes -> Temporal
  +-- No -> Mainly Python data pipelines?
        +-- Yes -> Prefect
        +-- No -> Already have Airflow infrastructure?
              +-- Yes -> Airflow
              +-- No -> Temporal (recommended default)
`

## Code Templates

| Engine | Template | Language |
|--------|----------|----------|
| Temporal | references/temporal-template.ts | TypeScript |
| Prefect | references/prefect-template.py | Python |
| Airflow | references/airflow-template.py | Python |
| Monitoring | references/monitoring-setup.md | Prometheus + Grafana |

### Temporal Example (TypeScript)

`	ypescript
// Auto-generated from agent graph
const [t2Result, t3Result] = await Promise.all([
  agentExecute({ agent_id: 'AGT-002', task_id: 'T2', input: t1Result }),
  agentExecute({ agent_id: 'AGT-003', task_id: 'T3', input: t1Result }),
]);

// Conditional branching
if (t4Result.output?.severity === 'high') {
  await notifyHuman({ message: 'High severity alert' });
}
`

### Prefect Example (Python)

`python
@flow(name="workflow-template")
def workflow_template(t1_input):
    t1_result = agent_execute(agent_id="AGT-001", task_id="T1", input_data=t1_input)
    
    # Parallel execution
    t2_future = agent_execute.submit(agent_id="AGT-002", ...)
    t3_future = agent_execute.submit(agent_id="AGT-003", ...)
    t2_result, t3_result = t2_future.result(), t3_future.result()
    
    return agent_execute(agent_id="AGT-004", input_data={...})
`

## Fault Tolerance Out of the Box

`yaml
retry_policy:
  max_attempts: 3
  backoff: exponential
  initial_interval_sec: 2

circuit_breaker:
  failure_threshold: 5
  reset_timeout_sec: 30

fallback:
  strategy: degrade  # or alternate_agent / human_escalation
`

## Monitoring Stack

Included: Prometheus metrics, Grafana dashboards, alert rules, Docker Compose.

`ash
docker-compose up  # Starts Prometheus + Grafana + Alertmanager
# Grafana at http://localhost:3000 (admin/admin)
# Prometheus at http://localhost:9090
`

Key dashboards:
- Agent success rate (5-min window)
- Agent P95 latency by agent_id
- Workflow execution trends
- Token consumption TOP 10

## Quick Start

1. Feed agent graph from [SRAO-Agent-Graph](https://github.com/beixuan577/SRAO-Agent-Graph)
2. Select an orchestration engine (or use the decision tree)
3. Generate DAG code from templates
4. Deploy with docker-compose up
5. Monitor at http://localhost:3000

## SRAO Framework Pipeline

`
+----------------------+     +----------------------+     +-----------------------------+
| SRAO-Domain-Modeler  |---->| SRAO-Agent-Graph     |---->| SRAO-Workflow-Orchestrator  |
| (Model & Structure)  | SRM | (Decompose & Map)    | DAG | (Orchestrate & Monitor)     |
+----------------------+     +----------------------+     +-----------------------------+
`

## License

MIT