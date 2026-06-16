# 监控面板配置

## Prometheus 指标定义

Agent执行相关指标，需在每个Agent服务中暴露：

```yaml
# agent_metrics.py - Python示例（使用prometheus_client）

from prometheus_client import Counter, Histogram, Gauge

# 请求计数
agent_requests_total = Counter(
    'agent_requests_total',
    'Total agent execution requests',
    ['agent_id', 'status']  # status: success | failure | timeout
)

# 执行延迟
agent_execution_duration_seconds = Histogram(
    'agent_execution_duration_seconds',
    'Agent execution duration',
    ['agent_id'],
    buckets=[0.5, 1, 2, 5, 10, 30, 60, 120, 300]
)

# Token使用量
agent_token_usage_total = Counter(
    'agent_token_usage_total',
    'Total tokens consumed by agent',
    ['agent_id', 'model']
)

# 当前在执行数
agent_active_executions = Gauge(
    'agent_active_executions',
    'Currently executing agent tasks',
    ['agent_id']
)

# 工作流级别指标
workflow_duration_seconds = Histogram(
    'workflow_duration_seconds',
    'Full workflow execution duration',
    ['workflow_name', 'status']
)

workflow_tasks_total = Counter(
    'workflow_tasks_total',
    'Total tasks executed in workflows',
    ['workflow_name', 'task_id', 'status']
)
```

## Grafana 面板 JSON

导入以下面板（关键面板说明）：

### Panel 1: Agent 成功率

```json
{
  "title": "Agent成功率 (5分钟窗口)",
  "type": "stat",
  "targets": [{
    "expr": "sum(rate(agent_requests_total{status=\"success\"}[5m])) / sum(rate(agent_requests_total[5m])) * 100"
  }],
  "fieldConfig": {
    "defaults": {
      "unit": "percent",
      "thresholds": {
        "steps": [
          {"value": 0, "color": "red"},
          {"value": 90, "color": "yellow"},
          {"value": 99, "color": "green"}
        ]
      }
    }
  }
}
```

### Panel 2: Agent P95延迟

```json
{
  "title": "Agent P95延迟",
  "type": "timeseries",
  "targets": [{
    "expr": "histogram_quantile(0.95, sum(rate(agent_execution_duration_seconds_bucket[5m])) by (le, agent_id))"
  }],
  "legendFormat": "{{agent_id}}"
}
```

### Panel 3: 工作流执行趋势

```json
{
  "title": "工作流执行数 (1小时)",
  "type": "timeseries",
  "targets": [{
    "expr": "sum(rate(workflow_duration_seconds_count[1h])) by (workflow_name, status)"
  }],
  "legendFormat": "{{workflow_name}} - {{status}}"
}
```

### Panel 4: Token消耗排行

```json
{
  "title": "Token消耗TOP10 Agent",
  "type": "barchart",
  "targets": [{
    "expr": "topk(10, sum(agent_token_usage_total) by (agent_id))"
  }]
}
```

## 告警规则

```yaml
# alert_rules.yml

groups:
  - name: srao_agent_alerts
    rules:
      - alert: AgentHighFailureRate
        expr: |
          sum(rate(agent_requests_total{status="failure"}[5m])) 
          / sum(rate(agent_requests_total[5m])) > 0.1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Agent失败率超过10%"
          description: "Agent {{ $labels.agent_id }} 5分钟内失败率 {{ $value | humanizePercentage }}"
      
      - alert: AgentHighLatency
        expr: |
          histogram_quantile(0.95, 
            sum(rate(agent_execution_duration_seconds_bucket[5m])) by (le, agent_id)
          ) > 60
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "Agent P95延迟超过60秒"
      
      - alert: WorkflowTimeout
        expr: |
          workflow_duration_seconds{status="timeout"} > 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "工作流执行超时"
          description: "工作流 {{ $labels.workflow_name }} 执行超时"

      - alert: CircuitBreakerOpen
        expr: |
          agent_active_executions == 0 
          AND sum(rate(agent_requests_total{status="failure"}[1m])) > 5
        for: 2m
        labels:
          severity: critical
        annotations:
          summary: "熔断器可能已打开"
          description: "Agent {{ $labels.agent_id }} 连续失败，可能已触发熔断"
```

## Docker Compose 监控栈

```yaml
# docker-compose.monitoring.yml

version: '3.8'
services:
  prometheus:
    image: prom/prometheus:latest
    ports:
      - "9090:9090"
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
      - ./alert_rules.yml:/etc/prometheus/alert_rules.yml
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'

  grafana:
    image: grafana/grafana:latest
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
    volumes:
      - grafana-data:/var/lib/grafana

  alertmanager:
    image: prom/alertmanager:latest
    ports:
      - "9093:9093"
    volumes:
      - ./alertmanager.yml:/etc/alertmanager/config.yml

volumes:
  grafana-data:
```
