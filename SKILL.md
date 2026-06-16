---
name: srao-workflow-orchestrator
description: 工作流编排落地与执行监控（SRAO阶段4-5）。将智能体图谱转化为可执行的DAG工作流，选择编排引擎，配置容错与监控，实现反馈闭环优化。触发词：工作流编排、DAG执行、编排引擎、Temporal、Prefect、Airflow、工作流监控、智能体调度、工作流部署、编排落地、从图谱到执行、工作流优化、反馈闭环。当用户已有Agent图谱需要落地执行时使用。
---

# SRAO 工作流编排落地器

将智能体图谱转化为可执行的编排工作流，配置引擎、监控与反馈闭环。

## 前置条件

需先完成 `srao-agent-graph` 的Agent图谱构建，获得 task_dag + agent_graph 输出。
若用户直接使用本skill，先确认是否已有DAG定义，否则引导先完成图谱构建。

## 核心流程

```
Agent图谱 → 编排引擎选型 → DAG定义文件 → 部署执行 → 监控告警 → 反馈优化
```

## 阶段4：编排执行

### 编排引擎选型

| 引擎 | 适用场景 | 优势 | 劣势 |
|------|---------|------|------|
| **Temporal** | 长时运行、需人工审批、复杂重试 | 持久化状态、版本管理、强一致性 | 学习曲线陡 |
| **Prefect** | 数据流水线、Python重度 | Python原生、易调试、云原生 | 复杂分支支持弱 |
| **Airflow** | 定时批处理、成熟生态 | 生态丰富、社区大 | 实时性差、DAG静态 |
| **AWS Step Functions** | AWS生态内 | 全托管、按执行付费 | 厂商锁定 |
| **轻量自建** | 原型验证、简单场景 | 零依赖、快速启动 | 功能有限 |

**选型决策树**：
```
需要人机交互/长时运行？
  ├─ 是 → Temporal
  └─ 否 → 主要是Python数据处理？
        ├─ 是 → Prefect
        └─ 否 → 已有Airflow基础设施？
              ├─ 是 → Airflow
              └─ 否 → Temporal（推荐默认）
```

### DAG定义文件生成

根据用户选择的引擎，自动生成对应的DAG定义文件。

**Temporal (TypeScript)**：见 `references/temporal-template.ts`
**Prefect (Python)**：见 `references/prefect-template.py`
**Airflow (Python)**：见 `references/airflow-template.py`

### 智能体接口标准化

所有Agent必须统一接口规范：

```yaml
agent_interface:
  endpoint: "POST /agent/{agent_id}/execute"
  request:
    schema_version: string
    task_id: string
    input: object        # 与SKILL.md中input_schema一致
    context: object      # 工作流上下文（可选）
    timeout_sec: number
  
  response:
    schema_version: string
    task_id: string
    status: "success" | "failure" | "pending"
    output: object       # 与SKILL.md中output_schema一致
    error: { code, message, retryable }
    metrics: { latency_ms, token_usage, cost }
```

### 容错策略配置

```yaml
fault_tolerance:
  retry_policy:
    max_attempts: 3
    backoff: exponential    # fixed | exponential | linear
    initial_interval_sec: 2
    max_interval_sec: 60
    
  circuit_breaker:
    enabled: true
    failure_threshold: 5     # 连续失败N次后熔断
    reset_timeout_sec: 30     # 熔断后多久尝试恢复
    
  fallback:
    strategy: degrade         # degrade | alternate_agent | human_escalation
    alternate_agent_id: AGT-999   # 备用Agent（strategy=alternate_agent时）
    human_escalation_webhook: "https://..."  # 人工升级
    
  timeout:
    per_task_sec: 300
    per_workflow_sec: 3600
```

## 阶段5：反馈优化

### 执行日志结构

每次工作流执行自动记录：

```json
{
  "execution_id": "EXEC-20250616-001",
  "workflow_name": "风机健康监测",
  "start_time": "2025-06-16T08:00:00Z",
  "end_time": "2025-06-16T08:02:35Z",
  "status": "success",
  "task_results": [
    {
      "task_id": "T1",
      "agent_id": "AGT-001",
      "status": "success",
      "latency_ms": 1200,
      "retry_count": 0,
      "output_summary": "采集数据点1532个"
    }
  ],
  "metrics": {
    "total_latency_ms": 155000,
    "token_usage_total": 45200,
    "estimated_cost_usd": 0.68,
    "sla_met": true
  },
  "anomalies": []
}
```

### 监控面板配置

见 `references/monitoring-setup.md` —— 包含Prometheus指标定义、Grafana面板JSON、告警规则。

### 反馈闭环：从执行到优化

```
执行完成
  ↓
记录execution_log → 存入向量DB
  ↓
分析：哪些task重试多？哪些Agent慢？
  ↓
更新SRM：修正constraints（如timeout过短）
  ↓
更新Agent图谱：替换低准确率Agent
  ↓
更新工作流模板：调整并行度、重排依赖
  ↓
重新部署 → 下一轮执行
```

## 从零到一实施Checklist

部署工作流时按此顺序验证：

- [ ] DAG定义文件语法检查通过
- [ ] 每个Agent的endpoint可访问（健康检查）
- [ ] 串行依赖：T2在T1完成后才触发 ✅
- [ ] 并行任务：T2和T3同时执行 ✅
- [ ] 重试策略：Agent失败自动重试3次 ✅
- [ ] 熔断：连续失败5次后跳过该Agent ✅
- [ ] 超时：单任务超过300s自动终止 ✅
- [ ] 人工升级：Agent无法处理时推送通知 ✅
- [ ] 监控面板：所有Agent的延迟/成功率可见 ✅
- [ ] 执行日志：完整写入，可回溯 ✅

## 快速命令

用户说"帮我部署这个工作流"时，按以下步骤：

1. 读取Agent图谱JSON
2. 询问用户首选引擎（默认Temporal）
3. 生成对应的DAG定义文件
4. 生成docker-compose.yml（本地测试用）
5. 输出监控面板配置
6. 提示用户执行 `docker-compose up` 启动

---

> 三个阶段Skill的协作关系：
> `srao-domain-modeler` → 产出：SRM文档 + 领域模型
> `srao-agent-graph` → 产出：task_dag + agent_graph + Agent能力卡
> `srao-workflow-orchestrator` → 产出：可执行DAG代码 + 监控配置 + 反馈闭环
