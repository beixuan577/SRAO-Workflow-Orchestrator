// Temporal Workflow Template - TypeScript
// 基于 srao-workflow-orchestrator 输出的 task_dag 自动生成

import { proxyActivities, executeChild, workflow, set, condition } from '@temporalio/workflow';
import type * as activities from './activities';

// 自动生成的Activity代理
const {
  agentExecute,    // 通用Agent执行Activity
  notifyHuman,     // 人工升级通知
} = proxyActivities<typeof activities>({
  startToCloseTimeout: '5 minutes',
  retry: {
    maximumAttempts: 3,
    initialInterval: '2 seconds',
    backoffCoefficient: 2,
    maximumInterval: '60 seconds',
  },
});

// ====== 工作流定义 ======
// 根据task_dag自动生成，以下为模板示例

export async function workflowTemplate(input: WorkflowInput): Promise<WorkflowOutput> {
  // T1: 第一步（无依赖）
  const t1Result = await agentExecute({
    agent_id: 'AGT-001',
    task_id: 'T1',
    input: input.t1Input,
    timeout_sec: 300,
  });

  if (!t1Result.status) {
    throw new Error(`T1 failed: ${t1Result.error?.message}`);
  }

  // T2, T3: 并行执行（都依赖T1）
  const [t2Result, t3Result] = await Promise.all([
    agentExecute({
      agent_id: 'AGT-002',
      task_id: 'T2',
      input: { ...input.t2Input, upstream: t1Result.output },
      timeout_sec: 300,
    }),
    agentExecute({
      agent_id: 'AGT-003',
      task_id: 'T3',
      input: { ...input.t3Input, upstream: t1Result.output },
      timeout_sec: 300,
    }),
  ]);

  // T4: 依赖T2和T3
  const t4Result = await agentExecute({
    agent_id: 'AGT-004',
    task_id: 'T4',
    input: {
      t2_output: t2Result.output,
      t3_output: t3Result.output,
    },
    timeout_sec: 300,
  });

  // 条件分支
  if (t4Result.output?.severity === 'high') {
    await notifyHuman({
      message: `高危预警: ${JSON.stringify(t4Result.output)}`,
      channel: 'webhook',
    });
  }

  return {
    status: 'success',
    final_output: t4Result.output,
    execution_metrics: {
      tasks_completed: 4,
      tasks_parallel: 2,
    },
  };
}

// ====== 类型定义 ======

interface WorkflowInput {
  t1Input: Record<string, any>;
  t2Input: Record<string, any>;
  t3Input: Record<string, any>;
}

interface WorkflowOutput {
  status: 'success' | 'failure';
  final_output: Record<string, any>;
  execution_metrics: {
    tasks_completed: number;
    tasks_parallel: number;
  };
}

// ====== 通用Activity接口 ======
// activities.ts 中需实现:

interface AgentExecuteInput {
  agent_id: string;
  task_id: string;
  input: Record<string, any>;
  timeout_sec: number;
}

interface AgentExecuteOutput {
  status: boolean;
  output: Record<string, any>;
  error?: { code: string; message: string; retryable: boolean };
  metrics?: { latency_ms: number; token_usage: number };
}

interface NotifyHumanInput {
  message: string;
  channel: 'webhook' | 'email' | 'sms';
}
