# Prefect Workflow Template - Python
# 基于 srao-workflow-orchestrator 输出的 task_dag 自动生成

from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import timedelta
from typing import Any, Dict, Optional
import httpx
import json

# ====== 通用Agent执行Task ======

@task(
    retries=3,
    retry_delay_seconds=2,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=1),
)
def agent_execute(
    agent_id: str,
    task_id: str,
    input_data: Dict[str, Any],
    timeout_sec: int = 300,
) -> Dict[str, Any]:
    """调用Agent执行原子任务"""
    logger = get_run_logger()
    logger.info(f"Executing agent={agent_id}, task={task_id}")
    
    # 实际调用Agent的HTTP endpoint
    # 替换为实际的Agent服务地址
    agent_url = f"http://localhost:8080/agent/{agent_id}/execute"
    
    payload = {
        "schema_version": "1.0",
        "task_id": task_id,
        "input": input_data,
        "timeout_sec": timeout_sec,
    }
    
    with httpx.Client(timeout=timeout_sec + 10) as client:
        resp = client.post(agent_url, json=payload)
        resp.raise_for_status()
        result = resp.json()
    
    if result.get("status") != "success":
        raise RuntimeError(f"Agent {agent_id} failed: {result.get('error')}")
    
    logger.info(f"Agent {agent_id} completed in {result.get('metrics', {}).get('latency_ms')}ms")
    return result["output"]


# ====== 工作流定义 ======
# 根据task_dag自动生成，以下为模板示例

@flow(
    name="workflow-template",
    description="基于SRAO框架的工作流模板",
    timeout_seconds=3600,
)
def workflow_template(
    t1_input: Dict[str, Any],
    t2_input: Dict[str, Any],
    t3_input: Dict[str, Any],
) -> Dict[str, Any]:
    """主工作流：串行+并行+条件分支"""
    logger = get_run_logger()
    logger.info("Workflow started")
    
    # T1: 第一步（无依赖）
    t1_result = agent_execute(
        agent_id="AGT-001",
        task_id="T1",
        input_data=t1_input,
    )
    
    # T2, T3: 并行执行
    t2_future = agent_execute.submit(
        agent_id="AGT-002",
        task_id="T2",
        input_data={**t2_input, "upstream": t1_result},
    )
    t3_future = agent_execute.submit(
        agent_id="AGT-003",
        task_id="T3",
        input_data={**t3_input, "upstream": t1_result},
    )
    
    t2_result = t2_future.result()
    t3_result = t3_future.result()
    
    # T4: 依赖T2和T3
    t4_result = agent_execute(
        agent_id="AGT-004",
        task_id="T4",
        input_data={
            "t2_output": t2_result,
            "t3_output": t3_result,
        },
    )
    
    # 条件分支
    if t4_result.get("severity") == "high":
        logger.warning(f"高危预警: {json.dumps(t4_result, ensure_ascii=False)}")
        # 触发人工升级
        notify_human(message=f"高危预警: {t4_result}")
    
    logger.info("Workflow completed")
    return {
        "status": "success",
        "final_output": t4_result,
    }


@task
def notify_human(message: str, channel: str = "webhook"):
    """人工升级通知"""
    # 替换为实际的Webhook URL
    webhook_url = "https://hooks.example.com/srao-alert"
    with httpx.Client() as client:
        client.post(webhook_url, json={"message": message, "channel": channel})


# ====== 本地测试 ======
if __name__ == "__main__":
    result = workflow_template(
        t1_input={"source": "test"},
        t2_input={"param": "value"},
        t3_input={"param": "value"},
    )
    print(f"Result: {json.dumps(result, ensure_ascii=False, indent=2)}")
