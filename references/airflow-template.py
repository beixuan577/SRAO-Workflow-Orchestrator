# Airflow DAG Template - Python
# 基于 srao-workflow-orchestrator 输出的 task_dag 自动生成

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.sensors.http_sensor import HttpSensor
from datetime import datetime, timedelta
import json

# ====== 默认参数 ======

default_args = {
    'owner': 'srao',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=2),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(seconds=60),
}

# ====== DAG定义 ======

with DAG(
    dag_id='srao_workflow_template',
    default_args=default_args,
    description='基于SRAO框架的工作流模板',
    schedule_interval=timedelta(hours=1),  # 根据实际trigger调整
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['srao', 'agent-cluster'],
) as dag:

    # T1: 第一步
    t1 = SimpleHttpOperator(
        task_id='T1_agent_execute',
        http_conn_id='agent_service',  # 需在Airflow中配置
        endpoint='/agent/AGT-001/execute',
        data=json.dumps({
            "task_id": "T1",
            "input": {},  # 从XCom或配置注入
        }),
        headers={'Content-Type': 'application/json'},
        response_check=lambda response: response.json().get('status') == 'success',
    )

    # T2: 依赖T1
    t2 = SimpleHttpOperator(
        task_id='T2_agent_execute',
        http_conn_id='agent_service',
        endpoint='/agent/AGT-002/execute',
        data=json.dumps({"task_id": "T2", "input": {}}),
        headers={'Content-Type': 'application/json'},
    )

    # T3: 依赖T1，与T2并行
    t3 = SimpleHttpOperator(
        task_id='T3_agent_execute',
        http_conn_id='agent_service',
        endpoint='/agent/AGT-003/execute',
        data=json.dumps({"task_id": "T3", "input": {}}),
        headers={'Content-Type': 'application/json'},
    )

    # T4: 依赖T2和T3
    t4 = SimpleHttpOperator(
        task_id='T4_agent_execute',
        http_conn_id='agent_service',
        endpoint='/agent/AGT-004/execute',
        data=json.dumps({"task_id": "T4", "input": {}}),
        headers={'Content-Type': 'application/json'},
    )

    # 依赖关系
    t1 >> [t2, t3] >> t4
    # 等价于:
    # t1.set_downstream([t2, t3])
    # t4.set_upstream([t2, t3])
