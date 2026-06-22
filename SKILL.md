---
name: srao-workflow-orchestrator
description: 宸ヤ綔娴佺紪鎺掕惤鍦颁笌鎵ц鐩戞帶锛圫RAO闃舵4-5锛夈€傚皢鏅鸿兘浣撳浘璋辫浆鍖栦负鍙墽琛岀殑DAG宸ヤ綔娴侊紝閫夊瀷缂栨帓寮曟搸锛圱emporal/Prefect/Airflow锛夛紝閰嶇疆澶氬眰瀹归敊锛堥噸璇?鐔旀柇/闄嶇骇/浜哄伐鍗囩骇锛夛紝闆嗘垚Prometheus+Grafana鐩戞帶锛屽缓绔嬩粠鎵ц鍒颁紭鍖栫殑鍙嶉闂幆銆傝Е鍙戣瘝锛氬伐浣滄祦缂栨帓銆丏AG鎵ц銆佺紪鎺掑紩鎿庛€乀emporal銆丳refect銆丄irflow銆佸伐浣滄祦鐩戞帶銆佹櫤鑳戒綋璋冨害銆佸伐浣滄祦閮ㄧ讲銆佺紪鎺掕惤鍦般€佷粠鍥捐氨鍒版墽琛屻€佸伐浣滄祦浼樺寲銆佸弽棣堥棴鐜€丏ocker Compose閮ㄧ讲銆丳rometheus鐩戞帶銆丟rafana闈㈡澘銆佸閿欓厤缃€佺啍鏂檷绾с€傚綋鐢ㄦ埛宸叉湁Agent鍥捐氨闇€瑕佽惤鍦版墽琛屾椂浣跨敤銆?
---

# SRAO 宸ヤ綔娴佺紪鎺掕惤鍦板櫒 v2.0

> **鏍稿績浠诲姟**锛氬皢鏅鸿兘浣撳浘璋辫浆鍖栦负鐢熶骇绾у彲鎵ц鐨勭紪鎺掑伐浣滄祦锛岄厤缃畬鏁寸殑鐩戞帶銆佸閿欏拰鍙嶉闂幆銆?

鎵挎帴 `srao-agent-graph` 鐨凙gent鍥捐氨锛屽皢钃濆浘钀藉湴涓哄彲杩愯銆佸彲鐩戞帶銆佸彲浼樺寲鐨勭敓浜х郴缁熴€?

---

## 涓€銆佸墠缃潯浠?

闇€鍏堝畬鎴?`srao-agent-graph` 鐨凙gent鍥捐氨鏋勫缓锛岃幏寰?task_dag + agent_cards + agent_graph 杈撳嚭銆?
鑻ョ敤鎴风洿鎺ヤ娇鐢ㄦ湰skill锛屽厛纭鏄惁宸叉湁DAG瀹氫箟锛屽惁鍒欏紩瀵煎厛瀹屾垚鍥捐氨鏋勫缓銆?

---

## 浜屻€佹牳蹇冩祦绋?

```
Agent鍥捐氨 鈫?寮曟搸閫夊瀷 鈫?DAG瀹氫箟鏂囦欢 鈫?瀹瑰櫒鍖栭儴缃?鈫?鐩戞帶鍛婅 鈫?鍙嶉浼樺寲闂幆
```

---

## 涓夈€侀樁娈?锛氱紪鎺掓墽琛?

### 3.1 缂栨帓寮曟搸璇︾粏閫夊瀷鐭╅樀

| 缁村害 | Temporal | Prefect | Airflow | Step Functions | 杞婚噺鑷缓 |
|------|----------|---------|---------|----------------|----------|
| **闀挎椂杩愯** | 鉁?鍘熺敓鏀寔 | 鈿狅笍 浠匬ython | 鉂?涓嶉€傚悎 | 鉁?鏀寔 | 鈿狅笍 鑷繁绠＄悊 |
| **浜烘満浜や簰** | 鉁?Signal/Wait | 鈿狅笍 鎵嬪姩瀹炵幇 | 鉂?| 鉁?Task Token | 鉂?|
| **澶嶆潅鍒嗘敮** | 鉁?鐏垫椿 | 鈿狅笍 鏈夐檺 | 鉂?DAG闈欐€?| 鉁?Choice鐘舵€?| 鈿狅笍 |
| **Python鍘熺敓** | 鈿狅笍 闇€SDK | 鉁?瀹屽叏鍘熺敓 | 鉁?鍘熺敓 | 鉂?JSON瀹氫箟 | 鉁?|
| **瀛︿範鏇茬嚎** | 馃敶 闄″抄 | 馃煛 涓瓑 | 馃煛 涓瓑 | 馃煝 绠€鍗?| 馃煝 鏈€绠€鍗?|
| **绀惧尯鐢熸€?* | 馃煛 蹇€熸垚闀?| 馃煝 娲昏穬 | 馃煝 鏈€鎴愮啛 | 馃煝 AWS鎴愮啛 | 馃敶 闈犺嚜宸?|
| **閮ㄧ讲澶嶆潅搴?* | 馃煛 Docker+DB | 馃煝 pip install | 馃煛 Docker+DB | 馃煝 鍏ㄦ墭绠?| 馃煝 鍗曡繘绋?|
| **鎴愭湰** | 鑷缓鍏嶈垂 | 寮€婧愬厤璐?浜戜粯璐?| 鑷缓鍏嶈垂 | 鎸夋墽琛屼粯璐?| 闆舵垚鏈?|
| **鍘傚晢閿佸畾** | 寮€婧?| 寮€婧?| 寮€婧?| 馃敶 AWS閿佸畾 | 馃煝 瀹屽叏鑷敱 |

### 3.2 閫夊瀷鍐崇瓥鏍?

```
Q1: 宸ヤ綔娴佹槸鍚﹂渶瑕佷汉鏈轰氦浜掞紙瀹℃壒銆佷汉宸ュ鏍革級锛?
  鈹溾攢 鏄?鈫?Q2: 鏄惁鏈堿WS璐﹀彿锛?
  鈹?      鈹溾攢 鏄?鈫?Step Functions锛堜汉鍔涙姇鍏ユ渶灏忥級
  鈹?      鈹斺攢 鍚?鈫?Temporal锛堝敮涓€寮€婧愮殑鎴愮啛浜烘満浜や簰鏂规锛?
  鈹斺攢 鍚?鈫?Q3: 鏄惁涓昏鏄疨ython鏁版嵁澶勭悊娴佹按绾匡紵
          鈹溾攢 鏄?鈫?Prefect锛圥ython鍘熺敓锛屼綋楠屾渶浣筹級
          鈹斺攢 鍚?鈫?Q4: 鍥㈤槦宸叉湁Airflow鍩虹璁炬柦锛?
                  鈹溾攢 鏄?鈫?Airflow锛堝鐢ㄧ幇鏈夎兘鍔涳級
                  鈹斺攢 鍚?鈫?Q5: 闇€瑕佸揩閫熷師鍨嬭繕鏄ǔ鍋ョ敓浜э紵
                          鈹溾攢 蹇€熷師鍨?鈫?Prefect锛堥浂閰嶇疆寮€鍙戯級
                          鈹斺攢 绋冲仴鐢熶骇 鈫?Temporal锛堝姛鑳芥渶寮猴級
```

**榛樿鎺ㄨ崘**锛?
- 馃 **Temporal** 鈥?閫傚悎90%鐨勬櫤鑳戒綋缂栨帓鍦烘櫙锛堥暱鏃惰繍琛屻€侀噸璇曘€佷汉鏈轰氦浜掞級
- 馃 **Prefect** 鈥?閫傚悎绾疨ython鏁版嵁鍒嗘瀽娴佹按绾?
- 馃 **杞婚噺鑷缓** 鈥?閫傚悎PoC楠岃瘉鍜屾瀬绠€鍦烘櫙

### 3.3 鏅鸿兘浣撴帴鍙ｆ爣鍑嗚鑼?

鎵€鏈堿gent蹇呴』閬靛惊姝ゆ爣鍑嗘帴鍙ｏ紝纭繚缂栨帓寮曟搸鍙粺涓€璋冨害锛?

```yaml
agent_interface_standard:
  base_url: "http://{agent_host}:{port}"
  
  # 鍋ュ悍妫€鏌ワ紙蹇呴』瀹炵幇锛?
  health:
    method: GET
    path: "/health"
    response: { status: "healthy", version: "1.0.0", uptime_sec: 3600 }
    
  # 鎵ц鎺ュ彛锛堝繀椤诲疄鐜帮級
  execute:
    method: POST
    path: "/agent/{agent_id}/execute"
    content_type: "application/json"
    
    request:
      schema_version: "1.0"
      task_id: "T1"
      execution_id: "EXEC-20260622-001"
      input:
        # 涓嶢gent鑳藉姏鍗′腑瀹氫箟鐨?input_schema 涓€鑷?
        field_1: "value"
        field_2: 42
      context:              # 鍙€夛細宸ヤ綔娴佸叡浜笂涓嬫枃
        workflow_name: "order-scheduling"
        previous_results: {}
    
    response:
      schema_version: "1.0"
      task_id: "T1"
      execution_id: "EXEC-20260622-001"
      status: "success | failure | pending | rejected"
      output:
        # 涓嶢gent鑳藉姏鍗′腑瀹氫箟鐨?output_schema 涓€鑷?
        result: { }
      error:                 # 浠呭湪status=failure鏃?
        code: "TIMEOUT"
        message: "澶勭悊瓒呮椂"
        retryable: true
        suggested_backoff_sec: 30
      metrics:               # 蹇呴』杩斿洖
        latency_ms: 1234
        token_usage: 1500    # 鑻ヤ娇鐢↙LM
        memory_mb: 256
        cost_estimate_usd: 0.003
```

### 3.4 澶氬眰瀹归敊閰嶇疆

鏅鸿兘浣撻泦缇ょ殑瀹归敊涓嶆槸绠€鍗曠殑閲嶈瘯锛岃€屾槸**鍥涘眰闃叉姢浣撶郴**锛?

```yaml
fault_tolerance:
  
  # 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
  # 绗竴灞傦細閲嶈瘯绛栫暐锛堢灛鏃舵晠闅滐級
  # 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
  retry_policy:
    max_attempts: 3
    backoff: "exponential"     # fixed | exponential | linear
    initial_interval_sec: 2
    max_interval_sec: 60
    max_delay_sec: 300
    non_retryable_errors:      # 杩欎簺閿欒涓嶉噸璇曪紙閲嶈瘯涔熸病鐢級
      - "INVALID_INPUT"
      - "AUTH_ERROR"
      - "PERMISSION_DENIED"
    retryable_on_timeout: true
  
  # 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
  # 绗簩灞傦細鐔旀柇鍣紙鎸佺画鏁呴殰锛?
  # 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
  circuit_breaker:
    enabled: true
    failure_threshold: 5       # 杩炵画澶辫触5娆″悗鐔旀柇
    success_threshold: 3       # 杩炵画鎴愬姛3娆″悗鎭㈠
    reset_timeout_sec: 30      # 鐔旀柇鍚?0绉掑皾璇曟仮澶?
    half_open_max_requests: 2  # 鍗婂紑鐘舵€佹渶澶氭斁琛?涓姹傛祴璇?
    
  # 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
  # 绗笁灞傦細闄嶇骇绛栫暐锛圓gent涓嶅彲鐢級
  # 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
  fallback:
    per_agent:
      - agent_id: "AGT-MFG-004"
        strategy: "degrade"     # 鍥涗釜閫夐」锛?
        # degrade: 鐢ㄧ畝鍖栭€昏緫锛堝瑙勫垯寮曟搸鏇夸唬LLM锛?
        # alternate_agent: 鍒囨崲鍒板鐢ˋgent
        # human_escalation: 鍗囩骇缁欎汉宸?
        # skip: 璺宠繃锛堥潪鍏抽敭浠诲姟锛?
        
        degrade_logic: |
          浣跨敤涓婂懆鍘嗗彶鎺掍骇缁撴灉鐨勫姞鏉冨钩鍧囦綔涓洪粯璁ゅ喅绛?
        
        alternate_agent_id: "AGT-MFG-004-B"  # 澶囬€堿gent锛坰trategy=alternate_agent鏃讹級
    
  # 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
  # 绗洓灞傦細浜哄伐鍗囩骇锛堟渶鍚庝繚闅滐級
  # 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
  human_escalation:
    enabled: true
    trigger_conditions:
      - "circuit_breaker_open AND strategy == human_escalation"
      - "all_retries_exhausted AND critical_task == true"
      - "output_confidence < 0.7"
    channels:
      - type: "webhook"
        url: "https://hooks.slack.com/xxx"
        template: "references/escalation-slack-template.json"
      - type: "email"
        to: "ops-team@company.com"
        template: "references/escalation-email-template.html"
      - type: "sms"
        to: "+86-13800138000"
        condition: "severity == 'critical' AND hour BETWEEN 8,22"
    timeout: 1800              # 浜虹被鍝嶅簲瓒呮椂锛堢锛?
    on_timeout: "auto_approve"  # 瓒呮椂鍚庤嚜鍔ㄦ壒鍑嗛檷绾ф柟妗?
    
  # 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
  # 鍏ㄥ眬瓒呮椂
  # 鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€
  timeout:
    per_task_default_sec: 300
    per_task_max_sec: 600
    per_workflow_default_sec: 3600
    per_workflow_max_sec: 7200
    # 鍙寜浠诲姟绫诲瀷瑕嗙洊
    overrides:
      - task_category: "鍒嗘瀽绫?
        timeout_sec: 600       # 璁＄畻瀵嗛泦鍨嬬粰鏇撮暱鏃堕棿
      - task_category: "鎰熺煡绫?
        timeout_sec: 120       # IOC閲囬泦涓嶈兘澶參
```

### 3.5 瀹瑰櫒鍖栭儴缃诧紙Docker Compose锛?

**鏈€灏忕紪鎺掔幆澧冧竴閿惎鍔?*锛?

```yaml
# docker-compose.yml 鈥?杞婚噺绾ф櫤鑳戒綋缂栨帓鐜
version: '3.8'

services:
  # ===== 缂栨帓寮曟搸 =====
  temporal:
    image: temporalio/auto-setup:1.22
    ports:
      - "7233:7233"
    environment:
      - DB=postgresql
      - DB_PORT=5432
      - POSTGRES_USER=temporal
      - POSTGRES_PWD=temporal
      - POSTGRES_SEEDS=postgresql
    depends_on:
      - postgresql
  
  # ===== 鏁版嵁鎬荤嚎 =====
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    command: redis-server --appendonly yes
  
  # ===== 娑堟伅闃熷垪锛堝彲閫夛紝浜嬩欢椹卞姩鍦烘櫙锛?=====
  kafka:
    image: bitnami/kafka:3.6
    ports:
      - "9092:9092"
    environment:
      - KAFKA_CFG_NODE_ID=0
      - KAFKA_CFG_PROCESS_ROLES=controller,broker
      - KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093
      - KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=0@kafka:9093
    profiles: ["full"]       # 浠協ull profile鍚敤
  
  # ===== 鐩戞帶 =====
  prometheus:
    image: prom/prometheus:latest
    ports:
      - "9090:9090"
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
  
  grafana:
    image: grafana/grafana:latest
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
    volumes:
      - ./grafana-dashboards:/etc/grafana/provisioning/dashboards
  
  # ===== Agent娉ㄥ唽涓績 =====
  consul:
    image: consul:1.15
    ports:
      - "8500:8500"
  
  # ===== 鏁版嵁搴?=====
  postgresql:
    image: postgres:15-alpine
    ports:
      - "5432:5432"
    environment:
      - POSTGRES_USER=temporal
      - POSTGRES_PASSWORD=temporal
```

**鍚姩鍛戒护**锛?
```bash
# 鏈€灏忕紪鎺掔幆澧?
docker-compose up -d temporal postgresql consul

# 瀹屾暣鐩戞帶鐜
docker-compose up -d

# 鏌ョ湅Agent娉ㄥ唽鐘舵€?
curl http://localhost:8500/v1/agent/services

# 鏌ョ湅Temporal UI
# 鎵撳紑 http://localhost:8233
```

### 3.6 DAG 瀹氫箟鏂囦欢鐢熸垚

鎸夐€夊畾鐨勫紩鎿庣敓鎴愬搴斾唬鐮併€備互涓嬩互 Temporal (TypeScript) 涓轰緥锛?

```typescript
// workflow.ts 鈥?鍒堕€犱笟璁㈠崟鎺掍骇宸ヤ綔娴?
import { proxyActivities, log, condition, defineSignal } from '@temporalio/workflow';
import type * as activities from './activities';

const { parseOrder, checkInventory, calculateCapacity, optimizeSchedule, generateWorkOrder } =
  proxyActivities<typeof activities>({
    startToCloseTimeout: '5 minutes',
    retry: {
      maximumAttempts: 3,
      backoffCoefficient: 2,
      initialInterval: '2 seconds',
      maximumInterval: '60 seconds',
      nonRetryableErrorTypes: ['InvalidInputError'],
    },
  });

export const orderSchedulingSignal = defineSignal<[{ approved: boolean; reason?: string }]>(
  'orderSchedulingApproval'
);

export async function orderSchedulingWorkflow(orderData: OrderInput): Promise<WorkflowOutput> {
  log.info('璁㈠崟鎺掍骇宸ヤ綔娴佸紑濮?, { orderId: orderData.id });

  // Step 1: 瑙ｆ瀽璁㈠崟
  const parsedOrder = await parseOrder(orderData);

  // Step 2-3: 骞惰鎵ц搴撳瓨妫€鏌ュ拰浜ц兘璁＄畻
  const [inventoryResult, capacityResult] = await Promise.all([
    checkInventory(parsedOrder.productModel),
    calculateCapacity(parsedOrder.productModel),
  ]);

  // 鏉′欢鍒嗘敮锛氱己鏂?鈫?瑙﹀彂閲囪喘
  if (inventoryResult.stockShortage) {
    log.warn('鐗╂枡涓嶈冻', { shortages: inventoryResult.shortages });
    // 鍙戦€佷汉宸ュ鎵归€氱煡
    // ...锛圱emporal Signal 鏈哄埗锛?
  }

  // Step 4: 鎺掍骇浼樺寲
  const schedule = await optimizeSchedule({
    productModel: parsedOrder.productModel,
    quantity: parsedOrder.quantity,
    availableCapacity: capacityResult,
    materialAvailable: inventoryResult.availableQuantity,
    deliveryDate: parsedOrder.deliveryDate,
  });

  // 鍒嗘敮锛氫骇鑳戒笉瓒?鈫?鍔犳€ヨ瘎浼?
  if (schedule.status === 'CAPACITY_INSUFFICIENT') {
    // 瑙﹀彂鍔犳€ユ祦绋?
  }

  // Step 5: 鐢熸垚宸ュ崟
  const workOrder = await generateWorkOrder({
    schedule: schedule,
    orderId: orderData.id,
  });

  log.info('璁㈠崟鎺掍骇宸ヤ綔娴佸畬鎴?, { orderId: orderData.id, workOrderId: workOrder.id });
  
  return {
    orderId: orderData.id,
    workOrderId: workOrder.id,
    assignedLine: schedule.lineId,
    estimatedStart: schedule.startTime,
    estimatedComplete: schedule.endTime,
  };
}
```

---

## 鍥涖€侀樁娈?锛氬弽棣堜紭鍖栭棴鐜?

### 4.1 鎵ц鏃ュ織鏍囧噯缁撴瀯

```json
{
  "execution_id": "EXEC-20260622-001",
  "parent_srm": "REQ-MFG-001",
  "workflow_name": "order-scheduling",
  "trigger": "scheduled | api_call | event | manual",
  
  "timeline": {
    "created": "2026-06-22T08:00:00Z",
    "started": "2026-06-22T08:00:01Z",
    "completed": "2026-06-22T08:02:35Z",
    "total_duration_ms": 154000
  },
  
  "status": "success | failure | timeout | cancelled",
  
  "task_results": [
    {
      "task_id": "T1",
      "agent_id": "AGT-MFG-001",
      "agent_name": "璁㈠崟瑙ｆ瀽Agent",
      "status": "success",
      "attempt": 1,
      "latency_ms": 1200,
      "input_hash": "sha256:abc123",
      "output_summary": "瑙ｆ瀽璁㈠崟鍙稯RD-2024-12345锛屼骇鍝佸瀷鍙稟-100",
      "error": null
    },
    {
      "task_id": "T4",
      "agent_id": "AGT-MFG-004",
      "agent_name": "鎺掍骇浼樺寲Agent",
      "status": "success",
      "attempt": 2,
      "retry_reason": "TIMEOUT",
      "latency_ms": 28500,
      "output_summary": "鎺ㄨ崘浜х嚎L3锛岄璁?:30寮€宸?
    }
  ],
  
  "aggregate_metrics": {
    "total_latency_ms": 154000,
    "critical_path_latency_ms": 152000,
    "parallel_efficiency": 0.87,
    "total_retries": 1,
    "total_token_usage": 45200,
    "estimated_cost_usd": 0.68,
    "agent_availability": {
      "AGT-MFG-001": { "uptime_percent": 99.9, "p95_latency_ms": 1500 },
      "AGT-MFG-004": { "uptime_percent": 98.5, "p95_latency_ms": 30000 }
    }
  },
  
  "sla_report": {
    "overall_status": "met",
    "violations": [
      {
        "task_id": "T4",
        "metric": "p95_latency_ms",
        "target": 5000,
        "actual": 28500,
        "severity": "warning"
      }
    ]
  },
  
  "anomalies": [],
  "human_interactions": []
}
```

### 4.2 鍙嶉闂幆锛氬洓姝ヨ嚜鍔ㄤ紭鍖?

```
鈹屸攢鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹?
鈹?姝ラ1: 鏀堕泦鎵ц鏃ュ織 鈫?瀛樺叆鍚戦噺鏁版嵁搴擄紙ChromaDB锛夆攤
鈹? 姣忔宸ヤ綔娴佹墽琛屽畬鎴愬悗鑷姩鍐欏叆 execution_log       鈹?
鈹斺攢鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹攢鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹?
                 鈻?
鈹屸攢鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹?
鈹?姝ラ2: 鍒嗘瀽鐡堕 鈫?鑷姩璇嗗埆浼樺寲鐐?                 鈹?
鈹? 路 鍝簺task閲嶈瘯娆℃暟澶氾紵鈫?Agent闇€浼樺寲/鏇挎崲         鈹?
鈹? 路 鍝簺Agent寤惰繜楂橈紵鈫?鍗囩骇纭欢/浼樺寲妯″瀷           鈹?
鈹? 路 骞惰搴︽槸鍚﹀锛熲啋 璋冩暣parallel_group            鈹?
鈹? 路 鍝簺鍒嗘敮浠庢湭瑙﹀彂锛熲啋 姝讳唬鐮佲啋鍒犻櫎               鈹?
鈹斺攢鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹攢鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹?
                 鈻?
鈹屸攢鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹?
鈹?姝ラ3: 鏇存柊妯″瀷 鈫?鑷姩鎴栦汉宸ョ‘璁?                 鈹?
鈹? 路 鏇存柊SRM: 淇constraints锛坱imeout澶煭鈫掕皟澶э級  鈹?
鈹? 路 鏇存柊Agent鍥捐氨: 鏇挎崲浣庡噯纭巼Agent               鈹?
鈹? 路 鏇存柊宸ヤ綔娴佹ā鏉? 璋冩暣骞惰搴︺€侀噸鎺掍緷璧栧叧绯?       鈹?
鈹? 路 鐗堟湰鍖栵細姣忔淇敼璁板綍鍒癵it                      鈹?
鈹斺攢鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹攢鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹?
                 鈻?
鈹屸攢鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹?
鈹?姝ラ4: 閲嶆柊閮ㄧ讲 鈫?涓嬩竴杞墽琛?                     鈹?
鈹? 路 鏂癉AG瀹氫箟鑷姩鎺ㄩ€佸埌缂栨帓寮曟搸                    鈹?
鈹? 路 A/B娴嬭瘯锛氭柊鐗堟湰10%娴侀噺锛岀ǔ瀹氬悗鍏ㄩ噺             鈹?
鈹? 路 鎸佺画鐩戞帶鏂扮増鏈〃鐜?                            鈹?
鈹斺攢鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹€鈹?
```

### 4.3 鐩戞帶闈㈡澘閰嶇疆

**Prometheus 鏍稿績鎸囨爣**锛圙o瀹炵幇绀轰緥锛夛細

```go
var (
    // Agent绾у埆
    agentExecuteTotal = prometheus.NewCounterVec(
        prometheus.CounterOpts{Name: "agent_execute_total", Help: "Total agent executions"},
        []string{"agent_id", "agent_name", "status"},
    )
    agentExecuteDuration = prometheus.NewHistogramVec(
        prometheus.HistogramOpts{Name: "agent_execute_duration_seconds", Buckets: []float64{0.1, 0.5, 1, 2, 5, 10, 30, 60}},
        []string{"agent_id"},
    )
    agentRetryTotal = prometheus.NewCounterVec(
        prometheus.CounterOpts{Name: "agent_retry_total", Help: "Total retry count per agent"},
        []string{"agent_id", "retry_attempt"},
    )
    
    // 宸ヤ綔娴佺骇鍒?
    workflowDuration = prometheus.NewHistogramVec(
        prometheus.HistogramOpts{Name: "workflow_duration_seconds", Buckets: []float64{10, 30, 60, 120, 300, 600, 1800, 3600}},
        []string{"workflow_name"},
    )
    workflowStatusTotal = prometheus.NewCounterVec(
        prometheus.CounterOpts{Name: "workflow_status_total"},
        []string{"workflow_name", "status"},
    )
    
    // 涓氬姟绾у埆
    slaViolationTotal = prometheus.NewCounterVec(
        prometheus.CounterOpts{Name: "sla_violation_total"},
        []string{"task_id", "metric", "severity"},
    )
    circuitBreakerState = prometheus.NewGaugeVec(
        prometheus.GaugeOpts{Name: "circuit_breaker_state", Help: "0=closed, 1=half-open, 2=open"},
        []string{"agent_id"},
    )
)
```

**Grafana 闈㈡澘鎺ㄨ崘甯冨眬**锛?

| 闈㈡澘 | 绫诲瀷 | 鏁版嵁婧?| 璇存槑 |
|------|------|--------|------|
| Agent鎵ц鎴愬姛鐜?| Stat + 棰滆壊缂栫爜 | `agent_execute_total` | 缁胯壊>99%, 榛勮壊95-99%, 绾㈣壊<95% |
| P95寤惰繜瓒嬪娍 | Time Series | `agent_execute_duration` | 鎸堿gent鍒嗙粍, 鍚槇鍊肩嚎 |
| 宸ヤ綔娴佹墽琛屾瑙?| Table | `workflow_duration` + `workflow_status_total` | 浠婃棩鎴愬姛/澶辫触/杩涜涓?|
| 閲嶈瘯鐑姏鍥?| Heatmap | `agent_retry_total` | X=Agent, Y=閲嶈瘯娆℃暟, 棰滆壊=璁℃暟 |
| 鐔旀柇鍣ㄧ姸鎬?| State Timeline | `circuit_breaker_state` | 鏄剧ず鍝簺Agent琚啍鏂?|
| SLA杈炬爣鐜?| Gauge | `sla_violation_total` | 鐜舰鍥? 涓績鏄剧ず杈炬爣鐜?|
| 鎴愭湰杩借釜 | Time Series | 鑷畾涔夋寚鏍?| 鎸夊伐浣滄祦/Agent鐨勪及绠楁垚鏈?|

---

## 浜斻€佽法琛屼笟鍦烘櫙瀹炴垬瀵规瘮

### 浼犵粺鏂瑰紡 vs 鏅鸿兘浣撻泦缇よ浆鍨?

**闅ч亾瀹夊叏鐩戞祴**锛?

| 缁村害 | 浼犵粺鏂瑰紡 | SRAO鏅鸿兘浣撻泦缇?| 鎻愬崌 |
|------|----------|----------------|------|
| 鏁版嵁閲囬泦 | 浜哄伐瀹氭湡鍏ㄧ珯浠祴閲?| 婵€鍏夐浄杈?澶氱洰鐩告満鑷姩铻嶅悎 | 鍛ㄦ湡锛?澶┾啋10鍒嗛挓 |
| 鏁版嵁澶勭悊 | 绂荤嚎瀵煎嚭鐐逛簯锛孋loudCompare鎵嬪姩閰嶅噯 | 鐐逛簯閰嶅噯Agent鑷姩瀵归綈 | 鑰楁椂锛?灏忔椂鈫?0绉?|
| 鍒嗘瀽璁＄畻 | Excel鎵嬪伐璁＄畻娌夐檷閲?| 浣嶇Щ鍦鸿绠桝gent杈撳嚭鐑姏鍥?| 绮惧害锛歮m鈫捨糾 |
| 缁撴灉浜や粯 | 绾歌川鎶ュ憡鎻愪氦 | 瀹炴椂鏁板瓧瀛敓澶у睆+棰勮鎺ㄩ€?| 寤惰繜锛氬懆鈫掔绾?|
| 寮傚父鍙戠幇 | 鑲夌溂瀵规瘮澶氭湡鏁版嵁 | AI鑷姩璇嗗埆缂撴參寰皬鍙樺舰 | 妫€鍑虹巼锛?0%鈫?7% |

**鍒堕€犱笟璐ㄩ噺妫€娴?*锛?

| 缁村害 | 浼犵粺鏂瑰紡 | SRAO鏅鸿兘浣撻泦缇?| 鎻愬崌 |
|------|----------|----------------|------|
| 妫€娴嬫柟寮?| 浜х嚎鏈浜哄伐鎶芥 | 澶氳瑙堿gent骞惰仈姣忓伐浣嶉儴缃?| 瑕嗙洊锛?%鈫?00% |
| 缂洪櫡璁板綍 | 绾歌川琛ㄦ牸 | 鍥惧儚+鎵规鑷姩鍏ュ簱+鏍瑰洜鍒嗘瀽 | 杩芥函锛氬ぉ鈫掔 |
| 闂鍒嗘瀽 | 姣忓懆璐ㄩ噺浼氳璁ㄨ | 寮傚父妯″紡鑷姩鑱氱被+鍐崇瓥鎺ㄨ崘 | 鍝嶅簲锛氬懆鈫掑疄鏃?|
| 璐ㄩ噺缁撴灉 | 婕忔鐜?% | 婕忔鐜?.2% | **25鍊嶆彁鍗?* |
| 浜х嚎褰卞搷 | 鍋滄満鏃堕棿楂?| 鍋滄満鏃堕棿鍑忓皯40% | **鎴愭湰鏄捐憲闄嶄綆** |

---

## 鍏€佷粠闆跺埌涓€瀹屾暣瀹炴柦璺嚎鍥?

### 绗?姝ワ細鏄庣‘鐩爣杈圭晫锛?澶╋級
- 纭畾瑕佽В鍐崇殑涓氬姟闂锛堜竴涓叿浣撳瓙鍦烘櫙锛屼笉瑕佷竴寮€濮嬪氨鍋氬叏琛屼笟锛?
- 瀹氫箟鎴愬姛鏍囧噯锛圞PI锛氬"棰勮鎻愬墠1灏忔椂"銆?妫€娴嬪噯纭巼>95%"锛?

### 绗?姝ワ細棰嗗煙鐭ヨ瘑寤烘ā锛?-3澶╋級
- 姊崇悊璇ラ鍩熺殑瀹炰綋-鍏崇郴-浜嬩欢
- 鏁寸悊鐜版湁宸ヤ綔娴侊紙琛ㄦ牸銆佹祦绋嬪浘銆丼OP锛?
- 杈撳嚭锛氶鍩熸蹇佃瘝鍏?+ 鍒濇宸ヤ綔娴佹ā鏉?
- 鈫?浣跨敤 `srao-domain-modeler` Skill

### 绗?姝ワ細鏅鸿兘浣撹兘鍔涚洏鐐癸紙1-2澶╋級
- 娓呯偣鐜版湁鍙敤宸ュ叿/API/妯″瀷
- 璇嗗埆鍝簺鑳藉姏闇€瑕佹柊寤篈gent
- 杈撳嚭锛欰gent娓呭崟 + 鑳藉姏缂哄彛鍒嗘瀽
- 鈫?浣跨敤 `srao-agent-graph` Skill

### 绗?姝ワ細缂栨帓骞冲彴鎼缓锛?澶╋級
- 閫夋嫨缂栨帓寮曟搸锛堥粯璁ゆ帹鑽怲emporal锛?
- 鍚姩Docker Compose鐜
- 寤虹珛Agent鎺ュ彛瑙勮寖
- 鎼缓鏁版嵁鎬荤嚎锛圧edis锛?

### 绗?姝ワ細鍘熷瀷宸ヤ綔娴佸疄鐜帮紙1-2鍛級
- 閫夊彇鏈€灏忓彲琛屽満鏅紙MVP锛?
- 涓茶仈2-3涓狝gent锛岄獙璇佺鍒扮
- 璁板綍鎵ц鏃ュ織銆佽€楁椂銆侀敊璇?
- 鈫?浣跨敤鏈琒kill

### 绗?姝ワ細杩唬涓庢墿灞曪紙鎸佺画锛?
- 鏍规嵁涓氬姟鍙嶉澧炲姞鏇村Agent
- 寤虹珛Agent鍥捐氨锛圢eo4j锛?
- 寮曞叆闇€姹傜粨鏋勫寲妯″潡

### 绗?姝ワ細娌荤悊涓庢寔缁紭鍖栵紙鎸佺画锛?
- 鐩戞帶Agent鍋ュ悍搴?
- 瀹氭湡鏇存柊鐗堟湰锛岀淮鎶ゅ閿欑瓥鐣?
- 绉疮鎵ц鏁版嵁锛岀敤浜庤嚜鍔ㄤ紭鍖栫紪鎺?

---

## 涓冦€佸叧閿妧鏈爤鍏ㄦ櫙

| 缁勪欢 | 鎺ㄨ崘鎶€鏈?| 鏇夸唬鏂规 | 閫夋嫨鐞嗙敱 |
|------|----------|----------|----------|
| **闇€姹傝В鏋?* | LangChain + GPT | Rasa, Semantic Kernel | 鐏垫椿銆佺敓鎬佸ソ |
| **浠诲姟鎷嗗垎** | 鑷畾涔夎鍒欏紩鎿?+ 鍚戦噺妫€绱?| Dialogflow CX | 鍙帶銆佸彲瀹氬埗 |
| **Agent寮€鍙?* | Python + FastAPI + Docker | Go/gRPC, TypeScript | 寮€鍙戝揩銆丄I鐢熸€佸ソ |
| **Agent娉ㄥ唽** | Consul | etcd, Kubernetes Service | 鎴愮啛銆佸仴搴锋鏌ュソ |
| **缂栨帓寮曟搸** | Temporal | Prefect, Airflow | 鍔熻兘鏈€鍏ㄣ€侀暱鏃惰繍琛?|
| **宸ヤ綔娴佸畾涔?* | YAML + 鑷畾涔塂SL | JSON, HCL | 浜虹被鍙銆佺増鏈帶鍒跺弸濂?|
| **鏁版嵁鎬荤嚎** | Redis Streams | Kafka, RabbitMQ | 杞婚噺銆佸鐢ㄩ€?|
| **鐩戞帶** | Prometheus + Grafana | ELK, Datadog | 寮€婧愭爣鍑嗐€佺敓鎬佹渶涓板瘜 |
| **鍓嶇** | React + WebSocket | Vue, Svelte | 瀹炴椂鎬уソ銆佺粍浠朵赴瀵?|
| **鍚戦噺DB** | ChromaDB | Pinecone, Weaviate | 杞婚噺鑷墭绠°€佸厤璐?|
| **鍥捐氨瀛樺偍** | Neo4j | ArangoDB, JanusGraph | 鏈€鎴愮啛鐨勫浘鏁版嵁搴?|

---

## 鍏€侀儴缃查獙璇丆hecklist锛?5椤癸級

- [ ] DAG瀹氫箟鏂囦欢璇硶妫€鏌ラ€氳繃锛坙int + 闈欐€佸垎鏋愶級
- [ ] 姣忎釜Agent鐨勫仴搴锋鏌ョ鐐瑰彲璁块棶锛坄GET /health` 鈫?200锛?
- [ ] 涓茶渚濊禆楠岃瘉锛歍2鍦═1瀹屾垚鍚庢墠瑙﹀彂 鉁?
- [ ] 骞惰浠诲姟楠岃瘉锛歍2鍜孴3鍚屾椂鎵ц 鉁?
- [ ] 閲嶈瘯绛栫暐楠岃瘉锛欰gent澶辫触鑷姩閲嶈瘯3娆?鉁?
- [ ] 鐔旀柇鍣ㄩ獙璇侊細杩炵画澶辫触5娆″悗璺宠繃璇gent 鉁?
- [ ] 瓒呮椂楠岃瘉锛氬崟浠诲姟瓒呰繃璁惧畾鏃堕棿鑷姩缁堟 鉁?
- [ ] 浜哄伐鍗囩骇楠岃瘉锛欰gent鏃犳硶澶勭悊鏃舵帹閫侀€氱煡 鉁?
- [ ] 鐩戞帶闈㈡澘楠岃瘉锛氭墍鏈堿gent鐨勫欢杩?鎴愬姛鐜?鐔旀柇鍙 鉁?
- [ ] 鎵ц鏃ュ織楠岃瘉锛氬畬鏁村啓鍏ャ€佸彲鍥炴函 鉁?
- [ ] 闄嶇骇绛栫暐楠岃瘉锛欰gent涓嶅彲鐢ㄦ椂瑙﹀彂闄嶇骇閫昏緫 鉁?
- [ ] 骞惰搴﹂獙璇侊細parallel_group浠诲姟瀹為檯骞惰鎵ц 鉁?
- [ ] 绔埌绔獙璇侊細涓€涓畬鏁村満鏅粠澶磋窇鍒板熬 鉁?
- [ ] 鎴愭湰杩借釜楠岃瘉锛氭瘡娆℃墽琛屼及绠楁垚鏈彲鏌?鉁?
- [ ] 鍙嶉闂幆楠岃瘉锛氭墽琛屾棩蹇椻啋鍒嗘瀽鈫掍紭鍖栤啋閲嶆柊閮ㄧ讲 鉁?

---

## 涔濄€佷笌鍏朵粬Skill鐨勫崗浣?

```
srao-domain-modeler 鈫?浜у嚭: SRM + 棰嗗煙妯″瀷 + 宸ヤ綔娴佹ā鏉?
                              鈹?
                              鈻?
srao-agent-graph       鈫?浜у嚭: task_dag + agent_cards + agent_graph
                              鈹?
                              鈻?
鏈琒kill (srao-workflow-orchestrator) 鈫?浜у嚭: 鍙墽琛孌AG浠ｇ爜
                                         + Docker Compose鐜
                                         + Prometheus+Grafana鐩戞帶
                                         + 鍙嶉闂幆鑷姩浼樺寲
```

### 鍏ㄩ摼璺Е鍙戝叧閿瘝

鐢ㄦ埛鍙渶璇达細
- "甯垜閮ㄧ讲XX琛屼笟鐨勬櫤鑳戒綋宸ヤ綔娴? 鈫?鑷姩涓茶仈涓変釜Skill
- "浠庨鍩熷缓妯″埌閮ㄧ讲涓€鏉￠緳" 鈫?绔埌绔墽琛屽叚涓樁娈?
- "鎶婃垜鐨凙gent鍥捐氨璺戣捣鏉? 鈫?鐩存帴浠庨樁娈?寮€濮?

---

## 鍗併€佸揩閫熷惎鍔ㄥ懡浠?

```bash
# 1. 鏈€灏忕紪鎺掔幆澧?
docker-compose up -d temporal postgresql consul

# 2. 瀹屾暣鐩戞帶鐜
docker-compose up -d

# 3. 娉ㄥ唽Agent鍒癈onsul
curl -X PUT http://localhost:8500/v1/agent/service/register -d @agent-register.json

# 4. 鍚姩Temporal Worker
cd workers && npm install && npm run worker

# 5. 瑙﹀彂宸ヤ綔娴?
temporal workflow start --task-queue order-scheduling --type orderSchedulingWorkflow \
  --input '{"orderId":"ORD-001","productModel":"A-100","quantity":1000}'

# 6. 鏌ョ湅鎵ц鐘舵€?
temporal workflow show -w <workflow-id>

# 7. 鏌ョ湅鐩戞帶
# Grafana: http://localhost:3000 (admin/admin)
# Prometheus: http://localhost:9090
# Consul: http://localhost:8500
```
