# 推理平台副本调度值班台

值班员用于在两台共享推理节点上安排模型副本，核心目标是**避免加载超时后重复占用显存**；
并支持在已可用的单副本部署上提交短文本推理请求，平台**自动合批**（至多 3 条 / 首条等待 200ms）
整批下发节点，降低逐条调用的开销，且故障场景下**不拆批重投、迟到回执不写入新轮**。
FastAPI + SQLite 实现，Docker Compose 一键启动 1 个调度平台 + 2 台可注入故障的本地节点。

## 一、快速启动

```bash
docker compose up --build
# 打开 http://localhost:8000
```

| 服务 | 端口 | 说明 |
| --- | --- | --- |
| platform | 8000 | 调度平台（网页 + REST API + 后台轮询/健康监测） |
| node-a | 9101 | 模拟节点 A，固定显存 8192MB |
| node-b | 9102 | 模拟节点 B，固定显存 8192MB |

不使用 Docker 时的本地运行方式：

```bash
pip install -r requirements.txt
# 终端 1/2：两个节点
NODE_ID=node-a NODE_NAME=模拟节点A CAPACITY_MB=8192 \
  uvicorn node.simulator:app --port 9101
NODE_ID=node-b NODE_NAME=模拟节点B CAPACITY_MB=8192 \
  uvicorn node.simulator:app --port 9102
# 终端 3：调度平台
PLATFORM_NODES="node-a|模拟节点A|http://127.0.0.1:9101|8192,node-b|模拟节点B|http://127.0.0.1:9102|8192" \
  uvicorn app.main:app --port 8000
```

## 二、网页操作

- **提交部署**：模型名、每副本显存（MB）、副本数（1～2）。
- **提交推理请求（自动合批）**：选择“可用”的单副本部署，填请求号与短文本；
  “连发 3 条”演示满批立即下发，单条提交演示 200ms 凑批窗口。
- **推理合批与结果**：实际批次（轮次、节点、状态、批内请求）与每条请求的模拟结果；
  待核批次可手动“查询原批”。
- **节点账本**：实时显示每台节点容量、已预留、剩余显存与在线状态。
- **部署列表**：每个部署的副本分配（节点、epoch、状态、错误原因）与事件流水。
- **故障注入**：对每台节点一键切换 正常 / 延迟 / 失败(503) / 失联(挂起超时)，
  以及“下次加载失败”“迟到回执”“重置节点”。
- **手动对账 / 重试**：部署行内按钮；页面 2 秒自动刷新。

## 三、核心调度规则（与需求逐条对应）

1. **副本分散**：同一部署的 2 个副本必须落在不同节点（按剩余显存降序选节点）。
2. **全部确认才可用**：所有副本都被节点确认 `loaded` 后部署才标记 `AVAILABLE`。
3. **并发不超额预留**：预留检查与插入在同一个 SQLite 写事务内串行执行，
   显存账本只统计仍占用的副本（`PENDING/LOADING/LOADED/TIMEOUT/UNVERIFIED/RELEASING`），
   不足时整个部署返回 409，不会产生半分配。
4. **超时/失联保留占用待核**：
   - 传输层错误（超时、连接失败）→ 节点真实状态不可知 → 副本转 `TIMEOUT`，**显存继续占用**；
   - 显式 HTTP 失败（4xx/5xx）→ 节点确认未占用 → 副本转 `LOAD_FAILED` 并释放预留。
5. **恢复对账**：节点恢复后由健康监测或业务调用成功联动触发自动对账，也可手动对账：
   - 节点无残留 → 副本确认 `RELEASED`，显存才可重新分配；
   - 节点其实已加载 → 同一 epoch 内补确认（合法，不算复活）；
   - 节点仍持有旧加载 → 重新下发卸载，直到确认释放；
   - 对账时节点仍失联 → 保持待核并在原因/备注中说明，绝不擅自释放。
6. **迟到回执不复活旧副本**：副本带 `epoch` 标识；重试开启新 epoch，
   节点侧与调度器侧都只接受当前 epoch 的状态，旧 epoch 回执被丢弃。
7. **失败可重试**：仅 `FAILED` 部署允许重试，重试前校验旧副本全部不再占显存，
   旧副本记录保留为终态用于审计。

## 四、推理合批规则（短文本逐条调用 → 整批一次下发）

8. **受理范围**：仅“可用”的**单副本**部署接受推理请求；请求号在部署内唯一，
   重复提交幂等返回原记录——**同一请求绝不进入两个批次**（并发提交由单写事务串行保证）。
9. **凑批窗口**：同一部署轮次（epoch）的待发请求至多凑 **3 条**；
   满批在同一事务内立即成批下发；未满时首条等待 **200ms** 后整批下发。
   节点一次处理整批并返回各请求的模拟结果，网页展示实际批次与逐条结果。
10. **下发前失去可用性**：凑批窗口关闭时复核部署状态，副本已不可用或轮次已结束的
    待发请求直接驳回（终态），**不得改投新轮**；恢复后也不会被重新凑批。
11. **已下发批次超时/失联**：整批保留待核（请求随批），**绝不拆批重投**；
    恢复后按原批号向节点查询结果：
    - 节点已算完 → 整批结果写回原批请求；
    - 节点仍在处理 → 恢复等待；
    - 节点确认从未收到（且已过宽限）→ 判定未执行、请求失败，可换新请求号重提。
12. **迟到回执不写入新轮**：批次与请求都绑定提交时的 epoch；
    回执轮次不符直接丢弃，迟到结果只写回原轮次请求，绝不影响新轮请求。

## 五、状态机

部署：`PENDING（调度中）→ AVAILABLE（可用）/ FAILED（失败可重试）/ UNVERIFIED（待核）`

副本：
`PENDING → LOADING → LOADED`；异常分支 `TIMEOUT/UNVERIFIED → RELEASING → RELEASED`，
显式失败为 `LOAD_FAILED`（终态，不占显存）。

推理请求：`PENDING（待凑批）→ BATCHED（批处理中）→ DONE（已完成）`；
异常分支 `TIMEOUT（随批待核）→ DONE/FAILED`；未下发即轮次失效为 `REJECTED`（终态），
节点明确未执行为 `FAILED`（终态）。

批次：`SENDING（下发中）→ DISPATCHED（已下发）→ DONE`；
异常分支 `TIMEOUT（超时待核，保留原批）→ DONE/FAILED`。

## 六、REST API 摘要

| 方法 | 路径 | 用途 |
| --- | --- | --- |
| POST | `/api/deployments` | 提交部署 `{model_name, mem_per_replica_mb, replica_count}` |
| GET | `/api/deployments` / `/api/deployments/{id}` | 部署列表/详情（含副本与事件） |
| POST | `/api/deployments/{id}/retry` | 失败部署重试（新 epoch） |
| POST | `/api/deployments/{id}/reconcile` | 手动对账，返回逐条处置说明 |
| POST | `/api/infer/requests` | 提交推理请求 `{deployment_id, request_no, text}`（请求号幂等） |
| GET | `/api/infer/requests` / `/api/infer/batches` | 推理请求/批次列表（可按部署过滤） |
| GET | `/api/infer/batches/{id}` | 批次详情（含批内各请求结果） |
| POST | `/api/infer/batches/{id}/reconcile` | 手动按原批号查询批次结果（不重投） |
| GET | `/api/nodes` | 节点显存账本与在线状态 |
| POST | `/api/nodes/{id}/control` | 故障注入（mode/延迟/load_fail 等） |
| POST | `/api/nodes/{id}/reset` | 重置节点模拟器 |
| GET | `/api/events` | 最近事件流水 |

## 七、自测

```bash
pip install -r requirements.txt pytest pytest-asyncio
pytest -q
```

`tests/test_platform.py` 基于 ASGI 传输同时拉起调度器与两台独立节点，覆盖：
完整部署流程、并发超额预留、显式失败释放与重试、失联超时保留占用、
恢复对账撤销旧加载并确认释放、迟到回执不复活旧 epoch、对账失败保持待核并说明原因；
推理合批：满批立即下发与节点单次整批处理、未满 200ms 凑批、并发同请求号幂等且不入两批、
下发失联保留待核并查询原批恢复（不重投）、节点确认未收到判失败、
下发前副本失效驳回且不改投新轮、迟到回执不写新轮。

## 八、目录结构

```
app/            调度平台
  config.py     环境配置（节点、超时、轮询间隔、合批参数）
  db.py         SQLite 访问层（WAL + 写锁事务）
  models.py     状态常量与中文映射（部署/副本/推理请求/批次）
  node_client.py 节点 HTTP 客户端（区分传输失败与显式 HTTP 失败）
  service.py    调度核心：预留/加载/超时/对账/重试/健康监测 + 推理合批
  main.py       FastAPI 入口、后台任务、故障注入代理
  static/index.html 值班网页（部署 + 推理合批演示）
node/
  simulator.py  节点模拟器（应用工厂，支持延迟/失败/失联/迟到回执/整批推理）
tests/          端到端自测
docker-compose.yml / Dockerfile
```

## 九、关键环境变量

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `PLATFORM_NODES` | 两台 8192MB 节点 | `id|名称|url|容量MB`，逗号分隔 |
| `PLATFORM_DB` | `data/platform.db` | SQLite 路径 |
| `NODE_CALL_TIMEOUT` | `1.5` 秒 | 单次节点 HTTP 调用超时 |
| `LOAD_TIMEOUT` | `30` 秒 | 等待加载确认上限，超时转待核 |
| `POLL_INTERVAL` / `HEALTH_INTERVAL` | `1` / `10` 秒 | 轮询与健康探测间隔 |
| `INFER_BATCH_MAX` | `3` 条 | 同一部署轮次单批最大请求数 |
| `INFER_BATCH_WAIT_MS` | `200` 毫秒 | 未满批时首条请求的凑批等待 |
| `INFER_TIMEOUT` | `15` 秒 | 等待整批结果上限，超时批次保留待核 |
