"""端到端自测：基于 ASGI transport 同时跑调度器与两台节点模拟器。

运行：
  PYTHONPATH=/tmp/pylibs PLATFORM_DB=/tmp/platform-test/test.db \
  python3 -m pytest tests/ -v
"""
from __future__ import annotations

import asyncio
import os
import sys
import uuid

import httpx
import pytest

# 必须在导入 app 之前配置环境
TEST_DIR = f"/tmp/platform-test-{uuid.uuid4().hex[:8]}"
os.environ["PLATFORM_DB"] = os.path.join(TEST_DIR, "test.db")
os.environ["PLATFORM_NODES"] = (
    "node-a|模拟节点A|http://testnode-a:9001|8192,"
    "node-b|模拟节点B|http://testnode-b:9002|8192"
)
os.environ["NODE_CALL_TIMEOUT"] = "1.0"
os.environ["LOAD_TIMEOUT"] = "4"
os.environ["POLL_INTERVAL"] = "0.2"
os.environ["HEALTH_INTERVAL"] = "3600"  # 测试中手动驱动健康监测
os.environ["INFER_BATCH_MAX"] = "3"
os.environ["INFER_BATCH_WAIT_MS"] = "200"
os.environ["INFER_TIMEOUT"] = "4"

from app.main import app as platform_app          # noqa: E402
from app import config, models, service           # noqa: E402
from app import db as db_mod                      # noqa: E402
from node import simulator                         # noqa: E402


class RoutingTransport(httpx.AsyncBaseTransport):
    """按 host 把请求路由到对应 ASGI 应用，无需真实端口。"""

    def __init__(self, sim_a, sim_b):
        self._a = httpx.ASGITransport(app=sim_a)
        self._b = httpx.ASGITransport(app=sim_b)
        self._p = httpx.ASGITransport(app=platform_app)

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        host = request.url.host
        if host == "testnode-a":
            return await self._a.handle_async_request(request)
        if host == "testnode-b":
            return await self._b.handle_async_request(request)
        return await self._p.handle_async_request(request)


@pytest.fixture
async def client():
    # 每台模拟器独立实例、独立内存态；每用例全新 SQLite 库
    os.makedirs(TEST_DIR, exist_ok=True)
    if os.path.exists(os.environ["PLATFORM_DB"]):
        os.remove(os.environ["PLATFORM_DB"])
    from app import db as db_mod
    db_mod._conn = None
    # ASGI 测试传输下用 0 表示直接抛连接异常；真实 uvicorn 下用长挂起复现超时
    sim_a = simulator.create_app("node-a", "模拟节点A", 8192, 0.3, 0.15,
                                 unreachable_sleep=0)
    sim_b = simulator.create_app("node-b", "模拟节点B", 8192, 0.3, 0.15,
                                 unreachable_sleep=0)
    transport = RoutingTransport(sim_a, sim_b)
    async with httpx.AsyncClient(base_url="http://platform", transport=transport,
                                 timeout=10) as c:
        # 手动初始化（不走 lifespan），并替换服务里的全局 httpx client
        service.init_db()
        await service.node_client.close()
        service.node_client._client = httpx.AsyncClient(
            base_url="http://platform", transport=transport, timeout=1.0
        )
        # 启动与生产相同的后台轮询（含健康监测，驱动失联检测与恢复自动对账）
        async def poll_loop():
            while True:
                await service.poll_loading()
                await service.poll_releasing()
                await service.poll_infer_batches()
                service.sweep_infer_pending()
                await asyncio.sleep(0.2)

        async def health_loop():
            while True:
                await service.health_monitor_once()
                await asyncio.sleep(0.5)

        poller = asyncio.create_task(poll_loop())
        health = asyncio.create_task(health_loop())
        yield c
        poller.cancel()
        health.cancel()
    await service.node_client.close()


async def wait_for(client, dep_id, statuses, timeout=12.0):
    deadline = asyncio.get_event_loop().time() + timeout
    last = None
    while asyncio.get_event_loop().time() < deadline:
        d = await client.get(f"/api/deployments/{dep_id}")
        last = d.json()
        if last["status"] in statuses:
            return last
        await asyncio.sleep(0.25)
    raise AssertionError(f"部署 {dep_id} 未进入 {statuses}，最后状态={last['status']} / {last['reason']}")


def node_free(book, nid):
    return next(n["free_mb"] for n in book if n["node_id"] == nid)


@pytest.mark.asyncio
async def test_01_full_deploy_flow_and_available(client):
    # 两台节点初始各 8192
    book = (await client.get("/api/nodes")).json()
    assert {n["node_id"] for n in book} == {"node-a", "node-b"}
    assert all(n["free_mb"] == 8192 for n in book)

    r = await client.post("/api/deployments", json={
        "model_name": "llama3-8b", "mem_per_replica_mb": 2048, "replica_count": 2})
    assert r.status_code == 201, r.text
    dep_id = r.json()["id"]

    dep = await wait_for(client, dep_id, {"AVAILABLE"})
    # 两个副本必须分处不同节点
    nodes = {rep["node_id"] for rep in dep["replicas"]}
    assert nodes == {"node-a", "node-b"}
    assert all(rep["status"] == "LOADED" for rep in dep["replicas"])

    book = (await client.get("/api/nodes")).json()
    assert all(n["used_mb"] == 2048 and n["free_mb"] == 6144 for n in book)


@pytest.mark.asyncio
async def test_02_concurrent_reservation_never_overcommits(client):
    # 每台 8192：每副本 3000，双副本部署每台占 3000；并发 3 个 -> 只有 2 个能成功
    async def submit(i):
        return await client.post("/api/deployments", json={
            "model_name": f"m{i}", "mem_per_replica_mb": 3000, "replica_count": 2})

    results = await asyncio.gather(*(submit(i) for i in range(3)))
    codes = [r.status_code for r in results]
    assert codes.count(201) == 2, codes
    assert codes.count(409) == 1, codes

    book = (await client.get("/api/nodes")).json()
    for n in book:
        assert n["used_mb"] <= n["capacity_mb"]
        assert n["used_mb"] == 6000, n  # 2 个部署 × 3000
        assert n["free_mb"] == 2192

    # 显存不足时再提交应明确拒绝
    r = await client.post("/api/deployments", json={
        "model_name": "big", "mem_per_replica_mb": 4000, "replica_count": 1})
    assert r.status_code == 409
    assert "显存不足" in r.json()["detail"]


@pytest.mark.asyncio
async def test_03_load_fail_releases_reservation_and_retry(client):
    # 让节点 A 加载失败；单副本部署落到剩余显存最多的节点（A/B 相同，排序后取 A）
    await client.post("/api/nodes/node-a/control", json={"load_fail": True})
    r = await client.post("/api/deployments", json={
        "model_name": "fail-model", "mem_per_replica_mb": 1024, "replica_count": 1})
    dep_id = r.json()["id"]
    dep = await wait_for(client, dep_id, {"FAILED"})
    rep = dep["replicas"][0]
    assert rep["status"] == "LOAD_FAILED"
    # 失败不占显存
    book = (await client.get("/api/nodes")).json()
    assert all(n["used_mb"] == 0 for n in book)

    # 清除故障后重试，应成功
    await client.post("/api/nodes/node-a/control",
                      json={"load_fail": False, "mode": "normal"})
    r2 = await client.post(f"/api/deployments/{dep_id}/retry")
    assert r2.status_code == 200
    dep2 = await wait_for(client, dep_id, {"AVAILABLE"})
    assert dep2["epoch"] == 2 and dep2["attempts"] == 2
    # 旧 epoch 副本保持终态，不复活
    old = [x for x in dep2["replicas"] if x["epoch"] == 1]
    new = [x for x in dep2["replicas"] if x["epoch"] == 2]
    assert old[0]["status"] == "LOAD_FAILED" and new[0]["status"] == "LOADED"


@pytest.mark.asyncio
async def test_04_unreachable_timeout_keeps_hold_then_reconcile_releases(client):
    await client.post("/api/nodes/node-b/control", json={"mode": "unreachable"})
    r = await client.post("/api/deployments", json={
        "model_name": "ghost", "mem_per_replica_mb": 1500, "replica_count": 2})
    dep_id = r.json()["id"]

    # A 正常加载，B 失联 -> 部署 UNVERIFIED，B 上显存保留占用
    dep = await wait_for(client, dep_id, {"UNVERIFIED"}, timeout=12)
    await service.health_monitor_once()  # 驱动一次探测周期，确认失联被健康监测捕获
    by_node = {x["node_id"]: x for x in dep["replicas"]}
    assert by_node["node-b"]["status"] == "TIMEOUT"
    # A 可能已加载或仍在加载，但绝不能被标记为失败/释放
    assert by_node["node-a"]["status"] in {"LOADING", "LOADED"}

    book = (await client.get("/api/nodes")).json()
    b = next(n for n in book if n["node_id"] == "node-b")
    assert b["used_mb"] == 1500  # 待核期间仍占用，不可被新部署用掉

    # 该显存不能被重新分配：节点 B 剩余不足时提交双副本必被拒
    r2 = await client.post("/api/deployments", json={
        "model_name": "blocked", "mem_per_replica_mb": 7000, "replica_count": 2})
    assert r2.status_code == 409

    # 节点恢复（失联期间请求从未到达模拟器，B 上实际没有该副本）
    await client.post("/api/nodes/node-b/control", json={"mode": "normal"})
    await service.health_monitor_once()  # 触发恢复自动对账
    dep = await wait_for(client, dep_id, {"AVAILABLE", "FAILED"}, timeout=12)
    # B 上实际没有副本 -> 对账释放 -> 该副本 LOAD_FAILED/RELEASED，部署 FAILED
    assert dep["status"] == "FAILED"
    by_node = {x["node_id"]: x for x in dep["replicas"]}
    assert by_node["node-b"]["status"] in {"RELEASED", "LOAD_FAILED"}
    book = (await client.get("/api/nodes")).json()
    b = next(n for n in book if n["node_id"] == "node-b")
    assert b["used_mb"] == 0  # 确认释放后显存可重新分配


@pytest.mark.asyncio
async def test_05_late_ack_must_not_revive_old_replica(client):
    # 场景：节点 A 在加载请求在途时失联，调度器保留占用转待核；
    # 之后节点其实完成了加载（迟到回执/延迟状态变化），恢复后补确认；
    # 再撤销旧加载、重试 epoch=2，旧 epoch 的迟到状态绝不能复活旧副本。

    # 1) 失联期间发起部署 -> TIMEOUT 待核，占用保留
    await client.post("/api/nodes/node-a/control", json={"mode": "unreachable"})
    await service.health_monitor_once()  # 确认健康监测捕获失联（online=False）
    r = await client.post("/api/deployments", json={
        "model_name": "late", "mem_per_replica_mb": 512, "replica_count": 1})
    dep_id = r.json()["id"]
    dep = await wait_for(client, dep_id, {"UNVERIFIED"}, timeout=10)
    rid = dep["replicas"][0]["id"]
    assert dep["replicas"][0]["status"] == "TIMEOUT"
    book = (await client.get("/api/nodes")).json()
    assert node_free(book, "node-a") == 8192 - 512

    # 2) 节点恢复，但它在“失联期间”其实把副本加载好了（模拟迟到回执落地）。
    #    先写入迟到状态再恢复，避免健康循环的自动对账抢先判定为无残留。
    sim_state = client._transport._a.app.state  # type: ignore[attr-defined]
    sim_state.replicas[rid] = {
        "model": "late", "mem_mb": 512, "epoch": 1, "status": "loaded",
    }
    await client.post("/api/nodes/node-a/control", json={"mode": "normal"})
    await service.health_monitor_once()  # 探测恢复边沿，触发自动对账
    # 对账补确认 -> AVAILABLE（同一 epoch 的合法补确认，不是复活）
    (await client.post(f"/api/deployments/{dep_id}/reconcile")).json()
    dep = await wait_for(client, dep_id, {"AVAILABLE"}, timeout=8)
    assert dep["replicas"][0]["status"] == "LOADED"

    # 3) 再次失联并人工撤销旧加载，恢复后对账确认释放
    await client.post("/api/nodes/node-a/control", json={"mode": "unreachable"})
    from app import models
    import app.db as db_mod
    with db_mod.tx() as conn:
        service._set_replica(conn, rid, models.REP_UNVERIFIED, error="演练：失联待核")
        service._evaluate(conn, dep_id)
    await service.request_release(rid, "演练：撤销旧加载")
    await client.post("/api/nodes/node-a/control", json={"mode": "normal"})
    deadline = asyncio.get_event_loop().time() + 15
    dep = None
    while asyncio.get_event_loop().time() < deadline:
        await client.post(f"/api/deployments/{dep_id}/reconcile")
        dep = (await client.get(f"/api/deployments/{dep_id}")).json()
        if all(x["status"] in {"RELEASED", "LOAD_FAILED"} for x in dep["replicas"]):
            break
        await asyncio.sleep(0.25)
    assert dep is not None and all(
        x["status"] in {"RELEASED", "LOAD_FAILED"} for x in dep["replicas"])

    # 4) 模拟旧 epoch 的“迟到回执”：节点侧再次出现 epoch=1 loaded
    sim_state.replicas[rid] = {
        "model": "late", "mem_mb": 512, "epoch": 1, "status": "loaded",
    }
    # 重试 -> epoch=2；调度器只认当前 epoch，旧副本终态不变
    r3 = await client.post(f"/api/deployments/{dep_id}/retry")
    assert r3.status_code == 200
    dep = await wait_for(client, dep_id, {"AVAILABLE"}, timeout=10)
    assert dep["epoch"] == 2
    old = [x for x in dep["replicas"] if x["epoch"] == 1]
    new = [x for x in dep["replicas"] if x["epoch"] == 2]
    assert all(x["status"] in {"RELEASED", "LOAD_FAILED"} for x in old)  # 不复活
    assert new[0]["status"] == "LOADED"
    # 新副本使用不同 rid，节点侧旧记录与新记录互不干扰
    assert new[0]["id"] != rid


@pytest.mark.asyncio
async def test_06_reconcile_failure_stays_unverified_with_reason(client):
    # B 失联 -> 待核；保持失联时手动对账，必须仍是待核并说明原因
    await client.post("/api/nodes/node-b/control", json={"mode": "unreachable"})
    r = await client.post("/api/deployments", json={
        "model_name": "nv", "mem_per_replica_mb": 900, "replica_count": 2})
    dep_id = r.json()["id"]
    dep = await wait_for(client, dep_id, {"UNVERIFIED"}, timeout=12)
    rec = (await client.post(f"/api/deployments/{dep_id}/reconcile")).json()
    assert rec["status"] == "UNVERIFIED"
    assert any("失联" in note or "不可达" in note for note in rec["reconcile_notes"])
    # 显存仍保留
    book = (await client.get("/api/nodes")).json()
    b = next(n for n in book if n["node_id"] == "node-b")
    assert b["used_mb"] == 900


@pytest.mark.asyncio
async def test_07_recovery_revokes_stale_load_then_confirms_release(client):
    # 正常部署成功后，节点 B 失联，调度器侧强制转待核并撤销；
    # 恢复后对账必须撤销节点上的旧加载，确认释放后才可重新分配
    r = await client.post("/api/deployments", json={
        "model_name": "svc", "mem_per_replica_mb": 2500, "replica_count": 2})
    dep_id = r.json()["id"]
    dep = await wait_for(client, dep_id, {"AVAILABLE"}, timeout=10)
    b_rep = next(x for x in dep["replicas"] if x["node_id"] == "node-b")
    rid = b_rep["id"]

    await client.post("/api/nodes/node-b/control", json={"mode": "unreachable"})
    # 直接通过服务层把 B 副本置为待核并请求释放（模拟失联期间人工处置）
    from app import models
    import app.db as db_mod
    with db_mod.tx() as conn:
        service._set_replica(conn, rid, models.REP_UNVERIFIED, error="演练：失联待核")
        service._evaluate(conn, dep_id)
    await service.request_release(rid, "演练：撤销旧加载")

    # 失联中对账不能释放
    rec = (await client.post(f"/api/deployments/{dep_id}/reconcile")).json()
    assert rec["status"] == "UNVERIFIED"
    book = (await client.get("/api/nodes")).json()
    assert node_free(book, "node-b") == 8192 - 2500

    # 恢复：节点上旧加载仍在（loaded），自动/手动对账应撤销并最终确认释放。
    # 自动对账与手动对账可能由不同轮次触发，终态为 RELEASED（确认释放）或
    # LOAD_FAILED（对账确认节点无残留）——两者都不再占用显存，语义等价。
    await client.post("/api/nodes/node-b/control", json={"mode": "normal"})
    await service.health_monitor_once()
    deadline = asyncio.get_event_loop().time() + 15
    while asyncio.get_event_loop().time() < deadline:
        dep = (await client.get(f"/api/deployments/{dep_id}")).json()
        b = next(x for x in dep["replicas"] if x["node_id"] == "node-b")
        if b["status"] in {"RELEASED", "LOAD_FAILED"}:
            break
        await client.post(f"/api/deployments/{dep_id}/reconcile")
        await asyncio.sleep(0.25)
    else:
        raise AssertionError("恢复后副本未在时限内确认释放")
    dep = (await client.get(f"/api/deployments/{dep_id}")).json()
    b = next(x for x in dep["replicas"] if x["node_id"] == "node-b")
    assert b["status"] in {"RELEASED", "LOAD_FAILED"}, dep
    book = (await client.get("/api/nodes")).json()
    assert node_free(book, "node-b") == 8192  # 关键：确认释放后显存全额可用

    # 节点侧旧记录应为 unloaded，且重新部署能拿到显存
    r2 = await client.post("/api/deployments", json={
        "model_name": "new-tenant", "mem_per_replica_mb": 7000, "replica_count": 1})
    assert r2.status_code == 201, r2.text


# ================================================================ 推理合批
async def make_available_single_dep(client, model="infer-svc", mem=1024):
    """部署一个单副本模型并等待可用，返回 (dep_id, 当前轮副本)。"""
    r = await client.post("/api/deployments", json={
        "model_name": model, "mem_per_replica_mb": mem, "replica_count": 1})
    assert r.status_code == 201, r.text
    dep_id = r.json()["id"]
    dep = await wait_for(client, dep_id, {"AVAILABLE"})
    cur = [x for x in dep["replicas"] if x["epoch"] == dep["epoch"]][0]
    return dep_id, cur


async def submit_infer(client, dep_id, no, text):
    return await client.post("/api/infer/requests", json={
        "deployment_id": dep_id, "request_no": no, "text": text})


async def get_batches(client, dep_id):
    return (await client.get(f"/api/infer/batches?deployment_id={dep_id}")).json()


async def get_ireq(client, dep_id, request_no):
    reqs = (await client.get(f"/api/infer/requests?deployment_id={dep_id}")).json()
    match = [r for r in reqs if r["request_no"] == request_no]
    assert match, f"请求 {request_no} 不存在"
    return match[0]


async def wait_ireq(client, dep_id, request_no, statuses, timeout=10.0):
    deadline = asyncio.get_event_loop().time() + timeout
    last = None
    while asyncio.get_event_loop().time() < deadline:
        last = await get_ireq(client, dep_id, request_no)
        if last["status"] in statuses:
            return last
        await asyncio.sleep(0.1)
    raise AssertionError(f"请求 {request_no} 未进入 {statuses}，最后状态={last}")


async def wait_batch(client, batch_id, statuses, timeout=10.0):
    deadline = asyncio.get_event_loop().time() + timeout
    last = None
    while asyncio.get_event_loop().time() < deadline:
        last = (await client.get(f"/api/infer/batches/{batch_id}")).json()
        if last["status"] in statuses:
            return last
        await asyncio.sleep(0.1)
    raise AssertionError(f"批次 {batch_id} 未进入 {statuses}，最后状态={last}")


def sim_state(client, nid):
    return (client._transport._a if nid == "node-a" else client._transport._b).app.state


@pytest.mark.asyncio
async def test_08_full_batch_dispatches_immediately(client):
    dep_id, rep = await make_available_single_dep(client)
    # 并发 3 条：第 3 条落库即满批，同一事务内成批，不等 200ms 窗口
    rs = await asyncio.gather(*(submit_infer(client, dep_id, f"r{i}", f"文本{i}")
                                for i in range(3)))
    assert all(r.status_code == 201 for r in rs), [r.text for r in rs]
    batches = await get_batches(client, dep_id)
    assert len(batches) == 1 and batches[0]["request_count"] == 3, batches
    batch_id = batches[0]["id"]
    assert batches[0]["epoch"] == 1 and batches[0]["node_id"] == rep["node_id"]

    # 节点一次处理整批：节点侧恰好 1 个批次记录，且只被下发过 1 次
    sim = sim_state(client, rep["node_id"])
    deadline = asyncio.get_event_loop().time() + 5
    while batch_id not in sim.batches and asyncio.get_event_loop().time() < deadline:
        await asyncio.sleep(0.05)
    assert batch_id in sim.batches
    assert sim.batches[batch_id]["posts"] == 1
    assert len(sim.batches[batch_id]["requests"]) == 3

    # 整批结果回收，各请求结果一一对应（模拟输出为文本逆序）
    b = await wait_batch(client, batch_id, {"DONE"})
    results = {r["request_no"]: r["result"] for r in b["requests"]}
    assert set(results) == {"r0", "r1", "r2"}
    for i in range(3):
        assert results[f"r{i}"].startswith("模拟输出[")
        assert f"文本{i}"[::-1] in results[f"r{i}"]
    assert all(r["status"] == "DONE" for r in b["requests"])


@pytest.mark.asyncio
async def test_09_partial_batch_flushes_after_200ms(client):
    dep_id, _ = await make_available_single_dep(client)
    r = await submit_infer(client, dep_id, "solo", "单独一条")
    assert r.status_code == 201 and r.json()["status"] == "PENDING"
    # 未满批：窗口未关闭前不得下发
    assert await get_batches(client, dep_id) == []

    t0 = asyncio.get_event_loop().time()
    batch = None
    while asyncio.get_event_loop().time() - t0 < 3:
        batches = await get_batches(client, dep_id)
        if batches:
            batch = batches[0]
            break
        await asyncio.sleep(0.05)
    assert batch is not None, "200ms 窗口后仍未凑批下发"
    elapsed = asyncio.get_event_loop().time() - t0
    assert elapsed >= 0.15, f"未满批应等待首条 200ms，实际 {elapsed:.3f}s"
    assert batch["request_count"] == 1

    b = await wait_batch(client, batch["id"], {"DONE"})
    assert b["requests"][0]["status"] == "DONE"
    assert "单独一条"[::-1] in b["requests"][0]["result"]


@pytest.mark.asyncio
async def test_10_concurrent_submit_never_double_batches(client):
    dep_id, _ = await make_available_single_dep(client)
    # 同一请求号并发 6 次：幂等，只落一条记录
    rs = await asyncio.gather(*(submit_infer(client, dep_id, "dup", "重复请求")
                                for _ in range(6)))
    codes = sorted(r.status_code for r in rs)
    assert codes.count(201) == 1 and codes.count(200) == 5, codes
    assert sum(1 for r in rs if r.json().get("deduplicated")) == 5
    reqs = [r for r in (await client.get(
        f"/api/infer/requests?deployment_id={dep_id}")).json() if r["request_no"] == "dup"]
    assert len(reqs) == 1

    # 再并发 7 条不同请求号：8 条总共应凑成 3+3+2 三个批次
    rs = await asyncio.gather(*(submit_infer(client, dep_id, f"c{i}", f"并发{i}")
                                for i in range(7)))
    assert all(r.status_code == 201 for r in rs)

    deadline = asyncio.get_event_loop().time() + 12
    while asyncio.get_event_loop().time() < deadline:
        reqs = (await client.get(f"/api/infer/requests?deployment_id={dep_id}")).json()
        if len(reqs) == 8 and all(r["status"] == "DONE" for r in reqs):
            break
        await asyncio.sleep(0.2)
    assert len(reqs) == 8 and all(r["status"] == "DONE" for r in reqs), reqs

    batches = await get_batches(client, dep_id)
    assert len(batches) == 3, batches
    assert sorted(b["request_count"] for b in batches) == [2, 3, 3]
    # 关键断言：任一请求只出现在一个批次中
    owner = {}
    for b in batches:
        for r in b["requests"]:
            assert r["id"] not in owner, f"请求 {r['request_no']} 进入两个批次"
            owner[r["id"]] = b["id"]
    assert len(owner) == 8


@pytest.mark.asyncio
async def test_11_dispatch_timeout_keeps_batch_then_recovers_original(client):
    # 场景：批次下发时节点失联（传输层失败），平台保留整批待核；
    # 节点其实已受理并算完该批（迟到回执），恢复后平台按原批号查询回收结果，
    # 绝不拆批重投。
    dep_id, rep = await make_available_single_dep(client)
    nid = rep["node_id"]
    await client.post(f"/api/nodes/{nid}/control", json={"mode": "unreachable"})
    rs = await asyncio.gather(*(submit_infer(client, dep_id, f"t{i}", f"超时{i}")
                                for i in range(3)))
    assert all(r.status_code == 201 for r in rs)
    batches = await get_batches(client, dep_id)
    assert len(batches) == 1
    batch_id = batches[0]["id"]

    # 下发失联 → 批次与批内请求保留待核
    b = await wait_batch(client, batch_id, {"TIMEOUT"})
    assert all(r["status"] == "TIMEOUT" for r in b["requests"])
    sim = sim_state(client, nid)
    assert sim.stats["infer_posts"] == 0  # 失联期间下发从未到达节点

    # 节点在“失联期间”其实受理并算完了该批（迟到回执落地），随后恢复
    sim.batches[batch_id] = {
        "batch_id": batch_id, "replica": rep["id"], "epoch": 1,
        "model": "infer-svc", "status": "done",
        "requests": [{"request_no": f"t{i}", "text": f"超时{i}"} for i in range(3)],
        "results": [{"request_no": f"t{i}",
                     "result": f"模拟输出[{nid}|infer-svc|e1|t{i}]::{f'超时{i}'[::-1]}"}
                    for i in range(3)],
        "posts": 1, "created_at": 0.0,
    }
    await client.post(f"/api/nodes/{nid}/control", json={"mode": "normal"})

    # 恢复后平台按原批号查询，回收整批结果
    b = await wait_batch(client, batch_id, {"DONE"}, timeout=10)
    assert all(r["status"] == "DONE" for r in b["requests"])
    results = {r["request_no"]: r["result"] for r in b["requests"]}
    for i in range(3):
        assert f"超时{i}"[::-1] in results[f"t{i}"]
    # 恢复靠的是查询原批，不是重投：节点侧下发端点始终未被再次调用
    assert sim.stats["infer_posts"] == 0


@pytest.mark.asyncio
async def test_12_unreachable_batch_never_resent_and_fails_definitively(client):
    dep_id, rep = await make_available_single_dep(client)
    nid = rep["node_id"]
    await client.post(f"/api/nodes/{nid}/control", json={"mode": "unreachable"})
    rs = await asyncio.gather(*(submit_infer(client, dep_id, f"u{i}", f"失联{i}")
                                for i in range(2)))
    assert all(r.status_code == 201 for r in rs)
    # 2 条未满批，等 200ms 窗口凑批
    batch_id = None
    deadline = asyncio.get_event_loop().time() + 3
    while asyncio.get_event_loop().time() < deadline:
        batches = await get_batches(client, dep_id)
        if batches:
            batch_id = batches[0]["id"]
            break
        await asyncio.sleep(0.05)
    assert batch_id is not None

    # 下发失联 → 批次保留待核
    b = await wait_batch(client, batch_id, {"TIMEOUT"})
    assert all(r["status"] == "TIMEOUT" for r in b["requests"])

    # 恢复后节点确认从未收到原批 → 明确判失败（未执行），且绝不重投
    await client.post(f"/api/nodes/{nid}/control", json={"mode": "normal"})
    b = await wait_batch(client, batch_id, {"FAILED"}, timeout=12)
    assert "未收到原批" in (b["last_error"] or "")
    assert all(r["status"] == "FAILED" for r in b["requests"])
    sim = sim_state(client, nid)
    assert batch_id not in sim.batches  # 节点从未收到，平台也未重投
    assert sim.stats["infer_posts"] == 0

    # 失败请求保持原批次终态，不会被重新凑批；新请求号可正常提交
    batches = await get_batches(client, dep_id)
    assert len(batches) == 1
    r = await submit_infer(client, dep_id, "n1", "恢复后的新请求")
    assert r.status_code == 201
    req = await wait_ireq(client, dep_id, "n1", {"DONE"})
    assert req["batch_id"] != batch_id


@pytest.mark.asyncio
async def test_13_pending_rejected_when_replica_lost_before_flush(client):
    dep_id, rep = await make_available_single_dep(client)
    r = await submit_infer(client, dep_id, "p1", "待下发")
    assert r.status_code == 201 and r.json()["status"] == "PENDING"

    # 副本在下发前失去可用性（部署转待核）
    with db_mod.tx() as conn:
        service._set_replica(conn, rep["id"], models.REP_UNVERIFIED, error="演练：副本待核")
        service._evaluate(conn, dep_id)

    # 凑批窗口关闭时不得改投：请求被驳回，不生成任何批次
    req = await wait_ireq(client, dep_id, "p1", {"REJECTED"})
    assert req["batch_id"] is None
    await asyncio.sleep(0.5)
    assert await get_batches(client, dep_id) == []

    # 部署恢复可用（同轮补确认）：已驳回请求不得重新入批
    with db_mod.tx() as conn:
        service._set_replica(conn, rep["id"], models.REP_LOADED, confirmed=True)
        service._evaluate(conn, dep_id)
    dep = (await client.get(f"/api/deployments/{dep_id}")).json()
    assert dep["status"] == "AVAILABLE"
    await asyncio.sleep(0.5)
    req = await get_ireq(client, dep_id, "p1")
    assert req["status"] == "REJECTED" and req["batch_id"] is None
    assert await get_batches(client, dep_id) == []

    # 同请求号重复提交：幂等返回已驳回记录，不会重新入批
    r2 = await submit_infer(client, dep_id, "p1", "待下发")
    assert r2.status_code == 200 and r2.json()["deduplicated"]
    assert r2.json()["status"] == "REJECTED"
    # 新请求号可正常入批完成
    r3 = await submit_infer(client, dep_id, "p2", "新的请求")
    assert r3.status_code == 201
    req = await wait_ireq(client, dep_id, "p2", {"DONE"})
    assert req["batch_id"]


@pytest.mark.asyncio
async def test_14_pending_rejected_when_round_ends_no_retarget(client):
    dep_id, rep = await make_available_single_dep(client)
    r = await submit_infer(client, dep_id, "e1", "旧轮请求")
    assert r.status_code == 201 and r.json()["epoch"] == 1

    # 部署失败（副本显式失效）→ 旧轮次结束
    with db_mod.tx() as conn:
        service._set_replica(conn, rep["id"], models.REP_LOAD_FAILED,
                             error="演练：副本失效", released=True)
        service._evaluate(conn, dep_id)
    req = await wait_ireq(client, dep_id, "e1", {"REJECTED"})
    assert req["batch_id"] is None

    # 重试开启新轮次，部署恢复可用
    r = await client.post(f"/api/deployments/{dep_id}/retry")
    assert r.status_code == 200
    dep = await wait_for(client, dep_id, {"AVAILABLE"})
    assert dep["epoch"] == 2

    # 旧轮请求不得改投新轮：仍为驳回终态，且不在任何批次中
    await asyncio.sleep(0.5)
    req = await get_ireq(client, dep_id, "e1")
    assert req["status"] == "REJECTED" and req["batch_id"] is None

    r2 = await submit_infer(client, dep_id, "e2", "新轮请求")
    assert r2.status_code == 201 and r2.json()["epoch"] == 2
    req2 = await wait_ireq(client, dep_id, "e2", {"DONE"})
    batches = await get_batches(client, dep_id)
    assert batches and all(b["epoch"] == 2 for b in batches)
    assert all(req["request_no"] != "e1" for b in batches for req in b["requests"])


@pytest.mark.asyncio
async def test_15_late_receipt_never_written_into_new_round(client):
    dep_id, rep = await make_available_single_dep(client)
    nid = rep["node_id"]
    # 节点处理整批需 6s（> INFER_TIMEOUT=4）：批次会超时待核，但节点最终算完
    await client.post(f"/api/nodes/{nid}/control", json={"infer_delay": 6})
    rs = await asyncio.gather(*(submit_infer(client, dep_id, f"w{i}", f"旧批{i}")
                                for i in range(3)))
    assert all(r.status_code == 201 for r in rs)
    batch1 = (await get_batches(client, dep_id))[0]["id"]
    b1 = await wait_batch(client, batch1, {"DISPATCHED"})
    assert b1["epoch"] == 1
    # 等待整批结果超时 → 保留待核
    b1 = await wait_batch(client, batch1, {"TIMEOUT"}, timeout=8)
    assert all(r["status"] == "TIMEOUT" for r in b1["requests"])

    # 旧批仍在节点处理时，部署进入新轮次（待核→释放→失败→重试）
    with db_mod.tx() as conn:
        service._set_replica(conn, rep["id"], models.REP_UNVERIFIED, error="演练：副本待核")
        service._evaluate(conn, dep_id)
    await service.request_release(rep["id"], "演练：撤销旧加载")
    await client.post(f"/api/deployments/{dep_id}/reconcile")
    dep = await wait_for(client, dep_id, {"FAILED"})
    r = await client.post(f"/api/deployments/{dep_id}/retry")
    assert r.status_code == 200
    dep = await wait_for(client, dep_id, {"AVAILABLE"})
    assert dep["epoch"] == 2

    # 新轮提交新请求（恢复正常推理时延）
    await client.post(f"/api/nodes/{nid}/control", json={"infer_delay": 0.3})
    rs = await asyncio.gather(*(submit_infer(client, dep_id, f"n{i}", f"新批{i}")
                                for i in range(3)))
    assert all(r.status_code == 201 and r.json()["epoch"] == 2 for r in rs)
    batches = await get_batches(client, dep_id)
    assert len(batches) == 2
    batch2 = next(b["id"] for b in batches if b["epoch"] == 2)
    assert batch2 != batch1

    # 旧批的迟到回执到达：只能写回旧轮请求，绝不能写入新轮
    b1 = await wait_batch(client, batch1, {"DONE"}, timeout=10)
    assert b1["epoch"] == 1
    assert all(r["status"] == "DONE" for r in b1["requests"])
    assert all("|e1|" in r["result"] for r in b1["requests"])
    assert {r["request_no"] for r in b1["requests"]} == {"w0", "w1", "w2"}

    b2 = await wait_batch(client, batch2, {"DONE"}, timeout=10)
    assert b2["epoch"] == 2
    assert {r["request_no"] for r in b2["requests"]} == {"n0", "n1", "n2"}
    assert all("|e2|" in r["result"] for r in b2["requests"])
    # 两个批次的请求集合互不重叠
    assert not ({r["request_no"] for r in b1["requests"]}
                & {r["request_no"] for r in b2["requests"]})
    # 旧批全程只下发过 1 次
    sim = sim_state(client, nid)
    assert sim.batches[batch1]["posts"] == 1
