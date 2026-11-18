"""节点 HTTP 客户端：所有节点交互的统一出口。

错误分类（决定显存处置语义）：
- 传输层异常（超时/连接失败/协议异常）：节点真实状态未知 -> 保留占用待核
- 显式 HTTP 错误响应（4xx/5xx）：节点明确表达拒绝/失败 -> 确认未占用，可释放预留
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx

from . import config


@dataclass
class CallResult:
    ok: bool
    status_code: int = 0
    data: Any = None
    error: str = ""
    timed_out: bool = False
    connect_error: bool = False  # 传输层问题：节点状态未知，必须保留占用

    @property
    def transport_failed(self) -> bool:
        """节点状态不可知（超时或连接异常），调用方须保留占用。"""
        return self.timed_out or self.connect_error

    @property
    def failed(self) -> bool:
        return not self.ok


_client: httpx.AsyncClient | None = None


async def client() -> httpx.AsyncClient:
    global _client
    if _client is None or _client.is_closed:
        _client = httpx.AsyncClient(timeout=config.CALL_TIMEOUT)
    return _client


async def close() -> None:
    global _client
    if _client and not _client.is_closed:
        await _client.aclose()
    _client = None


async def health(base_url: str) -> CallResult:
    try:
        c = await client()
        r = await c.get(f"{base_url}/health")
        if r.status_code == 200:
            return CallResult(True, 200, r.json())
        return CallResult(False, r.status_code, error=f"HTTP {r.status_code}")
    except httpx.TimeoutException:
        return CallResult(False, timed_out=True, error="健康探测超时（节点失联）")
    except (httpx.HTTPError, OSError) as exc:
        return CallResult(False, connect_error=True,
                          error=f"连接异常: {exc.__class__.__name__}")


async def load(base_url: str, model_name: str, replica_id: str, epoch: int, mem_mb: int) -> CallResult:
    try:
        c = await client()
        r = await c.post(
            f"{base_url}/models/{model_name}/load",
            params={"replica": replica_id, "epoch": epoch, "mem_mb": mem_mb},
        )
        if r.status_code in (202, 200):
            return CallResult(True, r.status_code, r.json())
        detail = ""
        try:
            detail = r.json().get("detail", "")
        except Exception:
            detail = r.text[:200]
        # 节点明确拒绝：确认未占用其显存
        return CallResult(False, r.status_code, error=f"加载被节点拒绝(HTTP {r.status_code}): {detail}")
    except httpx.TimeoutException:
        # 调用在途失联：节点可能已加载也可能没有，必须保留占用待核
        return CallResult(False, timed_out=True,
                          error="加载调用超时，节点状态未知（保留占用待核）")
    except (httpx.HTTPError, OSError) as exc:
        return CallResult(False, connect_error=True,
                          error=f"连接失败（节点失联，状态未知，保留占用待核）: {exc.__class__.__name__}")


async def unload(base_url: str, model_name: str, replica_id: str, epoch: int) -> CallResult:
    try:
        c = await client()
        r = await c.post(
            f"{base_url}/models/{model_name}/unload",
            params={"replica": replica_id, "epoch": epoch},
        )
        if r.status_code in (202, 200):
            return CallResult(True, r.status_code, r.json())
        return CallResult(False, r.status_code, error=f"卸载被拒(HTTP {r.status_code})")
    except httpx.TimeoutException:
        return CallResult(False, timed_out=True,
                          error="卸载调用超时（保持释放中，待对账确认）")
    except (httpx.HTTPError, OSError) as exc:
        return CallResult(False, connect_error=True,
                          error=f"连接失败（保持释放中，待对账确认）: {exc.__class__.__name__}")


async def query_replica(base_url: str, replica_id: str, epoch: int) -> CallResult:
    try:
        c = await client()
        r = await c.get(
            f"{base_url}/models/by-replica/{replica_id}",
            params={"epoch": epoch},
        )
        if r.status_code == 200:
            return CallResult(True, 200, r.json())
        if r.status_code == 404:
            return CallResult(True, 404, {"status": "unknown"})
        return CallResult(False, r.status_code, error=f"查询被拒(HTTP {r.status_code})")
    except httpx.TimeoutException:
        return CallResult(False, timed_out=True, error="查询超时（节点失联，继续待核）")
    except (httpx.HTTPError, OSError) as exc:
        return CallResult(False, connect_error=True,
                          error=f"连接失败（节点失联，继续待核）: {exc.__class__.__name__}")


async def infer_batch(base_url: str, model_name: str, payload: dict) -> CallResult:
    """整批下发推理请求。传输失败 = 批次是否到达不可知，调用方必须保留待核。"""
    try:
        c = await client()
        r = await c.post(f"{base_url}/models/{model_name}/infer/batch", json=payload)
        if r.status_code in (200, 202):
            return CallResult(True, r.status_code, r.json())
        detail = ""
        try:
            detail = r.json().get("detail", "")
        except Exception:
            detail = r.text[:200]
        # 节点明确拒绝（如副本未加载/轮次不符）：确认未执行，可判失败
        return CallResult(False, r.status_code,
                          error=f"批次下发被节点拒绝(HTTP {r.status_code}): {detail}")
    except httpx.TimeoutException:
        return CallResult(False, timed_out=True,
                          error="批次下发超时，节点是否收到未知（保留待核）")
    except (httpx.HTTPError, OSError) as exc:
        return CallResult(False, connect_error=True,
                          error=f"连接失败（节点失联，批次状态未知，保留待核）: {exc.__class__.__name__}")


async def query_infer_batch(base_url: str, batch_id: str, epoch: int) -> CallResult:
    """按批次号查询原批结果（恢复对账用）；绝不重新下发。"""
    try:
        c = await client()
        r = await c.get(f"{base_url}/infer/batches/{batch_id}", params={"epoch": epoch})
        if r.status_code == 200:
            return CallResult(True, 200, r.json())
        if r.status_code == 404:
            # 节点确认无此批：原批未到达/未执行（确定语义）
            return CallResult(True, 404, {"status": "unknown"})
        return CallResult(False, r.status_code, error=f"批次查询被拒(HTTP {r.status_code})")
    except httpx.TimeoutException:
        return CallResult(False, timed_out=True, error="批次查询超时（节点失联，继续待核）")
    except (httpx.HTTPError, OSError) as exc:
        return CallResult(False, connect_error=True,
                          error=f"连接失败（节点失联，继续待核）: {exc.__class__.__name__}")
