import asyncio

import fakeredis
import httpx
import pytest

from services.mock_models import app as mock


@pytest.fixture
async def client(monkeypatch):
    monkeypatch.setattr(mock, "redis", fakeredis.FakeAsyncRedis())
    monkeypatch.setattr(mock, "chaos", mock.Chaos(latency_min_s=0.05, latency_max_s=0.05, failure_rate=0.0,
                                                  rps_limit=1000))
    monkeypatch.setattr(mock, "bucket", mock.Bucket())
    monkeypatch.setattr(mock, "_inflight", {})
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=mock.app), base_url="http://mock") as c:
        yield c


def call(client, key="k1", data=b"page"):
    return client.post(f"/v1/predict/{mock.MODEL}", files={"file": ("p.pdf", data)},
                       headers={"Idempotency-Key": key})


async def stats(client):
    return (await client.get("/admin/stats")).json()


async def test_replayed_key_returns_cached_result_without_re_executing(client):
    first, second = await call(client), await call(client)
    assert first.status_code == second.status_code == 200
    assert first.json() == second.json() and second.headers["X-Idempotent-Replay"] == "true"
    s = await stats(client)
    assert s["counters"]["executions"] == 1 and s["counters"]["replays"] == 1 and s["duplicate_executions"] == 0


async def test_concurrent_duplicates_execute_inference_once(client):
    responses = await asyncio.gather(*(call(client, "dup") for _ in range(10)))
    assert all(r.status_code == 200 for r in responses)
    assert len({r.content for r in responses}) == 1
    s = await stats(client)
    assert s["counters"]["executions"] == 1 and s["duplicate_executions"] == 0
    assert s["counters"].get("inflight_joins", 0) + s["counters"].get("replays", 0) == 9


async def test_failures_are_not_cached_so_retries_execute(client):
    await client.post("/admin/chaos", json={"failure_rate": 1.0})
    assert (await call(client, "f")).status_code == 500
    await client.post("/admin/chaos", json={"failure_rate": 0.0})
    assert (await call(client, "f")).status_code == 200
    assert (await stats(client))["counters"]["executions"] == 1


async def test_rate_limit_returns_429_with_retry_after(client):
    await client.post("/admin/chaos", json={"rps_limit": 1.0})
    mock.bucket.tokens = 1.0
    ok, limited = await call(client, "r1"), await call(client, "r2")
    assert ok.status_code == 200 and limited.status_code == 429
    assert int(limited.headers["Retry-After"]) >= 1 and int(limited.headers["X-Retry-After-Ms"]) > 0


async def test_missing_idempotency_key_is_rejected(client):
    resp = await client.post(f"/v1/predict/{mock.MODEL}", files={"file": ("p.pdf", b"x")})
    assert resp.status_code == 400


def test_overload_factor():
    assert mock.overload_factor(5, capacity=10) == 1.0  # under capacity: base latency
    assert mock.overload_factor(20, capacity=10) == 2.0  # 2x overloaded: 2x slower


async def test_latency_rises_with_concurrent_load(client):
    await client.post("/admin/chaos", json={"capacity": 2})
    start = asyncio.get_running_loop().time()
    await call(client, "solo")
    solo = asyncio.get_running_loop().time() - start
    start = asyncio.get_running_loop().time()
    await asyncio.gather(*(call(client, f"burst{i}") for i in range(8)))
    burst = asyncio.get_running_loop().time() - start
    assert burst > 2.5 * solo  # 8 in flight on capacity 2 -> up to 4x slower
