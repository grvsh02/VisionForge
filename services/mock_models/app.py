"""Mock inference endpoints with realistic latency, rate limits, failures and idempotency.

One app serves either model (``MOCK_MODEL=layout|vlm``):
  POST /v1/predict/layout   50ms, 100 RPS, 2% failures
  POST /v1/predict/vlm      1.5-3s, 10 RPS, 5% failures

Latency is load-sensitive, like a real inference server: up to ``capacity`` concurrent
requests run at the base latency; beyond that every request slows down in proportion to
the overload (``active / capacity``), so backing off really does bring latency down.

Idempotency-Key semantics (Stripe-like): successful responses are cached per key, so a
retried request never re-runs inference; concurrent duplicates join the in-flight call;
errors are not cached so genuine retries do execute.

Admin: GET /admin/stats, POST /admin/chaos, POST /admin/reset. Chaos knobs for testing
backpressure (all changeable at runtime):
  rps_limit           lower it to produce a 429 storm
  capacity            lower it to overload the model (latency rises with our traffic)
  latency_multiplier  raise it for a load-independent slowdown (e.g. a slower model version)
  failure_rate        share of calls that fail with ``error_status`` (500 by default; set
                      401 / 400 / 404 / 408 to exercise the other error classes)
  outage              every request returns 503
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import os
import random
import time
from dataclasses import asdict, dataclass

from fastapi import FastAPI, File, Form, Header, HTTPException, Request, UploadFile
from fastapi.responses import JSONResponse, Response
from prometheus_client import Counter, make_asgi_app
from redis.asyncio import Redis

from vf_common import tracing

MODEL = os.environ.get("MOCK_MODEL", "vlm")
DEFAULTS = {
    "layout": {"rps": 100.0, "lat_min": 0.05, "lat_max": 0.05, "failure_rate": 0.02},
    "vlm": {"rps": 10.0, "lat_min": 1.5, "lat_max": 3.0, "failure_rate": 0.05},
}[MODEL]


def _env(name: str, default: float) -> float:
    return float(os.environ.get(name, default))


LATENCY_SCALE = _env("VF_LATENCY_SCALE", 1.0)
# Concurrency the model serves at base latency: its rate limit x mean latency, +25% headroom.
DEFAULT_CAPACITY = max(2.0, 1.25 * DEFAULTS["rps"] * (DEFAULTS["lat_min"] + DEFAULTS["lat_max"]) / 2 * LATENCY_SCALE)


@dataclass
class Chaos:
    rps_limit: float = _env("MOCK_RPS", DEFAULTS["rps"])
    capacity: float = _env("MOCK_CAPACITY", DEFAULT_CAPACITY)
    latency_min_s: float = _env("MOCK_LATENCY_MIN_S", DEFAULTS["lat_min"])
    latency_max_s: float = _env("MOCK_LATENCY_MAX_S", DEFAULTS["lat_max"])
    latency_multiplier: float = 1.0
    failure_rate: float = _env("MOCK_FAILURE_RATE", DEFAULTS["failure_rate"])
    error_status: int = 500
    outage: bool = False


REQUESTS = Counter("mock_requests_total", "Requests by outcome", ["model", "outcome"])

app = FastAPI(title=f"mock-{MODEL}")
app.add_middleware(tracing.RequestIdMiddleware)
app.mount("/metrics", make_asgi_app())
chaos = Chaos()
redis = Redis.from_url(os.environ.get("VF_REDIS_URL", "redis://localhost:6379/0"))
_inflight: dict[str, asyncio.Future] = {}
_active = 0  # requests currently executing (drives load-sensitive latency)


class Bucket:
    def __init__(self) -> None:
        self.tokens = chaos.rps_limit
        self.ts = time.monotonic()

    def take(self) -> float:
        """Return 0 if a token was taken, otherwise seconds until one is available."""
        now = time.monotonic()
        rate = max(chaos.rps_limit, 0.001)
        self.tokens = min(rate, self.tokens + (now - self.ts) * rate)
        self.ts = now
        if self.tokens >= 1:
            self.tokens -= 1
            return 0.0
        return (1 - self.tokens) / rate


bucket = Bucket()


def _k(suffix: str) -> str:
    return f"mock:{MODEL}:{suffix}"


async def _count(field: str) -> None:
    REQUESTS.labels(MODEL, field).inc()
    await redis.hincrby(_k("stats"), field, 1)


# --- deterministic synthetic output -------------------------------------------------

WORDS = ("invoice total amount due vendor contract clause party agreement table value sample "
         "measurement result section figure date signature account balance item quantity price "
         "tax shipping notes reference method analysis summary").split()
BLOCK_TYPES = ("title", "paragraph", "paragraph", "table", "list", "figure", "kv")
PAGE_W, PAGE_H = 612.0, 792.0


def _rng(data: bytes, salt: str) -> random.Random:
    return random.Random(hashlib.sha256(data + salt.encode()).digest())


def _phrase(rng: random.Random, n: int) -> str:
    return " ".join(rng.choice(WORDS) for _ in range(n))


def synth_layout(data: bytes) -> dict:
    rng = _rng(data, "layout")
    blocks, y = [], 48.0
    for i in range(rng.randint(3, 8)):
        h = rng.uniform(30, 110)
        if y + h > PAGE_H - 48:
            break
        btype = "title" if i == 0 else rng.choice(BLOCK_TYPES[1:])
        blocks.append({
            "id": f"b{i}", "type": btype, "order": i,
            "bbox": {"x": 54.0, "y": round(y, 2), "w": round(rng.uniform(300, 504), 2), "h": round(h, 2)},
            "text": _phrase(rng, rng.randint(2, 6)),
            "confidence": round(rng.uniform(0.6, 0.85), 3),
        })
        y += h + rng.uniform(8, 24)
    return {"model": "layout", "page_width": PAGE_W, "page_height": PAGE_H, "blocks": blocks}


def synth_vlm(data: bytes, hints: dict | None) -> dict:
    rng = _rng(data, "vlm")
    layout = hints if hints and hints.get("blocks") else synth_layout(data)
    blocks, tables, kv = [], [], {}
    for b in layout["blocks"]:
        out = dict(b, confidence=round(rng.uniform(0.9, 0.99), 3))
        if b["type"] == "table":
            rows = rng.randint(2, 5)
            md = ["| item | qty | price |", "|---|---|---|"]
            md += [f"| {rng.choice(WORDS)} | {rng.randint(1, 20)} | {rng.uniform(1, 500):.2f} |"
                   for _ in range(rows)]
            out["text"] = "\n".join(md)
            tables.append(out["text"])
        elif b["type"] == "kv":
            key, val = rng.choice(WORDS), f"{rng.randint(1000, 99999)}"
            kv[key] = val
            out["text"] = f"{key}: {val}"
        else:
            out["text"] = _phrase(rng, rng.randint(8, 40))
        blocks.append(out)
    return {"model": "vlm", "page_width": PAGE_W, "page_height": PAGE_H, "blocks": blocks,
            "tables_markdown": tables, "key_values": kv, "confidence": round(rng.uniform(0.9, 0.99), 3)}


# --- request handling ---------------------------------------------------------------

def overload_factor(active: int, capacity: float) -> float:
    """Latency multiplier from queueing: 1 up to capacity, then proportional to the overload."""
    return max(1.0, active / max(capacity, 0.001))


async def _execute(data: bytes, hints: dict | None) -> dict:
    global _active
    lo, hi = sorted((chaos.latency_min_s, chaos.latency_max_s))
    _active += 1
    try:
        base = random.uniform(lo, hi) * chaos.latency_multiplier * LATENCY_SCALE
        await asyncio.sleep(base * overload_factor(_active, chaos.capacity))
    finally:
        _active -= 1
    if random.random() < chaos.failure_rate:
        raise HTTPException(chaos.error_status, "injected inference failure")
    return synth_layout(data) if MODEL == "layout" else synth_vlm(data, hints)


async def _replay(body: bytes) -> Response:
    await _count("replays")
    return Response(body, media_type="application/json", headers={"X-Idempotent-Replay": "true"})


async def _predict(file: UploadFile, hints: str | None, idem_key: str | None) -> Response:
    await _count("requests")
    if not idem_key:
        raise HTTPException(400, "Idempotency-Key header is required")
    if chaos.outage:
        await _count("outage")
        raise HTTPException(503, "model outage (chaos)")

    cached = await redis.get(_k(f"idem:{idem_key}"))
    if cached is not None:
        return await _replay(cached)
    if idem_key in _inflight:  # concurrent duplicate: join instead of re-executing
        await _count("inflight_joins")
        body = await asyncio.shield(_inflight[idem_key])
        return Response(body, media_type="application/json", headers={"X-Idempotent-Replay": "true"})

    wait = bucket.take()
    if wait > 0:
        await _count("rate_limited")
        return JSONResponse({"error": "rate limited"}, status_code=429, headers={
            "Retry-After": str(max(1, math.ceil(wait))), "X-Retry-After-Ms": str(int(wait * 1000) + 1)})

    # Register synchronously after the in-flight check (no await in between), then re-check
    # the cache: a call that finished while we awaited the first GET has cached by now.
    fut: asyncio.Future = asyncio.get_running_loop().create_future()
    _inflight[idem_key] = fut
    try:
        cached = await redis.get(_k(f"idem:{idem_key}"))
        if cached is not None:
            fut.set_result(cached)
            return await _replay(cached)
        data = await file.read()
        result = await _execute(data, json.loads(hints) if hints else None)
        body = json.dumps(result).encode()
        await redis.set(_k(f"idem:{idem_key}"), body, ex=86_400)
        await redis.hincrby(_k("execs"), idem_key, 1)
        await _count("executions")
        fut.set_result(body)
        return Response(body, media_type="application/json")
    except HTTPException as exc:
        await _count("failures")
        fut.set_exception(exc)
        raise
    except BaseException as exc:
        fut.set_exception(HTTPException(500, "inference aborted"))
        raise exc
    finally:
        _inflight.pop(idem_key, None)
        if fut.done() and not fut.cancelled():
            fut.exception()  # mark retrieved so un-joined failures don't warn


@app.post(f"/v1/predict/{MODEL}")
async def predict(file: UploadFile = File(...), hints: str | None = Form(None),
                  idempotency_key: str | None = Header(None)) -> Response:
    return await _predict(file, hints, idempotency_key)


@app.get("/admin/stats")
async def stats(detail: bool = False) -> dict:
    counters = {k.decode(): int(v) for k, v in (await redis.hgetall(_k("stats"))).items()}
    execs = {k.decode(): int(v) for k, v in (await redis.hgetall(_k("execs"))).items()}
    dupes = {k: v for k, v in execs.items() if v > 1}
    out = {"model": MODEL, "counters": counters, "unique_keys": len(execs),
           "duplicate_executions": len(dupes), "duplicates": dict(list(dupes.items())[:50]),
           "chaos": asdict(chaos)}
    if detail:
        out["executions"] = execs
    return out


@app.post("/admin/chaos")
async def set_chaos(request: Request) -> dict:
    patch = await request.json()
    for key, value in patch.items():
        if not hasattr(chaos, key):
            raise HTTPException(400, f"unknown chaos field {key}")
        setattr(chaos, key, type(getattr(chaos, key))(value))
    return asdict(chaos)


@app.post("/admin/reset")
async def reset() -> dict:
    global chaos
    chaos = Chaos()
    keys = [k async for k in redis.scan_iter(match=f"mock:{MODEL}:*", count=1000)]
    for i in range(0, len(keys), 500):
        await redis.delete(*keys[i:i + 500])
    return {"reset": True, "deleted": len(keys)}


@app.get("/healthz")
async def healthz() -> dict:
    return {"ok": True, "model": MODEL}
