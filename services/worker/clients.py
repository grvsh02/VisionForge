"""HTTP client for the model endpoints. It reports what happened (status, timeout, latency);
``vf_common.errors.classify`` decides what that means."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass

import httpx

from vf_common.config import Model, Settings
from vf_common import tracing
from vf_common.errors import Outcome, classify


@dataclass
class CallResult:
    status: int | None  # None: timeout or connection error
    latency_s: float
    timed_out: bool = False
    body: dict | None = None
    retry_after_s: float = 1.0
    error: str | None = None
    replay: bool = False  # served from the model's idempotency cache

    @property
    def outcome(self) -> Outcome:
        return classify(self.status, self.timed_out)


def _retry_after(resp: httpx.Response) -> float:
    ms = resp.headers.get("X-Retry-After-Ms")
    if ms:
        return max(0.05, int(ms) / 1000)
    try:
        return max(0.05, float(resp.headers.get("Retry-After", "1")))
    except ValueError:
        return 1.0


class ModelClient:
    def __init__(self, settings: Settings, model: Model) -> None:
        self.url = f"{settings.model_url(model)}/v1/predict/{model.name}"
        self.timeout_s = model.timeout_s
        self.http = httpx.AsyncClient(limits=httpx.Limits(max_connections=256, max_keepalive_connections=64))

    async def close(self) -> None:
        await self.http.aclose()

    async def predict(self, data: bytes, idem_key: str, hints: dict | None) -> CallResult:
        start = time.perf_counter()
        try:
            resp = await self.http.post(
                self.url, files={"file": ("page.pdf", data, "application/pdf")},
                data={"hints": json.dumps(hints)} if hints else None,
                headers={"Idempotency-Key": idem_key, **tracing.headers()}, timeout=self.timeout_s)
        except httpx.TimeoutException:
            return CallResult(None, time.perf_counter() - start, timed_out=True, error="timeout")
        except httpx.HTTPError as exc:
            return CallResult(None, time.perf_counter() - start, error=f"{type(exc).__name__}: {exc}")
        latency = time.perf_counter() - start
        if resp.status_code == 200:
            return CallResult(200, latency, body=resp.json(), replay=resp.headers.get("X-Idempotent-Replay") == "true")
        return CallResult(resp.status_code, latency, retry_after_s=_retry_after(resp),
                          error=f"HTTP {resp.status_code}: {resp.text[:200]}")
