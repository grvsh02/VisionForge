"""Settings shared by every service.

Infrastructure endpoints come from the environment (prefix ``VF_``). The two models'
limits are constants taken from the architecture doc, in one place (``MODELS``).
"""

from __future__ import annotations

import os
import socket
from dataclasses import dataclass
from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


@dataclass(frozen=True)
class Model:
    """One inference endpoint and the limits the controller works within."""

    name: str  # model name in the API path (/v1/predict/<name>)
    stage: str  # pipeline stage it serves: "fast" or "slow"
    hard_rps: float  # provider's hard rate limit; never exceeded
    start_rps: float  # adaptive RPS at startup
    start_concurrency: int
    max_concurrency: int
    nominal_latency_s: float  # overload if p95 latency > 2x this
    normal_error_rate: float  # overload if 5xx rate > 2x this
    timeout_s: float


MODELS: dict[str, Model] = {
    "fast": Model(name="layout", stage="fast", hard_rps=100, start_rps=80, start_concurrency=5,
                  max_concurrency=10, nominal_latency_s=0.05, normal_error_rate=0.02, timeout_s=2.0),
    "slow": Model(name="vlm", stage="slow", hard_rps=10, start_rps=8, start_concurrency=20,
                  max_concurrency=30, nominal_latency_s=3.0, normal_error_rate=0.05, timeout_s=10.0),
}
MODEL_VERSION = "v1"  # part of every idempotency key; bump when a model changes


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="VF_", extra="ignore")

    database_url: str = "postgresql://vf:vf@localhost:5432/vf"
    redis_url: str = "redis://localhost:6379/0"
    s3_endpoint: str = "http://localhost:8333"
    s3_access_key: str = "visionforge"
    s3_secret_key: str = "visionforge"
    s3_region: str = "ap-south-1"
    s3_bucket: str = "visionforge"
    sqs_endpoint: str = "http://localhost:9324"
    queue_prefix: str = "vf"  # queues are <prefix>-split, <prefix>-fast, <prefix>-slow (+ -dlq)
    layout_url: str = "http://localhost:8081"
    vlm_url: str = "http://localhost:8082"

    stage: str = "slow"  # which stage a worker serves: fast | slow
    max_upload_bytes: int = 100 * 1024 * 1024
    max_pages: int = 100
    upload_part_bytes: int = 5 * 1024 * 1024

    instance_id: str = f"{socket.gethostname()}-{os.getpid()}"
    metrics_port: int = 9100
    log_level: str = "INFO"

    def model_url(self, model: Model) -> str:
        return self.layout_url if model.name == "layout" else self.vlm_url


@lru_cache
def get_settings() -> Settings:
    return Settings()
