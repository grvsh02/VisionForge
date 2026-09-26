"""Prometheus metrics shared by the services, plus helpers to expose them."""

from __future__ import annotations

import json
import logging
import sys

import psutil
from prometheus_client import Counter, Gauge, Histogram, start_http_server

from vf_common import tracing

# --- models (architecture doc, section 12) -------------------------------------------
MODEL_CALLS = Counter("vf_model_calls_total", "Model calls by outcome class", ["model", "outcome"])
MODEL_LATENCY = Histogram(
    "vf_model_latency_seconds", "Model call latency", ["model"],
    buckets=(0.025, 0.05, 0.1, 0.25, 0.5, 1, 1.5, 2, 2.5, 3, 4, 5, 7.5, 10))
ADAPTIVE_RPS = Gauge("vf_adaptive_rps", "Current adaptive RPS limit", ["model"])
ADAPTIVE_CONCURRENCY = Gauge("vf_adaptive_concurrency", "Current adaptive concurrency limit", ["model"])
CONCURRENCY_IN_USE = Gauge("vf_concurrency_in_use", "Concurrency slots in use", ["model"])
CONTROLLER_VERDICTS = Counter("vf_controller_verdicts_total", "Window verdicts", ["model", "verdict"])
BREAKER_OPEN = Gauge("vf_breaker_open", "1 if the model's circuit breaker is not closed", ["model"])
FAST_ADMISSION_RPS = Gauge("vf_fast_admission_rps", "Fast-stage admission cap from the slow queue depth")

# --- queues and pipeline ------------------------------------------------------------
QUEUE_DEPTH = Gauge("vf_queue_depth", "Messages waiting in an SQS queue", ["queue"])
QUEUE_OLDEST = Gauge("vf_queue_oldest_seconds", "Age of the oldest page waiting for a stage", ["stage"])
DRAIN_TIME = Gauge("vf_slow_drain_time_seconds", "Slow queue depth / recent slow throughput")
QUEUE_WAIT = Histogram("vf_queue_wait_seconds", "Time a page waited before a stage picked it up", ["stage"],
                       buckets=(0.1, 0.5, 1, 2, 5, 10, 20, 30, 60, 120, 300, 600))
PAGE_LATENCY = Histogram("vf_page_end_to_end_seconds", "Page created -> completed", ["source"],
                         buckets=(1, 2, 5, 10, 20, 30, 60, 120, 300, 600, 1800))
S3_WRITE = Histogram("vf_s3_write_seconds", "Result write latency", buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 1))
PAGES = Counter("vf_pages_total", "Stage outcomes", ["stage", "outcome"])
RETRIES = Counter("vf_retries_total", "Retries scheduled", ["stage", "reason"])
DLQ_SENT = Counter("vf_dlq_sent_total", "Messages copied to a DLQ", ["queue"])
OUTBOX_PUBLISHED = Counter("vf_outbox_published_total", "Outbox rows published", ["queue"])
OUTBOX_LAG = Histogram("vf_outbox_lag_seconds", "DB commit -> queue publish latency",
                       buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10))
OUTBOX_BACKLOG = Gauge("vf_outbox_backlog", "Unsent outbox rows")
OUTBOX_OLDEST = Gauge("vf_outbox_oldest_seconds", "Age of the oldest unsent outbox row")

STREAM_SUBSCRIBERS = Gauge("vf_stream_subscribers", "Connected stream subscribers")
STREAM_FRAMES = Counter("vf_stream_frames_total", "Frames sent", ["kind"])
UPLOADS = Counter("vf_uploads_total", "Job submissions", ["outcome"])
EVAL_SECONDS = Histogram("vf_eval_seconds", "Time to compute one /evaluate metric", ["metric"],
                         buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 1, 2.5))
EVALUATIONS = Counter("vf_evaluations_total", "/evaluate requests", ["outcome"])

RSS = Gauge("vf_process_rss_bytes", "Resident set size of this process")


def sample_rss() -> int:
    rss = psutil.Process().memory_info().rss
    RSS.set(rss)
    return rss


def serve_metrics(port: int) -> None:
    start_http_server(port)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        out = {"ts": self.formatTime(record), "level": record.levelname, "logger": record.name,
               "msg": record.getMessage()}
        out.update(tracing.current())  # request_id / job_id / page of the work being done
        out.update(getattr(record, "fields", {}))
        if record.exc_info:
            out["exc"] = self.formatException(record.exc_info)
        return json.dumps(out, default=str)


def setup_logging(level: str = "INFO") -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    logging.basicConfig(level=level, handlers=[handler], force=True)
