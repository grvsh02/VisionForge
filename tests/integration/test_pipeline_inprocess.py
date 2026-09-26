"""In-process pipeline tests: the real outbox publisher, splitter and fast/slow workers run
against Postgres (VF_TEST_DATABASE_URL) and SQS (VF_TEST_SQS_URL), with fakeredis, an
in-memory object store and the model endpoints replaced by an httpx MockTransport."""

from __future__ import annotations

import asyncio
import json
import re
import time
import uuid
from collections import Counter
from pathlib import Path

import fakeredis
import httpx
import pytest

from scripts.common import make_pdf, stream
from services.mock_models.app import synth_layout, synth_vlm
from services.outbox_publisher import main as publisher
from services.splitter.main import Splitter
from services.stream_gateway import app as gateway
from services.stream_gateway.broker import JobSignals
from services.worker import main as worker_main
from services.worker.main import Worker
from tests.integration.conftest import PG_DSN, SQS_URL, create_split_job
from vf_common import repo
from vf_common.config import Settings
from vf_common.queues import MAX_DELIVERIES
from vf_common.ratelimit import breaker as brk
from vf_common.ratelimit import controller as ctl
from vf_common.storage import result_key

pytestmark = pytest.mark.skipif(not (PG_DSN and SQS_URL),
                                reason="set VF_TEST_DATABASE_URL and VF_TEST_SQS_URL to run pipeline tests")


class FakeStorage:
    def __init__(self) -> None:
        self.files: dict[str, bytes] = {}

    async def get_bytes(self, key: str) -> bytes:
        return self.files.get(key, key.encode())  # a page's bytes default to its key

    async def put_bytes(self, key: str, data: bytes, content_type: str = "") -> None:
        self.files[key] = data

    async def put_json(self, key: str, data) -> None:
        self.files[key] = json.dumps(data).encode()

    async def get_json(self, key: str):
        return json.loads(self.files[key])

    async def download_to_file(self, key: str, path: Path) -> int:
        path.write_bytes(self.files[key])
        return len(self.files[key])


class FakeModels:
    """``status[model]`` forces an error status; ``first[model] = (status, n)`` returns it for
    the first n calls only."""

    def __init__(self) -> None:
        self.calls: Counter[str] = Counter()
        self.pages: Counter[tuple[str, str, int]] = Counter()  # (model, job_id, idx) -> calls
        self.status: dict[str, int] = {}
        self.first: dict[str, tuple[int, int]] = {}

    def handler(self, request: httpx.Request) -> httpx.Response:
        model = request.url.path.rsplit("/", 1)[1]
        self.calls[model] += 1
        found = re.search(rb"pages/([0-9a-f-]{36})/(\d{4})", request.content)
        if found:
            self.pages[(model, found.group(1).decode(), int(found.group(2)))] += 1
        status, left = self.first.get(model, (200, 0))
        if left:
            self.first[model] = (status, left - 1)
        else:
            status = self.status.get(model, 200)
        if status == 429:
            return httpx.Response(429, headers={"X-Retry-After-Ms": "50"})
        if status != 200:
            return httpx.Response(status, text="injected")
        key = request.headers["Idempotency-Key"].encode()
        return httpx.Response(200, json=synth_layout(key) if model == "layout" else synth_vlm(key, None))


class Pipeline:
    def __init__(self, pg_dsn, pool, queues, models, storage=None, splitter=False) -> None:
        self.pg_dsn, self.pool, self.queues = pg_dsn, pool, queues
        self.redis = fakeredis.FakeAsyncRedis()
        self.storage = storage or FakeStorage()
        base = dict(database_url=pg_dsn, sqs_endpoint=SQS_URL, queue_prefix=queues.settings.queue_prefix)
        self.workers = []
        for stage in ("fast", "slow"):
            w = Worker(Settings(stage=stage, **base), pool, self.redis, self.storage, queues)
            w.client.http = httpx.AsyncClient(transport=httpx.MockTransport(models.handler))
            w.breaker = brk.CircuitBreaker(self.redis, open_s=brk.OPEN_S)
            self.workers.append(w)
        self.splitter = Splitter(Settings(**base), pool, self.storage, queues) if splitter else None
        self.stopping = asyncio.Event()

    async def __aenter__(self) -> "Pipeline":
        self.tasks = [asyncio.create_task(publisher.run(self.pool, self.queues, self.pg_dsn, self.stopping))]
        self.tasks += [asyncio.create_task(w.run()) for w in self.workers]
        if self.splitter:
            self.tasks.append(asyncio.create_task(self.splitter.run()))
        return self

    async def __aexit__(self, *exc) -> None:
        self.stopping.set()
        for w in self.workers:
            w.stopping.set()
        if self.splitter:
            self.splitter.stopping.set()
        await asyncio.wait_for(asyncio.gather(*self.tasks, return_exceptions=True), 30)


@pytest.fixture(autouse=True)
def quick_retries(monkeypatch, request):
    monkeypatch.setattr(worker_main, "backoff_s", lambda attempt: 0.0)
    monkeypatch.setattr(worker_main, "deferral_s", lambda: 1.0)
    monkeypatch.setattr(brk, "OPEN_S", 0.5)
    if "budget" not in request.node.name:  # only the budget test exercises the retry budget
        monkeypatch.setattr(ctl, "RETRY_BUDGET_MIN", 1_000_000)


async def wait_complete(pool, job_ids, timeout=60.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        n = await pool.fetchval("SELECT count(*) FROM jobs WHERE id = ANY($1) AND status IN ('COMPLETE', 'FAILED')",
                                job_ids)
        if n == len(job_ids):
            return
        await asyncio.sleep(0.1)
    states = await pool.fetch("SELECT status, count(*) FROM pages GROUP BY status")
    raise AssertionError(f"jobs did not finish: {[tuple(r) for r in states]}")


async def pages_of(pool, job_id):
    return await pool.fetch("SELECT * FROM pages WHERE job_id = $1 ORDER BY idx", job_id)


async def new_upload(conn, storage=None, pdf: Path | None = None, pages: int = 1) -> uuid.UUID:
    """An uploaded (not yet split) job; its split message sits in the outbox."""
    await repo.ensure_client(conn, "acme")
    job_id = uuid.uuid4()
    if storage is not None and pdf is not None:
        storage.files[f"uploads/{job_id}"] = pdf.read_bytes()
    await repo.create_job(conn, job_id=job_id, client_id="acme", content_type="application/pdf",
                          object_key=f"uploads/{job_id}", size_bytes=1, total_pages=pages)
    return job_id


async def test_pages_flow_fast_then_slow_exactly_once(pg_dsn, pool, queues):
    models = FakeModels()
    async with pool.acquire() as conn:
        jobs = [await create_split_job(conn, c, 8) for c in ("a", "b", "c")]
    async with Pipeline(pg_dsn, pool, queues, models) as p:
        await wait_complete(pool, jobs)
    for job in jobs:
        rows = await pages_of(pool, job)
        assert {(r["status"], r["source"]) for r in rows} == {("COMPLETED", "vlm")}
        for r in rows:
            for name in ("fast", "slow", "final"):
                assert result_key(job, r["idx"], name) in p.storage.files  # deterministic S3 paths
    assert models.calls == {"layout": 24, "vlm": 24}
    assert set(models.pages.values()) == {1}  # every page called each model exactly once
    per_page = await pool.fetch("SELECT job_id, page_idx, count(*) FROM events WHERE page_idx IS NOT NULL "
                                "GROUP BY 1, 2")
    assert len(per_page) == 24 and {r["count"] for r in per_page} == {1}
    assert await pool.fetchval("SELECT count(*) FROM outbox_events WHERE sent_at IS NULL") == 0
    assert await queues.depth("fast") == await queues.depth("slow") == 0


async def test_slow_failures_fall_back_to_the_fast_result_and_go_to_the_dlq(pg_dsn, pool, queues):
    models = FakeModels()
    models.status["vlm"] = 503
    async with pool.acquire() as conn:
        job = await create_split_job(conn, "acme", 4)
    async with Pipeline(pg_dsn, pool, queues, models):
        await wait_complete(pool, [job])
    rows = await pages_of(pool, job)
    assert {(r["status"], r["source"], r["low_confidence"]) for r in rows} == {
        ("COMPLETED", "layout_fallback", True)}
    assert {r["slow_attempts"] for r in rows} == {2}  # 3 calls: 2 retries, then give up
    assert await queues.depth("slow-dlq") == 4


async def test_retry_budget_defers_retries_without_calling_the_model(pg_dsn, pool, queues, monkeypatch):
    """Retries never exceed the budget; the ones over it wait without calling the model, and
    no page is dropped."""
    monkeypatch.setattr(ctl, "WINDOW_S", 2)  # short windows so the budget refills quickly
    models = FakeModels()
    models.status["vlm"] = 503
    async with pool.acquire() as conn:
        job = await create_split_job(conn, "acme", 10)
    async with Pipeline(pg_dsn, pool, queues, models) as p:
        await asyncio.sleep(3)
        del models.status["vlm"]  # the model recovers
        await wait_complete(pool, [job])
        windows = {}
        for key in await p.redis.keys("win:slow:*"):
            raw = {k.decode(): int(v) for k, v in (await p.redis.hgetall(key)).items()}
            windows[int(key.decode().rsplit(":", 1)[1])] = raw
    first = sum(w.get("first", 0) for w in windows.values())
    retries = sum(w.get("retries", 0) for w in windows.values())
    assert models.calls["vlm"] == first + retries  # every call was a first attempt or a reserved retry
    for wid, w in windows.items():  # per two-window budget: max(5, 10% of first attempts)
        prev = windows.get(wid - 1, {})
        assert w.get("retries", 0) + prev.get("retries", 0) <= max(5, 0.1 * (w.get("first", 0) + prev.get("first", 0)))
    deferred = await pool.fetchval("SELECT count(*) FROM outbox_events WHERE queue = 'slow' AND delay_s = 1")
    assert deferred > 0  # some retries had to wait for the budget
    assert {r["status"] for r in await pages_of(pool, job)} == {"COMPLETED"}  # nothing dropped


async def test_throttled_calls_back_off_without_using_attempts(pg_dsn, pool, queues):
    models = FakeModels()
    models.first["layout"] = (429, 5)
    async with pool.acquire() as conn:
        job = await create_split_job(conn, "acme", 4)
    async with Pipeline(pg_dsn, pool, queues, models) as p:
        await wait_complete(pool, [job])
        frozen = await p.redis.hget("bucket:fast", "frozen_until")
    rows = await pages_of(pool, job)
    assert {r["status"] for r in rows} == {"COMPLETED"} and {r["fast_attempts"] for r in rows} == {0}
    assert frozen is not None  # the 429 froze the bucket for every call


async def test_auth_error_opens_the_circuit_without_using_attempts(pg_dsn, pool, queues):
    models = FakeModels()
    models.first["layout"] = (401, 1)
    async with pool.acquire() as conn:
        job = await create_split_job(conn, "acme", 1)
    async with Pipeline(pg_dsn, pool, queues, models) as p:
        await wait_complete(pool, [job])
        trips = await p.redis.hget("brk:layout", "trips")
    [row] = await pages_of(pool, job)
    assert (row["status"], row["fast_attempts"]) == ("COMPLETED", 0)
    assert int(trips) == 1


async def test_bad_input_fails_the_page_at_the_fast_stage(pg_dsn, pool, queues):
    models = FakeModels()
    models.status["layout"] = 400
    async with pool.acquire() as conn:
        job = await create_split_job(conn, "acme", 2)
    async with Pipeline(pg_dsn, pool, queues, models):
        await wait_complete(pool, [job])
    assert {r["status"] for r in await pages_of(pool, job)} == {"FAILED"}
    assert models.calls["vlm"] == 0 and models.calls["layout"] == 2  # permanent: no retries
    assert await queues.depth("fast-dlq") == 2
    kinds = [r["kind"] for r in await pool.fetch("SELECT kind FROM events WHERE job_id = $1 ORDER BY seq", job)]
    assert kinds[-1] == "job_complete"


async def test_message_of_a_crashed_worker_is_redelivered(pg_dsn, pool, queues):
    models = FakeModels()
    async with pool.acquire() as conn:
        job = await create_split_job(conn, "acme", 1)
    await publisher.publish_once(pool, queues)
    crashed = await queues.receive("fast", wait_s=2)  # taken by a worker that then died
    await queues.extend("fast", crashed, 1)  # (its visibility timeout, shortened for the test)
    async with Pipeline(pg_dsn, pool, queues, models):
        await wait_complete(pool, [job])
    [row] = await pages_of(pool, job)
    assert (row["status"], row["source"]) == ("COMPLETED", "vlm")


async def test_a_page_that_keeps_crashing_workers_is_given_up(pg_dsn, pool, queues):
    models = FakeModels()
    async with pool.acquire() as conn:
        job = await create_split_job(conn, "acme", 1)
    await publisher.publish_once(pool, queues)
    for _ in range(MAX_DELIVERIES):  # delivered three times, and each time the worker died
        await queues.extend("fast", await queues.receive("fast", wait_s=2), 0)
    async with Pipeline(pg_dsn, pool, queues, models):
        await wait_complete(pool, [job])
    [row] = await pages_of(pool, job)
    # The fast stage gave up without calling its model; the slow stage still produced a result.
    assert (row["status"], row["source"], row["has_fast_result"]) == ("COMPLETED", "vlm", False)
    assert models.calls["layout"] == 0 and "delivered 4 times" in row["last_error"]


async def test_duplicate_messages_are_processed_once(pg_dsn, pool, queues):
    async with pool.acquire() as conn:
        job = await create_split_job(conn, "acme", 5)
    await publisher.publish_once(pool, queues)
    await queues.send_batch("fast", [({"job_id": str(job), "idx": i}, 0) for i in range(5)])  # re-sent
    async with Pipeline(pg_dsn, pool, queues, FakeModels()):
        await wait_complete(pool, [job])
    assert await pool.fetchval("SELECT count(*) FROM outbox_events WHERE queue = 'slow'") == 5
    assert await pool.fetchval("SELECT count(*) FROM events WHERE job_id = $1 AND page_idx IS NOT NULL", job) == 5


async def test_uploaded_document_is_split_through_the_queue(pg_dsn, pool, queues, tmp_path):
    storage = FakeStorage()
    async with pool.acquire() as conn:
        job = await new_upload(conn, storage, make_pdf(tmp_path / "doc.pdf", 3), pages=3)
    async with Pipeline(pg_dsn, pool, queues, FakeModels(), storage=storage, splitter=True):
        await wait_complete(pool, [job])
    assert [r["status"] for r in await pages_of(pool, job)] == ["COMPLETED"] * 3
    assert all(f"pages/{job}/{i:04d}.pdf" in storage.files for i in range(3))


async def test_splitter_abandons_a_document_that_keeps_crashing_splitters(pg_dsn, pool, queues):
    async with pool.acquire() as conn:
        job = await new_upload(conn)
    await publisher.publish_once(pool, queues)
    for _ in range(MAX_DELIVERIES):
        await queues.extend("split", await queues.receive("split", wait_s=2), 0)
    async with Pipeline(pg_dsn, pool, queues, FakeModels(), splitter=True):
        await wait_complete(pool, [job])
    row = await pool.fetchrow("SELECT status, error FROM jobs WHERE id = $1", job)
    assert row["status"] == "FAILED" and "split abandoned" in row["error"]


@pytest.fixture
async def gateway_app(pg_dsn, pool):
    app = gateway.app
    app.state.pool = pool
    app.state.signals = JobSignals(pg_dsn)
    await app.state.signals.start()
    await asyncio.sleep(0.2)  # LISTEN connection up
    yield app
    await app.state.signals.stop()


async def test_stream_delivers_live_pages_in_order_and_resumes(pg_dsn, pool, queues, gateway_app):
    async with pool.acquire() as conn:
        job = await create_split_job(conn, "acme", 10)
    transport = httpx.ASGITransport(app=gateway_app)
    async with httpx.AsyncClient(transport=transport, base_url="http://gw") as http:
        async with Pipeline(pg_dsn, pool, queues, FakeModels()):
            asm = await asyncio.wait_for(stream(http, "http://gw", "acme", str(job)), 60)
        assert asm.complete and [p["page_index"] for p in asm.ordered] == list(range(10))
        denied = await http.get(f"/jobs/{job}/stream", headers={"X-Client-ID": "intruder"})
        assert denied.status_code == 404
        resumed = await http.get(f"/jobs/{job}/stream",
                                 headers={"X-Client-ID": "acme", "Last-Event-ID": str(asm.last_seq)})
        assert "id:" not in resumed.text
