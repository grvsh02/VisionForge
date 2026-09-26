"""End-to-end tests against a running stack (``make fast-up`` then ``make e2e``)."""

from __future__ import annotations

import asyncio
import os
import uuid

import httpx
import pytest
from redis.asyncio import Redis

from scripts.common import MOCK_URLS, REDIS_URL, fixture, stream, submit

BASE = os.environ.get("VF_E2E_BASE_URL")
pytestmark = [pytest.mark.e2e, pytest.mark.skipif(not BASE, reason="set VF_E2E_BASE_URL to run e2e tests")]


def cid(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:6]}"


@pytest.fixture
async def http():
    async with httpx.AsyncClient(limits=httpx.Limits(max_connections=100)) as client:
        yield client


@pytest.fixture
async def chaos(http):
    patches: list[str] = []

    async def apply(model: str, **patch):
        patches.append(model)
        (await http.post(f"{MOCK_URLS[model]}/admin/chaos", json=patch)).raise_for_status()

    yield apply
    for model in set(patches):
        await http.post(f"{MOCK_URLS[model]}/admin/reset")


async def run_jobs(http, client_id: str, doc, n: int):
    job_ids = []
    for _ in range(n):
        resp = await submit(http, BASE, client_id, doc)
        assert resp.status_code == 202, resp.text
        job_ids.append(resp.json()["job_id"])
    return await asyncio.gather(*(stream(http, BASE, client_id, j, timeout_s=900) for j in job_ids))


async def test_three_clients_out_of_order_stream_reassembles_in_order(http):
    doc = fixture(20)
    results = await asyncio.gather(*(run_jobs(http, cid(f"e2e{i}"), doc, 5) for i in range(3)))
    assemblers = [a for per_client in results for a in per_client]
    assert all(a.complete for a in assemblers)
    assert all([p["page_index"] for p in a.ordered] == list(range(20)) for a in assemblers)
    assert all(p["source"] in ("vlm", "layout_fallback") for a in assemblers for p in a.ordered)
    assert sum(a.out_of_order for a in assemblers) > 0, "expected some pages to finish out of order"


async def test_missing_client_id_is_rejected_and_jobs_are_client_scoped(http):
    doc = fixture(5)
    assert (await http.post(f"{BASE}/jobs", content=doc.read_bytes(),
                            headers={"Content-Type": "application/pdf"})).status_code == 400
    owner = cid("owner")
    job_id = (await submit(http, BASE, owner, doc)).json()["job_id"]
    other = await http.get(f"{BASE}/jobs/{job_id}", headers={"X-Client-ID": cid("intruder")})
    assert other.status_code == 404
    mine = await http.get(f"{BASE}/jobs/{job_id}", headers={"X-Client-ID": owner})
    assert mine.status_code == 200


async def test_resume_with_last_event_id_replays_only_newer_events(http):
    client_id = cid("resume")
    resp = await submit(http, BASE, client_id, fixture(5))
    job_id = resp.json()["job_id"]
    full = await stream(http, BASE, client_id, job_id)
    async with http.stream("GET", f"{BASE}/jobs/{job_id}/stream",
                           headers={"X-Client-ID": client_id, "Last-Event-ID": str(full.last_seq - 1)}) as r:
        ids = [line for line in [l async for l in r.aiter_lines()] if line.startswith("id:")]
    assert ids == [f"id: {full.last_seq}"]


async def test_status_and_result_endpoints_match_the_stream(http):
    client_id = cid("result")
    job_id = (await submit(http, BASE, client_id, fixture(5))).json()["job_id"]
    streamed = await stream(http, BASE, client_id, job_id)
    headers = {"X-Client-ID": client_id}
    status = (await http.get(f"{BASE}/jobs/{job_id}", headers=headers)).json()
    assert status["status"] == "COMPLETE" and status["pages"] == {"COMPLETED": 5}
    resp = await http.get(f"{BASE}/jobs/{job_id}/result", headers=headers)
    assert resp.status_code == 200, resp.text
    pages = resp.json()["pages"]
    assert [p["page_index"] for p in pages] == list(range(5))
    assert {p["state"] for p in pages} == {"COMPLETED"}
    assert [p["result"] for p in pages] == streamed.ordered  # same payloads as the stream


async def test_vlm_failures_fall_back_to_layout_with_low_confidence(http, chaos):
    await chaos("vlm", failure_rate=1.0)
    [asm] = await run_jobs(http, cid("fallback"), fixture(5), 1)
    assert asm.complete and len(asm.ordered) == 5
    assert all(p["source"] == "layout_fallback" and p["low_confidence"] for p in asm.ordered)
    assert all(p["confidence"] < 0.5 for p in asm.ordered)


async def test_backpressure_lowers_the_slow_rate_without_losing_pages(http, chaos):
    """A 5 RPS limit (429s) on the slow model: the controller halves its RPS after two
    overloaded windows, and every page still completes."""
    redis = Redis.from_url(REDIS_URL)
    await redis.delete("ctl:slow", "bucket:slow")  # start from the initial limits
    await chaos("vlm", rps_limit=5)
    lowest = 99.0

    async def watch():
        nonlocal lowest
        while True:
            rps = await redis.hget("ctl:slow", "rps")
            lowest = min(lowest, float(rps or 99))
            await asyncio.sleep(1)

    watcher = asyncio.create_task(watch())
    try:
        results = await run_jobs(http, cid("pressure"), fixture(100), 3)  # ~60 s of slow-stage work
    finally:
        watcher.cancel()
        await redis.aclose()
    assert all(a.complete and len(a.ordered) == 100 for a in results)
    assert lowest <= 4.0, f"controller never backed off (lowest rps {lowest})"


async def test_upload_counts_pages_and_rejects_bad_documents(http, tmp_path):
    client_id = cid("count")
    ok = await submit(http, BASE, client_id, fixture(20))
    assert ok.status_code == 202 and ok.json()["total_pages"] == 20

    junk = tmp_path / "junk.pdf"
    junk.write_bytes(b"%PDF-1.7 this is not really a pdf")
    bad = await submit(http, BASE, client_id, junk)
    assert bad.status_code == 422 and "could not read" in bad.text

    too_long = await submit(http, BASE, client_id, fixture(101))
    assert too_long.status_code == 422 and "101 pages exceeds the limit of 100" in too_long.text

    status = (await http.get(f"{BASE}/jobs/{ok.json()['job_id']}", headers={"X-Client-ID": client_id})).json()
    assert status["total_pages"] == 20  # stored at upload, before the splitter runs


async def test_evaluate_scores_a_real_page_tree(http):
    client_id = cid("eval")
    job_id = (await submit(http, BASE, client_id, fixture(5))).json()["job_id"]
    await stream(http, BASE, client_id, job_id)
    pages = (await http.get(f"{BASE}/jobs/{job_id}/result", headers={"X-Client-ID": client_id})).json()["pages"]
    tree = pages[0]["result"]["tree"]

    same = await http.post(f"{BASE}/evaluate", json={"predicted": tree, "ground_truth": tree})
    assert same.status_code == 200, same.text
    body = same.json()
    assert body["text"]["cer"] == 0 and body["bbox"]["f1"] == 1 and body["tree"]["ted"] == 0

    drifted = {**tree, "children": tree["children"][1:]}  # the model missed the first block
    body = (await http.post(f"{BASE}/evaluate", json={"predicted": drifted, "ground_truth": tree})).json()
    assert body["tree"]["ted"] == 1 and body["bbox"]["recall"] < 1 and body["text"]["cer"] > 0
