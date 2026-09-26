import asyncio
import json
import logging

from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from fastapi.testclient import TestClient

from vf_common import envelope, tracing
from vf_common.metrics import JsonFormatter

app = FastAPI()
app.add_middleware(tracing.RequestIdMiddleware)


@app.get("/rid")
def rid() -> dict:
    return {"request_id": tracing.request_id()}


@app.get("/stream")
def stream() -> StreamingResponse:
    return StreamingResponse(iter([b"data: 1\n\n", b"data: 2\n\n"]), media_type="text/event-stream")


client = TestClient(app)


def test_a_well_formed_client_id_is_kept_and_echoed():
    r = client.get("/rid", headers={"X-Request-ID": "req-123.abc"})
    assert r.headers["X-Request-ID"] == "req-123.abc" and r.json()["request_id"] == "req-123.abc"


def test_a_missing_or_unsafe_id_is_replaced():
    generated = client.get("/rid")
    assert len(generated.headers["X-Request-ID"]) == 32
    assert generated.json()["request_id"] == generated.headers["X-Request-ID"]
    for bad in ("has space", "x" * 200, "semi;colon"):
        r = client.get("/rid", headers={"X-Request-ID": bad})
        assert r.headers["X-Request-ID"] != bad and len(r.headers["X-Request-ID"]) == 32


def test_streaming_responses_get_the_header_and_their_body_untouched():
    r = client.get("/stream", headers={"X-Request-ID": "sse-1"})
    assert r.headers["X-Request-ID"] == "sse-1" and r.text == "data: 1\n\ndata: 2\n\n"


def test_bound_fields_nest_skip_none_and_reset():
    assert tracing.current() == {}
    with tracing.bound(request_id="r1", job_id="j1", page=None):
        with tracing.bound(page=3):
            assert tracing.current() == {"request_id": "r1", "job_id": "j1", "page": 3}
            assert tracing.headers() == {"X-Request-ID": "r1"}
        assert tracing.current() == {"request_id": "r1", "job_id": "j1"}
    assert tracing.current() == {} and tracing.headers() == {}


async def test_concurrent_tasks_keep_their_own_ids():
    seen = {}

    async def page(rid):
        with tracing.bound(request_id=rid):
            await asyncio.sleep(0.01)  # interleave with the other task
            seen[rid] = tracing.request_id()

    await asyncio.gather(page("a"), page("b"))
    assert seen == {"a": "a", "b": "b"} and tracing.request_id() is None


def test_json_log_lines_carry_the_bound_fields():
    record = logging.LogRecord("worker", logging.INFO, __file__, 1, "model call", None, None)
    record.fields = {"outcome": "ok"}
    with tracing.bound(request_id="r9", job_id="j9", page=2):
        line = json.loads(JsonFormatter().format(record))
    assert line["request_id"] == "r9" and line["job_id"] == "j9" and line["page"] == 2 and line["outcome"] == "ok"


def test_stream_header_carries_the_request_id():
    h = envelope.build_header(job_id="j", client_id="c", seq=1, page_index=None, total_pages=1,
                              done_pages=[], request_id="r1")
    assert h["request_id"] == "r1"
