"""Evaluation API: compare an extracted document tree with a ground-truth tree.

POST /evaluate
  {"predicted": <DocNode>, "ground_truth": <DocNode>, "iou_threshold": 0.5}

Both trees use the pipeline's output schema (a page result's ``tree``, or a ``document``
root whose children are ``page`` nodes). The response has CER / WER, bounding-box IoU and
tree edit distance (see vf_common.evaluation), plus how long each metric took.

Stateless and CPU-bound: the handler is a plain function, so FastAPI runs it in a worker
thread and the event loop stays free.
"""

from __future__ import annotations

import time

from fastapi import FastAPI, HTTPException
from prometheus_client import make_asgi_app
from pydantic import BaseModel, Field

from vf_common import evaluation as ev
from vf_common import metrics as m
from vf_common import tracing
from vf_common.config import get_settings
from vf_common.models import DocNode

m.setup_logging(get_settings().log_level)
app = FastAPI(title="visionforge-eval")
app.add_middleware(tracing.RequestIdMiddleware)
app.mount("/metrics", make_asgi_app())


class EvaluateRequest(BaseModel):
    predicted: DocNode
    ground_truth: DocNode
    iou_threshold: float = Field(0.5, gt=0, le=1)


@app.post("/evaluate")
def evaluate(req: EvaluateRequest) -> dict:
    predicted = req.predicted.model_dump(exclude_none=True)
    truth = req.ground_truth.model_dump(exclude_none=True)
    for name, tree in (("predicted", predicted), ("ground_truth", truth)):
        if ev.count_nodes(tree) > ev.MAX_TREE_NODES:
            m.EVALUATIONS.labels("too_large").inc()
            raise HTTPException(422, f"{name} has more than {ev.MAX_TREE_NODES} nodes")

    result, timings = {}, {}
    for name, fn in (("text", ev.text_metrics), ("bbox", lambda p, t: ev.bbox_metrics(p, t, req.iou_threshold)),
                     ("tree", ev.tree_metrics)):
        start = time.perf_counter()
        try:
            result[name] = fn(predicted, truth)
        except ev.TooComplex as exc:
            m.EVALUATIONS.labels("too_complex").inc()
            raise HTTPException(422, str(exc)) from exc
        took = time.perf_counter() - start
        m.EVAL_SECONDS.labels(name).observe(took)
        timings[name] = round(took * 1000, 3)
    m.EVALUATIONS.labels("ok").inc()
    return {**result, "timings_ms": timings}


@app.get("/healthz")
def healthz() -> dict:
    return {"ok": True}
