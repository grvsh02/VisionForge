"""Pure helpers for the page stages: result assembly and idempotency keys."""

from __future__ import annotations

import hashlib
from statistics import fmean

from vf_common.config import MODEL_VERSION
from vf_common.models import BBox, DocNode, PageResult

FALLBACK_CONFIDENCE_CAP = 0.49


def idempotency_key(job_id: str, page_idx: int, model: str) -> str:
    """``job:page:model:version`` (architecture doc, section 8). Stable across attempts and
    redeliveries, so a repeated call is served from the model's idempotency cache."""
    return hashlib.sha256(f"{job_id}:{page_idx}:{model}:{MODEL_VERSION}".encode()).hexdigest()


def _tree(page_idx: int, body: dict) -> DocNode:
    blocks = sorted(body.get("blocks", []), key=lambda b: (b.get("order", 0), b["bbox"]["y"], b["bbox"]["x"]))
    return DocNode(
        type="page",
        order=page_idx,
        bbox=BBox(x=0, y=0, w=body.get("page_width", 0), h=body.get("page_height", 0)),
        children=[
            DocNode(type=b["type"], text=b.get("text"), bbox=BBox(**b["bbox"]), order=b.get("order"),
                    confidence=b.get("confidence"))
            for b in blocks
        ],
    )


def vlm_result(page_idx: int, body: dict) -> dict:
    tree = _tree(page_idx, body)
    return PageResult(
        page_index=page_idx, source="vlm", confidence=body.get("confidence", 0.9), low_confidence=False,
        tree=tree, text="\n".join(c.text or "" for c in tree.children),
        tables_markdown=body.get("tables_markdown", []), key_values=body.get("key_values", {}),
    ).model_dump(mode="json")


def fallback_result(page_idx: int, layout_body: dict, reason: str) -> dict:
    tree = _tree(page_idx, layout_body)
    confs = [c.confidence for c in tree.children if c.confidence is not None]
    confidence = min(FALLBACK_CONFIDENCE_CAP, round(fmean(confs) * 0.6, 3)) if confs else 0.2
    return PageResult(
        page_index=page_idx, source="layout_fallback", confidence=confidence, low_confidence=True,
        tree=tree, text="\n".join(c.text or "" for c in tree.children), error=reason,
    ).model_dump(mode="json")


def failed_result(page_idx: int, reason: str) -> dict:
    return PageResult(
        page_index=page_idx, source="none", confidence=0.0, low_confidence=True,
        tree=DocNode(type="page", order=page_idx), error=reason,
    ).model_dump(mode="json")
