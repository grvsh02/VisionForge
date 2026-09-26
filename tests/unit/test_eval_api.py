import copy
import random

from fastapi.testclient import TestClient

from services.eval_api.app import app
from vf_common import evaluation as ev

client = TestClient(app)


def page(n_blocks=49, seed=0):
    """A realistic page tree: 1 page node + n_blocks blocks (tables have cell children)."""
    rng = random.Random(seed)
    blocks, y = [], 40.0
    for i in range(n_blocks):
        blocks.append({"type": rng.choice(["title", "paragraph", "list", "kv"]) if i else "title", "order": i,
                       "text": " ".join(rng.choice(["total", "amount", "due", "invoice", "net"]) for _ in range(6)),
                       "bbox": {"x": 50.0, "y": y, "w": 400.0, "h": 12.0}})
        y += 15
    return {"type": "page", "children": blocks}


def test_identical_trees_score_perfectly():
    tree = page()
    r = client.post("/evaluate", json={"predicted": tree, "ground_truth": tree})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["text"]["cer"] == 0 and body["text"]["wer"] == 0
    assert body["bbox"]["mean_iou"] == 1 and body["bbox"]["f1"] == 1 and body["bbox"]["type_accuracy"] == 1
    assert body["tree"] == {"ted": 0, "ted_normalized": 0, "predicted_nodes": 50, "ground_truth_nodes": 50}
    assert set(body["timings_ms"]) == {"text", "bbox", "tree"}


def test_drift_shows_up_in_every_metric_and_a_50_node_tree_diffs_under_100ms():
    truth = page()
    pred = copy.deepcopy(truth)
    pred["children"][3]["text"] = pred["children"][3]["text"].replace("total", "tota1")  # OCR slip
    pred["children"][5]["type"] = "table"  # relabel
    pred["children"][5]["children"] = [{"type": "cell", "text": "x"}]  # extra nested node
    pred["children"][7]["bbox"]["x"] += 100  # box drift
    del pred["children"][9]  # missed block
    body = client.post("/evaluate", json={"predicted": pred, "ground_truth": truth}).json()
    assert 0 < body["text"]["cer"] < 0.1 and 0 < body["text"]["wer"] < 0.1
    assert body["bbox"]["ground_truth_boxes"] == 49 and body["bbox"]["predicted_boxes"] == 48
    assert body["bbox"]["recall"] < 1 and body["bbox"]["mean_iou"] < 1
    assert body["tree"]["ted"] == 3  # relabel + insert cell + delete block
    assert body["timings_ms"]["tree"] < 100


def test_invalid_bodies_are_rejected():
    assert client.post("/evaluate", json={"predicted": {"type": "page"}}).status_code == 422
    bad_box = {"type": "page", "bbox": {"x": 1}}
    assert client.post("/evaluate", json={"predicted": bad_box, "ground_truth": bad_box}).status_code == 422
    tree = page(3)
    assert client.post("/evaluate", json={"predicted": tree, "ground_truth": tree, "iou_threshold": 0}).status_code == 422


def test_oversized_and_pathological_trees_get_422_not_a_hang():
    big = {"type": "document", "children": [{"type": "p"} for _ in range(ev.MAX_TREE_NODES)]}
    r = client.post("/evaluate", json={"predicted": big, "ground_truth": page(3)})
    assert r.status_code == 422 and "nodes" in r.json()["detail"]
    zigzag = cur = {"type": "a", "children": []}
    for i in range(200):
        kid = {"type": "a", "children": []}
        cur["children"] = [kid, {"type": "b"}] if i % 2 else [{"type": "b"}, kid]
        cur = kid
    r = client.post("/evaluate", json={"predicted": zigzag, "ground_truth": zigzag})
    assert r.status_code == 422 and "steps" in r.json()["detail"]
