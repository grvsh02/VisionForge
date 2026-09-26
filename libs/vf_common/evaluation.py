"""Evaluation metrics: compare an extracted document tree against a ground-truth tree.

Both trees use the pipeline's output schema (``DocNode``: type, text, bbox, order, children).

  text   CER and WER: Levenshtein edit distance over the reading-order text stream,
         divided by the ground-truth length (characters / whitespace-separated words).
  bbox   IoU: every predicted box is paired with at most one ground-truth box on the same
         page, greedily by highest IoU (the usual object-detection matching). Reports mean
         IoU (missed ground-truth boxes count as 0), precision / recall / F1 at a threshold,
         and how often matched boxes agree on node type.
  tree   Tree edit distance (Zhang-Shasha) on node types: insert, delete and relabel cost 1.
         Memory is two (n1+1) x (n2+1) tables, allocated once, and trees are capped at
         MAX_TREE_NODES nodes. Time is bounded too: Zhang-Shasha's work is known before it
         runs (cells = keyroot cost of tree 1 x keyroot cost of tree 2), so both trees are
         mirrored when that is cheaper (a simplified APTED path choice), and a comparison
         over MAX_TED_CELLS is refused instead of run.

Reading order: pre-order traversal, children sorted by ``order`` (nodes without one keep
their list position after the ordered ones).
"""

from __future__ import annotations

from collections.abc import Hashable, Sequence
from dataclasses import dataclass

MAX_TREE_NODES = 1000  # TED memory is two n1 x n2 tables
MAX_TED_CELLS = 20_000_000  # about 2 s of work; typical 50-node pages need well under 1 ms


class TooComplex(ValueError):
    """The tree comparison would exceed the node or work budget."""


# --- tree helpers -----------------------------------------------------------------------

def _children(node: dict, mirror: bool = False) -> list[dict]:
    kids = node.get("children") or []
    indexed = list(enumerate(kids))
    indexed.sort(key=lambda p: (p[1].get("order") is None, p[1].get("order") or 0, p[0]))
    ordered = [k for _, k in indexed]
    return ordered[::-1] if mirror else ordered


def count_nodes(root: dict) -> int:
    n, stack = 0, [root]
    while stack:
        node = stack.pop()
        n += 1
        stack.extend(node.get("children") or [])
    return n


def preorder(root: dict) -> list[tuple[dict, int | None]]:
    """(node, page number) in reading order; the page is the nearest ``page`` ancestor's position."""
    out, stack = [], [(root, None)]
    page_counter = 0
    while stack:
        node, page = stack.pop()
        if node.get("type") == "page":
            page = page_counter
            page_counter += 1
        out.append((node, page))
        stack.extend((child, page) for child in reversed(_children(node)))
    return out


# --- text: CER / WER --------------------------------------------------------------------

def levenshtein(a: Sequence[Hashable], b: Sequence[Hashable]) -> int:
    """Edit distance (insert / delete / substitute, cost 1), bit-parallel (Myers / Hyyro).

    Each column of the DP matrix is a pair of bit vectors held in Python ints, so the
    cost is O(len(b)) big-int operations over len(a) bits instead of len(a) * len(b)
    Python-level steps.
    """
    if len(a) < len(b):
        a, b = b, a
    m = len(a)
    if m == 0 or not b:
        return m or len(b)
    peq: dict[Hashable, int] = {}
    for i, c in enumerate(a):
        peq[c] = peq.get(c, 0) | (1 << i)
    full, top = (1 << m) - 1, 1 << (m - 1)
    vp, vn, score = full, 0, m
    for c in b:
        eq = peq.get(c, 0)
        xv = eq | vn
        xh = (((eq & vp) + vp) ^ vp) | eq
        hp = (vn | ~(xh | vp)) & full
        hn = vp & xh
        if hp & top:
            score += 1
        elif hn & top:
            score -= 1
        hp = ((hp << 1) | 1) & full
        hn = (hn << 1) & full
        vp = (hn | ~(xv | hp)) & full
        vn = hp & xv
    return score


def text_stream(root: dict) -> str:
    texts = [node["text"] for node, _ in preorder(root) if node.get("text")]
    return " ".join(" ".join(texts).split())  # collapse whitespace, keep case


def text_metrics(predicted: dict, truth: dict) -> dict:
    hyp, ref = text_stream(predicted), text_stream(truth)
    char_edits = levenshtein(ref, hyp)
    hyp_words, ref_words = hyp.split(), ref.split()
    word_edits = levenshtein(ref_words, hyp_words)
    return {"cer": char_edits / max(len(ref), 1), "wer": word_edits / max(len(ref_words), 1),
            "char_edits": char_edits, "ref_chars": len(ref), "hyp_chars": len(hyp),
            "word_edits": word_edits, "ref_words": len(ref_words), "hyp_words": len(hyp_words)}


# --- boxes: IoU ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Box:
    x: float
    y: float
    w: float
    h: float
    type: str
    page: int | None


def iou(a: Box, b: Box) -> float:
    ix = max(0.0, min(a.x + a.w, b.x + b.w) - max(a.x, b.x))
    iy = max(0.0, min(a.y + a.h, b.y + b.h) - max(a.y, b.y))
    inter = ix * iy
    union = a.w * a.h + b.w * b.h - inter
    return inter / union if union > 0 else 0.0


def boxes(root: dict) -> list[Box]:
    out = []
    for node, page in preorder(root):
        bb = node.get("bbox")
        if bb:
            out.append(Box(bb["x"], bb["y"], bb["w"], bb["h"], node.get("type", ""), page))
    return out


def bbox_metrics(predicted: dict, truth: dict, threshold: float = 0.5) -> dict:
    pred, gt = boxes(predicted), boxes(truth)
    pairs = []  # (iou, pred index, gt index), same page only
    by_page: dict[int | None, list[int]] = {}
    for j, g in enumerate(gt):
        by_page.setdefault(g.page, []).append(j)
    for i, p in enumerate(pred):
        for j in by_page.get(p.page, ()):
            v = iou(p, gt[j])
            if v > 0:
                pairs.append((v, i, j))
    pairs.sort(key=lambda t: t[0], reverse=True)
    used_p, used_g, matched = set(), set(), []
    for v, i, j in pairs:
        if i not in used_p and j not in used_g:
            used_p.add(i)
            used_g.add(j)
            matched.append((v, i, j))
    hits = [(v, i, j) for v, i, j in matched if v >= threshold]
    precision = len(hits) / len(pred) if pred else (1.0 if not gt else 0.0)
    recall = len(hits) / len(gt) if gt else (1.0 if not pred else 0.0)
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return {
        "iou_threshold": threshold,
        "predicted_boxes": len(pred), "ground_truth_boxes": len(gt),
        "mean_iou": sum(v for v, _, _ in matched) / len(gt) if gt else (1.0 if not pred else 0.0),
        "matched_mean_iou": sum(v for v, _, _ in matched) / len(matched) if matched else 0.0,
        "matches_at_threshold": len(hits),
        "precision": precision, "recall": recall, "f1": f1,
        "type_accuracy": (sum(pred[i].type == gt[j].type for _, i, j in hits) / len(hits)) if hits else 0.0,
    }


# --- tree: Zhang-Shasha tree edit distance -------------------------------------------------

def _postorder(root: dict, mirror: bool = False) -> tuple[list[str], list[int], list[int]]:
    """Node labels in post-order, each node's leftmost leaf descendant, and the keyroots."""
    labels: list[str] = []
    lmld: list[int] = []
    stack: list[tuple[dict, int, int | None]] = [(root, 0, None)]  # node, next child, lmld so far
    kids_cache: dict[int, list[dict]] = {}
    while stack:
        node, k, first = stack.pop()
        kids = kids_cache.setdefault(id(node), _children(node, mirror))
        if k < len(kids):
            stack.append((node, k + 1, first))
            stack.append((kids[k], 0, None))
            continue
        idx = len(labels)
        labels.append(node.get("type", ""))
        leftmost = idx if first is None else first
        lmld.append(leftmost)
        kids_cache.pop(id(node), None)
        if stack:  # tell the parent its first child's leftmost leaf
            parent, pk, pfirst = stack[-1]
            if pfirst is None:
                stack[-1] = (parent, pk, leftmost)
    last: dict[int, int] = {}
    for i, l in enumerate(lmld):
        last[l] = i
    return labels, lmld, sorted(last.values())


def _keyroot_cost(lmld: list[int], keyroots: list[int]) -> int:
    return sum(k - lmld[k] + 1 for k in keyroots)


def tree_edit_distance(a: dict, b: dict, max_cells: int = MAX_TED_CELLS) -> int:
    """Zhang-Shasha, run on the orientation (as given, or both trees mirrored) that needs
    fewer DP cells; the distance is the same either way. Raises TooComplex over budget."""
    for tree in (a, b):
        if count_nodes(tree) > MAX_TREE_NODES:
            raise TooComplex(f"trees are limited to {MAX_TREE_NODES} nodes")
    options = []
    for mirror in (False, True):
        t1, t2 = _postorder(a, mirror), _postorder(b, mirror)
        options.append((_keyroot_cost(t1[1], t1[2]) * _keyroot_cost(t2[1], t2[2]), t1, t2))
    cells, (lab1, l1, kr1), (lab2, l2, kr2) = min(options, key=lambda o: o[0])
    if cells > max_cells:
        raise TooComplex(f"tree comparison needs {cells:,} steps (limit {max_cells:,}); "
                         "the trees are too deep and irregular")
    n1, n2 = len(lab1), len(lab2)
    td = [[0] * n2 for _ in range(n1)]
    fd = [[0] * (n2 + 1) for _ in range(n1 + 1)]  # reused for every keyroot pair
    for i in kr1:
        li = l1[i]
        rows = i - li + 2
        for j in kr2:
            lj = l2[j]
            cols = j - lj + 2
            fd[0][0] = 0
            for x in range(1, rows):
                fd[x][0] = x
            row0 = fd[0]
            for y in range(1, cols):
                row0[y] = y
            for x in range(1, rows):
                xi = li + x - 1
                lxi, labx = l1[xi], lab1[xi]
                prev, cur = fd[x - 1], fd[x]
                tdx = td[xi]
                if lxi == li:  # xi's subtree spans the whole forest's left edge
                    for y in range(1, cols):
                        yj = lj + y - 1
                        if l2[yj] == lj:
                            d = min(prev[y] + 1, cur[y - 1] + 1, prev[y - 1] + (labx != lab2[yj]))
                            cur[y] = d
                            tdx[yj] = d
                        else:
                            cur[y] = min(prev[y] + 1, cur[y - 1] + 1, fd[0][l2[yj] - lj] + tdx[yj])
                else:
                    base = fd[lxi - li]
                    for y in range(1, cols):
                        yj = lj + y - 1
                        cur[y] = min(prev[y] + 1, cur[y - 1] + 1, base[l2[yj] - lj] + tdx[yj])
    return td[n1 - 1][n2 - 1]


def tree_metrics(predicted: dict, truth: dict) -> dict:
    ted = tree_edit_distance(predicted, truth)
    n_pred, n_truth = count_nodes(predicted), count_nodes(truth)
    return {"ted": ted, "ted_normalized": ted / max(n_pred, n_truth),
            "predicted_nodes": n_pred, "ground_truth_nodes": n_truth}
