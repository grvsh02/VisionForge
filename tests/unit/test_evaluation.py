import random
import time
from functools import lru_cache

import pytest

from vf_common import evaluation as ev


def node(t, *children, text=None, bbox=None, order=None):
    n = {"type": t, "children": list(children)}
    if text is not None:
        n["text"] = text
    if bbox is not None:
        n["bbox"] = dict(zip("xywh", bbox))
    if order is not None:
        n["order"] = order
    return n


# --- Levenshtein ---------------------------------------------------------------------------

def dp_levenshtein(a, b):
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        cur = [i]
        for j, cb in enumerate(b, 1):
            cur.append(min(prev[j] + 1, cur[j - 1] + 1, prev[j - 1] + (ca != cb)))
        prev = cur
    return prev[-1]


@pytest.mark.parametrize("a,b,d", [("", "", 0), ("abc", "", 3), ("", "ab", 2), ("kitten", "sitting", 3),
                                   ("flaw", "lawn", 2), ("same", "same", 0)])
def test_levenshtein_known_values(a, b, d):
    assert ev.levenshtein(a, b) == d


def test_bit_parallel_levenshtein_matches_the_textbook_dp():
    rng = random.Random(7)
    for _ in range(500):
        a = "".join(rng.choice("abcd ") for _ in range(rng.randint(0, 90)))
        b = "".join(rng.choice("abcd ") for _ in range(rng.randint(0, 90)))
        assert ev.levenshtein(a, b) == dp_levenshtein(a, b), (a, b)
    words = [rng.choice(["the", "a", "total", "due"]) for _ in range(40)]
    other = words[:10] + ["x"] + words[12:]
    assert ev.levenshtein(words, other) == dp_levenshtein(words, other)


def test_cer_and_wer_follow_reading_order():
    truth = node("page", node("p", text="world", order=1), node("title", text="hello", order=0))
    pred = node("page", node("title", text="hello"), node("p", text="wordl"))
    m = ev.text_metrics(pred, truth)
    assert ev.text_stream(truth) == "hello world"
    assert m["char_edits"] == 2 and m["cer"] == pytest.approx(2 / 11)
    assert m["word_edits"] == 1 and m["wer"] == pytest.approx(1 / 2)


# --- IoU -------------------------------------------------------------------------------------

def test_iou_values():
    a = ev.Box(0, 0, 10, 10, "p", None)
    assert ev.iou(a, a) == 1.0
    assert ev.iou(a, ev.Box(5, 0, 10, 10, "p", None)) == pytest.approx(50 / 150)
    assert ev.iou(a, ev.Box(20, 20, 5, 5, "p", None)) == 0.0


def test_boxes_match_one_to_one_and_only_on_the_same_page():
    truth = node("document",
                 node("page", node("title", bbox=(0, 0, 100, 20)), node("p", bbox=(0, 30, 100, 50))),
                 node("page", node("p", bbox=(0, 0, 100, 20))))
    pred = node("document",
                node("page", node("title", bbox=(0, 0, 100, 20)), node("table", bbox=(0, 35, 100, 50)),
                     node("p", bbox=(0, 0, 100, 18))),  # duplicate of the title: unmatched
                node("page", node("p", bbox=(500, 500, 10, 10))))  # nothing like it on page 2
    m = ev.bbox_metrics(pred, truth)
    assert m["predicted_boxes"] == 4 and m["ground_truth_boxes"] == 3
    assert m["matches_at_threshold"] == 2
    assert m["precision"] == pytest.approx(2 / 4) and m["recall"] == pytest.approx(2 / 3)
    assert m["type_accuracy"] == pytest.approx(1 / 2)  # title/title yes, table/p no
    iou_p = 45 * 100 / (100 * 50 * 2 - 45 * 100)  # overlap 45 px of height
    assert m["mean_iou"] == pytest.approx((1 + iou_p) / 3)


# --- tree edit distance ----------------------------------------------------------------------

def to_tuple(n):
    return (n["type"], tuple(to_tuple(c) for c in ev._children(n)))


def size(t):
    return 1 + sum(size(c) for c in t[1])


@lru_cache(maxsize=None)
def forest_distance(f, g):
    """The recursive definition of ordered forest edit distance (exponential; small trees only)."""
    if not f:
        return sum(size(t) for t in g)
    if not g:
        return sum(size(t) for t in f)
    v, w = f[-1], g[-1]
    return min(forest_distance(f[:-1] + v[1], g) + 1,
               forest_distance(f, g[:-1] + w[1]) + 1,
               forest_distance(f[:-1], g[:-1]) + forest_distance(v[1], w[1]) + (v[0] != w[0]))


def random_tree(rng, n, labels="abc"):
    nodes = [node(rng.choice(labels))]
    for _ in range(n - 1):
        rng.choice(nodes)["children"].append(new := node(rng.choice(labels)))
        nodes.append(new)
    return nodes[0]


def test_classic_zhang_shasha_example():
    t1 = node("f", node("d", node("a"), node("c", node("b"))), node("e"))
    t2 = node("f", node("c", node("d", node("a"), node("b"))), node("e"))
    assert ev.tree_edit_distance(t1, t2) == 2
    assert ev.tree_edit_distance(t1, t1) == 0


def test_ted_matches_the_recursive_definition_on_random_trees():
    rng = random.Random(3)
    for _ in range(300):
        a, b = random_tree(rng, rng.randint(1, 7)), random_tree(rng, rng.randint(1, 7))
        assert ev.tree_edit_distance(a, b) == forest_distance((to_tuple(a),), (to_tuple(b),))


def test_ted_follows_reading_order_not_list_order():
    a = node("page", node("title", order=0), node("table", order=1))
    b = node("page", node("table", order=1), node("title", order=0))
    assert ev.tree_edit_distance(a, b) == 0


def test_deep_trees_do_not_hit_the_recursion_limit():
    chain = node("n")
    for _ in range(ev.MAX_TREE_NODES - 1):  # depth ~1000: a recursive walk would overflow
        chain = node("n", chain)
    assert ev.tree_edit_distance(chain, chain) == 0


def test_oversized_or_pathological_trees_are_refused_before_any_work():
    big = node("page", *[node("p") for _ in range(ev.MAX_TREE_NODES)])
    with pytest.raises(ev.TooComplex, match="limited to"):
        ev.tree_edit_distance(big, node("page"))
    zigzag = shapes(400)["zigzag"]
    start = time.perf_counter()
    with pytest.raises(ev.TooComplex, match="steps"):
        ev.tree_edit_distance(zigzag, zigzag)
    assert time.perf_counter() - start < 0.1


def test_mirroring_picks_the_cheaper_direction_without_changing_the_distance():
    rng = random.Random(11)
    for _ in range(100):
        a, b = random_tree(rng, rng.randint(1, 7)), random_tree(rng, rng.randint(1, 7))
        expected = forest_distance((to_tuple(a),), (to_tuple(b),))
        assert ev.tree_edit_distance(a, b) == expected
        mirrored = [ev._postorder(t, mirror=True) for t in (a, b)]
        assert len(mirrored[0][0]) == ev.count_nodes(a)


def shapes(n):
    rng = random.Random(n)
    chain = node("p")
    for _ in range(n - 1):
        chain = node("p", chain)
    star = node("page", *[node(rng.choice(["p", "title", "table"])) for _ in range(n - 1)])
    zigzag, cur = node("a"), None
    cur = zigzag
    for i in range(n // 2 - 1):  # alternating left/right spines: Zhang-Shasha's bad case
        kid = node("a")
        cur["children"] = [kid, node("b")] if i % 2 else [node("b"), kid]
        cur = kid
    return {"chain": chain, "star": star, "random": random_tree(rng, n, "pqrst"), "zigzag": zigzag}


def test_tree_diff_of_a_50_node_document_takes_under_100ms():
    trees = shapes(50)
    for name, a in trees.items():
        for other, b in trees.items():
            start = time.perf_counter()
            ev.tree_edit_distance(a, b)
            took = time.perf_counter() - start
            assert took < 0.1, f"{name} vs {other}: {took * 1000:.1f} ms"
