import io
from pathlib import Path

import pikepdf
import pytest
from PIL import Image

from services.mock_models.app import synth_layout, synth_vlm
from services.splitter.main import iter_image_pages, iter_pdf_pages
from services.worker import stages
from vf_common.documents import InvalidDocument, count_pages


def make_pdf(path: Path, pages: int) -> None:
    pdf = pikepdf.new()
    for i in range(pages):
        pdf.add_blank_page(page_size=(612, 792))
        pdf.pages[-1].Contents = pdf.make_stream(f"BT /F1 12 Tf 72 720 Td (page {i}) Tj ET".encode())
    pdf.save(path)


def test_pdf_split_yields_single_page_pdfs(tmp_path):
    src = tmp_path / "doc.pdf"
    make_pdf(src, 5)
    parts = list(iter_pdf_pages(src, max_pages=100))
    assert len(parts) == 5
    for i, data in enumerate(parts):
        with pikepdf.open(io.BytesIO(data)) as one:
            assert len(one.pages) == 1
            assert f"page {i}".encode() in one.pages[0].Contents.read_bytes()


def test_pdf_split_memory_is_bounded_by_a_few_pages(tmp_path):
    """qpdf caches streams per open document; the splitter must not hold the whole file."""
    import psutil

    from scripts.common import make_pdf as make_padded_pdf

    src = make_padded_pdf(tmp_path / "padded.pdf", pages=40, page_kb=512)  # ~20 MiB
    proc = psutil.Process()
    base = peak = proc.memory_info().rss
    for _ in iter_pdf_pages(src, max_pages=100):
        peak = max(peak, proc.memory_info().rss)
    assert (peak - base) / 2**20 < 12, f"splitter grew {(peak - base) / 2**20:.1f} MiB"


def test_pdf_split_enforces_page_limit(tmp_path):
    src = tmp_path / "big.pdf"
    make_pdf(src, 3)
    with pytest.raises(InvalidDocument, match="3 pages exceeds the limit of 2"):
        next(iter_pdf_pages(src, max_pages=2))


def test_count_pages_for_pdf_tiff_and_garbage(tmp_path):
    pdf, tiff, junk = tmp_path / "d.pdf", tmp_path / "d.tiff", tmp_path / "junk.pdf"
    make_pdf(pdf, 7)
    frames = [Image.new("L", (10, 10)) for _ in range(4)]
    frames[0].save(tiff, save_all=True, append_images=frames[1:])
    junk.write_bytes(b"not a pdf at all")
    assert count_pages(pdf, "application/pdf", 100) == 7
    assert count_pages(tiff, "image/tiff", 100) == 4
    with pytest.raises(InvalidDocument, match="could not read"):
        count_pages(junk, "application/pdf", 100)


def test_multipage_tiff_split(tmp_path):
    src = tmp_path / "doc.tiff"
    frames = [Image.new("L", (50, 60), color=c) for c in (0, 128, 255)]
    frames[0].save(src, save_all=True, append_images=frames[1:])
    assert len(list(iter_image_pages(src, max_pages=10))) == 3


def test_idempotency_key_is_stable_per_model_and_version(monkeypatch):
    assert stages.idempotency_key("j", 1, "vlm") == stages.idempotency_key("j", 1, "vlm")
    assert stages.idempotency_key("j", 1, "vlm") != stages.idempotency_key("j", 1, "layout")
    before = stages.idempotency_key("j", 1, "vlm")
    monkeypatch.setattr(stages, "MODEL_VERSION", "v2")
    assert stages.idempotency_key("j", 1, "vlm") != before  # a new model version is a new call


def test_results_from_mock_outputs():
    layout = synth_layout(b"page-bytes")
    assert synth_layout(b"page-bytes") == layout  # deterministic
    vlm = synth_vlm(b"page-bytes", layout)
    ok = stages.vlm_result(3, vlm)
    assert ok["source"] == "vlm" and not ok["low_confidence"] and ok["tree"]["order"] == 3
    assert len(ok["tree"]["children"]) == len(layout["blocks"])
    fb = stages.fallback_result(3, layout, "retries exhausted")
    assert fb["low_confidence"] and fb["confidence"] <= stages.FALLBACK_CONFIDENCE_CAP
    assert fb["source"] == "layout_fallback" and fb["error"] == "retries exhausted"
