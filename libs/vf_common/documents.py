"""Page counting shared by the ingest API (at upload) and the splitter."""

from __future__ import annotations

from pathlib import Path

import pikepdf
from PIL import Image, UnidentifiedImageError


class InvalidDocument(ValueError):
    """The upload is not a readable document, has no pages, or has too many."""


def count_pages(path: Path, content_type: str, max_pages: int) -> int:
    """Return the page count, raising ``InvalidDocument`` for bad or oversized documents.

    Cheap in memory: pikepdf reads only the cross-reference table and page tree (not page
    content), and PIL reads only the TIFF frame directory.
    """
    try:
        if content_type == "application/pdf":
            with pikepdf.open(path) as pdf:
                count = len(pdf.pages)
        else:
            with Image.open(path) as img:
                count = getattr(img, "n_frames", 1)
    except (pikepdf.PdfError, UnidentifiedImageError, OSError) as exc:
        raise InvalidDocument(f"could not read the document: {exc}") from exc
    if count == 0:
        raise InvalidDocument("document has no pages")
    if count > max_pages:
        raise InvalidDocument(f"{count} pages exceeds the limit of {max_pages}")
    return count
