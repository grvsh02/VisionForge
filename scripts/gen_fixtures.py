"""Generate fixture documents under ./fixtures.

  python scripts/gen_fixtures.py                 # doc5, doc20, doc100 (text only) + a 3-page TIFF
  python scripts/gen_fixtures.py --pages 100 --page-kb 1000   # ~100MB, 100-page PDF
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from PIL import Image  # noqa: E402

from scripts.common import ROOT, make_pdf  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pages", type=int, nargs="*", default=[5, 20, 100])
    ap.add_argument("--page-kb", type=int, default=0, help="pad each page with N KB of image data")
    args = ap.parse_args()
    out = ROOT / "fixtures"
    for pages in args.pages:
        suffix = f"_{args.page_kb}kb" if args.page_kb else ""
        path = make_pdf(out / f"doc{pages}{suffix}.pdf", pages, args.page_kb)
        print(f"{path} ({path.stat().st_size / 2**20:.1f} MiB)")
    frames = [Image.new("L", (850, 1100), color=c) for c in (255, 200, 150)]
    tiff = out / "scan3.tiff"
    frames[0].save(tiff, save_all=True, append_images=frames[1:])
    print(tiff)


if __name__ == "__main__":
    main()
