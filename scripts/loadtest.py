"""Sustained-load test: many jobs across many clients, with container memory sampling.

  python scripts/loadtest.py                                   # 40 jobs x 50 pages, 10 clients
  python scripts/loadtest.py --jobs 200 --pages 100 --page-kb 1000   # the full plan run (~35 min)

Asserts every page finished and every VisionForge container's memory is flat after
warm-up (slope below --max-slope MiB/min) and under its peak budget.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import shutil
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import httpx  # noqa: E402

from scripts.common import ROOT, fixture, memory_report, sample_docker_memory, stream, submit  # noqa: E402

SERVICES = ("ingest-api", "outbox-publisher", "splitter", "fast-worker", "slow-worker", "stream-gateway")


async def run(args) -> int:
    doc = fixture(args.pages, args.page_kb)
    print(f"fixture {doc.name}: {doc.stat().st_size / 2**20:.1f} MiB")
    samples: dict = {}
    stop = asyncio.Event()
    sampler = asyncio.create_task(sample_docker_memory(stop, samples)) if shutil.which("docker") else None

    started = time.monotonic()
    sem = asyncio.Semaphore(args.submit_concurrency)
    rejected = 0

    async with httpx.AsyncClient(limits=httpx.Limits(max_connections=args.jobs + 20)) as http:
        async def one(i: int):
            nonlocal rejected
            client_id = f"load-{i % args.clients}"
            async with sem:
                while True:
                    resp = await submit(http, args.base_url, client_id, doc)
                    if resp.status_code != 429:
                        break
                    rejected += 1
                    await asyncio.sleep(float(resp.headers.get("Retry-After", "5")))
                resp.raise_for_status()
            return await stream(http, args.base_url, client_id, resp.json()["job_id"])

        results = await asyncio.gather(*(one(i) for i in range(args.jobs)))

    elapsed = time.monotonic() - started
    stop.set()
    if sampler:
        await sampler

    pages = sum(len(a.ordered) for a in results)
    fallback = sum(1 for a in results for p in a.ordered if p["low_confidence"])
    in_order = all([p["page_index"] for p in a.ordered] == list(range(args.pages)) for a in results)
    report = {
        "jobs": args.jobs, "pages": pages, "elapsed_s": round(elapsed, 1),
        "pages_per_s": round(pages / elapsed, 2), "fallback_pages": fallback, "ingest_429s": rejected,
        "all_in_order": in_order, "out_of_order_arrivals": sum(a.out_of_order for a in results),
        "memory": [r for r in memory_report(samples) if any(s in r["container"] for s in SERVICES)],
    }
    print(json.dumps(report, indent=2))
    (ROOT / "reports").mkdir(exist_ok=True)
    (ROOT / "reports" / "loadtest.json").write_text(json.dumps(report, indent=2))

    failures = []
    if pages != args.jobs * args.pages or not in_order:
        failures.append("not every page was delivered in order")
    for row in report["memory"]:
        if row["slope_mib_per_min"] > args.max_slope:
            failures.append(f"{row['container']} memory still growing ({row['slope_mib_per_min']} MiB/min)")
        if row["peak_mib"] > args.max_peak_mib:
            failures.append(f"{row['container']} peak {row['peak_mib']} MiB > {args.max_peak_mib}")
    for f in failures:
        print("FAIL:", f)
    print("PASS" if not failures else f"{len(failures)} failure(s)")
    return 1 if failures else 0


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--base-url", default="http://localhost:8080")
    ap.add_argument("--jobs", type=int, default=40)
    ap.add_argument("--clients", type=int, default=10)
    ap.add_argument("--pages", type=int, default=50)
    ap.add_argument("--page-kb", type=int, default=200)
    ap.add_argument("--submit-concurrency", type=int, default=8)
    ap.add_argument("--max-slope", type=float, default=5.0, help="MiB/min after warm-up")
    ap.add_argument("--max-peak-mib", type=float, default=300.0)
    raise SystemExit(asyncio.run(run(ap.parse_args())))


if __name__ == "__main__":
    main()
