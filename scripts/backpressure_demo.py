"""Backpressure demo: watch the slow model's limits adapt to 429s and overload, and the slow
queue throttle the fast stage.

  uv run python scripts/backpressure_demo.py              # against the running stack

Submits enough pages to keep the slow model busy, then drives the mock VLM through four
phases via /admin/chaos:

  baseline   normal model                        -> slow RPS climbs from 8 towards the 10 RPS
                                                    hard limit (+1 per healthy window)
  429 storm  rate limit lowered to 5 RPS         -> 429s freeze the token bucket; after two
                                                    overloaded 10 s windows RPS and concurrency
                                                    are halved (and again while it lasts)
  overload   limit restored, capacity lowered:   -> p95 latency > 2x nominal: halved again,
             latency rises with our own traffic     which brings latency back down
  recovery   everything back to normal           -> after three healthy windows, +10% per window

Meanwhile the fast stage fills the slow queue, and the fast admission cap drops
100 -> 50 -> 20 -> paused as it grows (queue-level backpressure).

Every 2 s it prints the slow model's limits (Redis ``ctl:slow``), the last window's p95
latency, 429s/s from the mock, the slow queue depth (SQS), the resulting fast admission cap
and completed calls/s, then checks each phase reacted as expected and every page finished.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import asyncpg  # noqa: E402
import httpx  # noqa: E402
from redis.asyncio import Redis  # noqa: E402

from scripts.common import MOCK_URLS, PG_DSN, REDIS_URL, SQS_URL, fixture, submit  # noqa: E402
from vf_common.config import Settings  # noqa: E402
from vf_common.queues import Queues  # noqa: E402
from vf_common.ratelimit.controller import WINDOW_S, fast_admission_rps, p95  # noqa: E402

VLM = MOCK_URLS["vlm"]


@dataclass
class Phase:
    name: str
    seconds: float
    chaos: dict
    samples: list[dict] = field(default_factory=list)


async def snapshot(redis: Redis, queues: Queues) -> dict:
    rps, conc = await redis.hmget("ctl:slow", "rps", "concurrency")
    last_window = int(time.time() // WINDOW_S) - 1
    latencies = [float(x) for x in await redis.lrange(f"lat:slow:{last_window}", 0, -1)]
    depth = await queues.depth("slow")
    return {"rps": float(rps or 8), "conc": int(conc or 20), "p95": p95(latencies), "slow_queue": depth,
            "admission": fast_admission_rps(depth)}


async def run(args) -> int:
    redis = Redis.from_url(REDIS_URL)
    queues = await Queues(Settings(sqs_endpoint=SQS_URL)).start()
    # Start from fresh limits, as after a deploy, so earlier runs can't skew the baseline.
    await redis.delete("ctl:slow", "bucket:slow", "brk:vlm", "brk:vlm:win")
    client_id = f"bp-demo-{uuid.uuid4().hex[:6]}"
    doc = fixture(100)
    plan = [Phase("baseline", args.baseline, {}),
            Phase("429 storm", args.storm, {"rps_limit": args.storm_rps}),
            Phase("overload", args.overload, {"capacity": args.overload_capacity}),
            Phase("recovery", args.recovery, {})]
    async with httpx.AsyncClient(timeout=300) as http:
        await http.post(f"{VLM}/admin/reset")
        defaults = (await http.get(f"{VLM}/admin/stats")).json()["chaos"]
        job_ids = []
        for _ in range(args.jobs):
            resp = await submit(http, args.base_url, client_id, doc)
            resp.raise_for_status()
            job_ids.append(uuid.UUID(resp.json()["job_id"]))
        print(f"submitted {args.jobs} x 100 pages as {client_id}\n")
        print("   t  phase       slow rps  conc  p95(s)  429s/s  slow queue  fast admission  done/s")
        t0, prev = time.monotonic(), (await http.get(f"{VLM}/admin/stats")).json()["counters"]
        try:
            for phase in plan:
                await http.post(f"{VLM}/admin/chaos", json={**defaults, **phase.chaos})
                end = time.monotonic() + phase.seconds
                while time.monotonic() < end:
                    await asyncio.sleep(2)
                    now = (await http.get(f"{VLM}/admin/stats")).json()["counters"]
                    s = await snapshot(redis, queues)
                    s["429"] = (now.get("rate_limited", 0) - prev.get("rate_limited", 0)) / 2
                    s["done"] = (now.get("executions", 0) - prev.get("executions", 0)) / 2
                    prev = now
                    phase.samples.append(s)
                    admission = "paused" if s["admission"] == 0 else f"{s['admission']:.0f} rps"
                    print(f"{time.monotonic() - t0:4.0f}  {phase.name:<10} {s['rps']:8.1f} {s['conc']:5d}"
                          f"  {s['p95']:6.2f}  {s['429']:6.1f}  {s['slow_queue']:10d}  {admission:>14}"
                          f"  {s['done']:6.1f}")
        finally:
            await http.post(f"{VLM}/admin/chaos", json=defaults)
    await queues.close()

    print("\nwaiting for every page to finish ...")
    conn = await asyncpg.connect(PG_DSN)
    try:
        deadline = time.monotonic() + args.drain_timeout
        while time.monotonic() < deadline:
            left = await conn.fetchval("SELECT count(*) FROM pages WHERE job_id = ANY($1) AND status NOT IN "
                                       "('COMPLETED', 'FAILED')", job_ids)
            if left == 0:
                break
            await asyncio.sleep(2)
        states = dict(await conn.fetch("SELECT status, count(*) FROM pages WHERE job_id = ANY($1) GROUP BY 1",
                                       job_ids))
    finally:
        await conn.close()
        await redis.aclose()

    base, storm, over, rec = plan
    lowest = lambda ph: min(s["rps"] for s in ph.samples)  # noqa: E731
    min_admission = min(s["admission"] for ph in plan for s in ph.samples)
    checks = {
        f"baseline: rps rose towards the hard limit (max {max(s['rps'] for s in base.samples):.1f})":
            max(s["rps"] for s in base.samples) > base.samples[0]["rps"],
        f"429 storm: model returned 429s ({sum(s['429'] for s in storm.samples) * 2:.0f})":
            sum(s["429"] for s in storm.samples) > 0,
        f"429 storm: rps at most halved (lowest {lowest(storm):.1f} from {storm.samples[0]['rps']:.1f})":
            lowest(storm) <= storm.samples[0]["rps"] / 2,
        f"overload: p95 latency rose (peak {max(s['p95'] for s in over.samples):.1f}s)":
            max(s["p95"] for s in over.samples) > 6.0,
        f"overload: concurrency cut (lowest {min(s['conc'] for s in over.samples)} from {over.samples[0]['conc']})":
            min(s["conc"] for s in over.samples) < over.samples[0]["conc"],
        f"recovery: rps climbed back (end {rec.samples[-1]['rps']:.1f} from lowest {lowest(rec):.1f})":
            rec.samples[-1]["rps"] > lowest(rec),
        f"slow queue throttled the fast stage (lowest admission {min_admission:.0f} rps)": min_admission < 100,
        f"no page lost ({states})": set(states) == {"COMPLETED"} and sum(states.values()) == args.jobs * 100,
    }
    print()
    for name, ok in checks.items():
        print(("PASS " if ok else "FAIL ") + name)
    return 0 if all(checks.values()) else 1


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--base-url", default="http://localhost:8080")
    ap.add_argument("--jobs", type=int, default=20, help="100-page jobs to keep the slow model busy")
    ap.add_argument("--baseline", type=float, default=50)
    ap.add_argument("--storm", type=float, default=50)
    ap.add_argument("--storm-rps", type=float, default=5)
    ap.add_argument("--overload", type=float, default=50)
    ap.add_argument("--overload-capacity", type=float, default=2,
                    help="concurrent requests the mock serves at base latency (normally ~28)")
    ap.add_argument("--recovery", type=float, default=80)
    ap.add_argument("--drain-timeout", type=float, default=900)
    raise SystemExit(asyncio.run(run(ap.parse_args())))


if __name__ == "__main__":
    main()
