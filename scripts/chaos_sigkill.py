"""Crash-recovery chaos test: SIGKILL workers, the splitter and the outbox publisher mid-run.

Submits ``--jobs`` x ``--pages`` documents, streams them, and ``--kills`` times kills a
random one of slow-worker / fast-worker / splitter / outbox-publisher with SIGKILL, restarting it
a few seconds later. Then verifies:
  * every page reached a terminal state
  * no page produced more than one page event (no duplicate results)
  * the mock models executed each idempotency key successfully at most once
    (retries after a kill replayed the cached response instead of re-running inference)
  * every streaming client received every page exactly once, in order
"""

from __future__ import annotations

import argparse
import asyncio
import random
import sys
import time
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import asyncpg  # noqa: E402
import httpx  # noqa: E402

from scripts.common import MOCK_URLS, PG_DSN, container_ids, docker, fixture, stream, submit  # noqa: E402

VICTIMS = ("slow-worker", "fast-worker", "splitter", "outbox-publisher")
TERMINAL = ["COMPLETED", "FAILED"]


async def chaos_monkey(args, done: asyncio.Event, log: list[str]) -> None:
    rng = random.Random(args.seed)
    for n in range(args.kills):
        try:
            await asyncio.wait_for(done.wait(), rng.uniform(args.min_gap, args.max_gap))
            return  # everything finished before the next kill
        except TimeoutError:
            pass
        service = rng.choice(VICTIMS)
        ids = container_ids(service)
        if not ids:
            continue
        cid = rng.choice(ids)
        docker("kill", "-s", "KILL", cid)
        log.append(f"kill #{n + 1}: SIGKILL {service} ({cid[:12]})")
        print(log[-1])
        await asyncio.sleep(args.down_s)
        docker("start", cid)
        print(f"          restarted {service} ({cid[:12]})")


async def run(args) -> int:
    doc = fixture(args.pages)
    client_id = f"chaos-{uuid.uuid4().hex[:6]}"
    async with httpx.AsyncClient(limits=httpx.Limits(max_connections=100)) as http:
        for url in MOCK_URLS.values():
            (await http.post(f"{url}/admin/reset")).raise_for_status()
        job_ids = []
        for _ in range(args.jobs):
            resp = await submit(http, args.base_url, client_id, doc)
            resp.raise_for_status()
            job_ids.append(resp.json()["job_id"])
        print(f"submitted {len(job_ids)} x {args.pages} pages as {client_id}")

        started = time.monotonic()
        done = asyncio.Event()
        kills: list[str] = []
        monkey = asyncio.create_task(chaos_monkey(args, done, kills))
        results = await asyncio.gather(*(stream(http, args.base_url, client_id, j, timeout_s=args.timeout)
                                         for j in job_ids))
        done.set()
        await monkey
        elapsed = time.monotonic() - started
        stats = {m: (await http.get(f"{url}/admin/stats")).json() for m, url in MOCK_URLS.items()}

    ids = [uuid.UUID(j) for j in job_ids]
    conn = await asyncpg.connect(PG_DSN)
    try:
        unfinished = await conn.fetchval(
            "SELECT count(*) FROM pages WHERE job_id = ANY($1) AND status <> ALL($2::text[])", ids, TERMINAL)
        dup_events = await conn.fetch(
            "SELECT job_id, page_idx, count(*) AS n FROM events WHERE job_id = ANY($1) AND page_idx IS NOT NULL "
            "GROUP BY 1, 2 HAVING count(*) > 1", ids)
        states = dict(await conn.fetch(
            "SELECT status, count(*) FROM pages WHERE job_id = ANY($1) GROUP BY 1", ids))
    finally:
        await conn.close()

    expected = list(range(args.pages))
    checks = {
        f"all pages terminal ({unfinished} unfinished)": unfinished == 0,
        f"no duplicate page events ({len(dup_events)} duplicates)": not dup_events,
        "every client stream received every page once, in order":
            all([p["page_index"] for p in a.ordered] == expected and a.complete for a in results),
    }
    for model, st in stats.items():
        checks[f"{model}: no duplicate inference ({st['duplicate_executions']} keys executed twice, "
               f"{st['counters'].get('replays', 0)} replays, {st['counters'].get('inflight_joins', 0)} joins)"] = \
            st["duplicate_executions"] == 0

    print(f"\n{len(kills)} kills, {elapsed:.0f}s, page states {states}, "
          f"{sum(a.duplicates for a in results)} duplicate frames ignored by clients")
    for name, ok in checks.items():
        print(("PASS " if ok else "FAIL ") + name)
    return 0 if all(checks.values()) else 1


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--base-url", default="http://localhost:8080")
    ap.add_argument("--jobs", type=int, default=5)
    ap.add_argument("--pages", type=int, default=100)
    ap.add_argument("--kills", type=int, default=3)
    ap.add_argument("--min-gap", type=float, default=8)
    ap.add_argument("--max-gap", type=float, default=20)
    ap.add_argument("--down-s", type=float, default=3)
    ap.add_argument("--timeout", type=float, default=1800)
    ap.add_argument("--seed", type=int, default=None)
    raise SystemExit(asyncio.run(run(ap.parse_args())))


if __name__ == "__main__":
    main()
