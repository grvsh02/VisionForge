"""asyncpg pool setup and a minimal, advisory-locked migration runner.

Run migrations with ``python -m vf_common.db migrate``.
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
from importlib import resources

import asyncpg

from vf_common.config import get_settings

log = logging.getLogger(__name__)

MIGRATION_LOCK_ID = 7_310_001


async def _init_connection(conn: asyncpg.Connection) -> None:
    for typ in ("json", "jsonb"):
        await conn.set_type_codec(typ, encoder=json.dumps, decoder=json.loads, schema="pg_catalog")


async def create_pool(dsn: str | None = None, min_size: int = 1, max_size: int = 10) -> asyncpg.Pool:
    dsn = dsn or get_settings().database_url
    for attempt in range(30):
        try:
            return await asyncpg.create_pool(
                dsn, min_size=min_size, max_size=max_size, init=_init_connection
            )
        except (OSError, asyncpg.CannotConnectNowError) as exc:
            log.warning("postgres not ready (%s), retry %d", exc, attempt)
            await asyncio.sleep(1)
    raise RuntimeError("could not connect to postgres")


async def migrate(pool: asyncpg.Pool) -> list[str]:
    applied: list[str] = []
    files = sorted(
        (f for f in resources.files("vf_common.migrations").iterdir() if f.name.endswith(".sql")),
        key=lambda f: f.name,
    )
    async with pool.acquire() as conn:
        await conn.execute("SELECT pg_advisory_lock($1)", MIGRATION_LOCK_ID)
        try:
            await conn.execute(
                "CREATE TABLE IF NOT EXISTS schema_migrations "
                "(name text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())"
            )
            done = {r["name"] for r in await conn.fetch("SELECT name FROM schema_migrations")}
            for f in files:
                if f.name in done:
                    continue
                async with conn.transaction():
                    await conn.execute(f.read_text())
                    await conn.execute("INSERT INTO schema_migrations (name) VALUES ($1)", f.name)
                applied.append(f.name)
                log.info("applied migration %s", f.name)
        finally:
            await conn.execute("SELECT pg_advisory_unlock($1)", MIGRATION_LOCK_ID)
    return applied


async def _main(argv: list[str]) -> None:
    if argv[:1] != ["migrate"]:
        raise SystemExit("usage: python -m vf_common.db migrate")
    pool = await create_pool()
    try:
        applied = await migrate(pool)
        print(f"migrations applied: {applied or 'none'}")
    finally:
        await pool.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(_main(sys.argv[1:]))
