"""Shared fixtures for tests that need a real Postgres (VF_TEST_DATABASE_URL) and, for the
pipeline tests, a real SQS endpoint (VF_TEST_SQS_URL, e.g. the compose ElasticMQ)."""

from __future__ import annotations

import os
import uuid

import asyncpg
import pytest

from vf_common import repo
from vf_common.config import Settings
from vf_common.db import _init_connection, migrate
from vf_common.queues import Queues

PG_DSN = os.environ.get("VF_TEST_DATABASE_URL")
SQS_URL = os.environ.get("VF_TEST_SQS_URL")


def _with_db(dsn: str, name: str) -> str:
    base, sep, query = dsn.partition("?")
    return base.rsplit("/", 1)[0] + f"/{name}" + sep + query


@pytest.fixture
async def pg_dsn():
    """A fresh, migrated throwaway database per test."""
    name = f"vf_test_{uuid.uuid4().hex[:8]}"
    admin = await asyncpg.connect(PG_DSN)
    await admin.execute(f"CREATE DATABASE {name}")
    dsn = _with_db(PG_DSN, name)
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=2, init=_init_connection)
    await migrate(pool)
    await pool.close()
    yield dsn
    await admin.execute(f"DROP DATABASE {name} WITH (FORCE)")
    await admin.close()


@pytest.fixture
async def pool(pg_dsn):
    p = await asyncpg.create_pool(pg_dsn, min_size=1, max_size=20, init=_init_connection)
    yield p
    await p.close()


@pytest.fixture
async def queues():
    """Fresh queues (and DLQs) under a unique prefix per test."""
    if not SQS_URL:
        pytest.skip("set VF_TEST_SQS_URL to run SQS tests")
    q = await Queues(Settings(sqs_endpoint=SQS_URL, queue_prefix=f"t{uuid.uuid4().hex[:8]}")).start()
    yield q
    for url in q.urls.values():
        await q.sqs.delete_queue(QueueUrl=url)
    await q.close()


async def create_split_job(conn, client_id: str, pages: int) -> uuid.UUID:
    """A job already split into ``pages`` PENDING pages, each with a 'fast' outbox row."""
    await repo.ensure_client(conn, client_id)
    job_id = uuid.uuid4()
    await repo.create_job(conn, job_id=job_id, client_id=client_id, content_type="application/pdf",
                          object_key=f"uploads/{job_id}", size_bytes=1, total_pages=pages)
    await conn.execute("UPDATE outbox_events SET sent_at = now() WHERE queue = 'split'")  # skip splitting
    assert await repo.split_done(conn, job_id, client_id, [f"pages/{job_id}/{i:04d}.pdf" for i in range(pages)])
    return job_id
