import asyncio
import io
import os

import pytest

from vf_common import storage as st
from vf_common.config import Settings
from vf_common.storage import Storage, UploadTooLarge

PART = 100  # bytes per part in these tests


class FakeS3:
    """Records part uploads; each takes ``delay`` seconds so uploads overlap."""

    def __init__(self, delay: float = 0.02, fail_part: int | None = None) -> None:
        self.delay, self.fail_part = delay, fail_part
        self.parts: dict[tuple[str, int], bytes] = {}
        self.active: dict[str, int] = {}
        self.max_active: dict[str, int] = {}
        self.total_active = self.max_total_active = 0
        self.completed: dict[str, list] = {}
        self.aborted: set[str] = set()
        self.put: dict[str, bytes] = {}

    async def create_multipart_upload(self, Bucket, Key, ContentType):
        return {"UploadId": f"up-{Key}"}

    async def upload_part(self, Bucket, Key, UploadId, PartNumber, Body):
        self.active[Key] = self.active.get(Key, 0) + 1
        self.total_active += 1
        self.max_active[Key] = max(self.max_active.get(Key, 0), self.active[Key])
        self.max_total_active = max(self.max_total_active, self.total_active)
        try:
            await asyncio.sleep(self.delay)
            if PartNumber == self.fail_part:
                raise RuntimeError("S3 is down")
            self.parts[(Key, PartNumber)] = Body
            return {"ETag": f"etag-{PartNumber}"}
        finally:
            self.active[Key] -= 1
            self.total_active -= 1

    async def complete_multipart_upload(self, Bucket, Key, UploadId, MultipartUpload):
        self.completed[Key] = MultipartUpload["Parts"]

    async def abort_multipart_upload(self, Bucket, Key, UploadId):
        self.aborted.add(Key)

    async def put_object(self, Bucket, Key, Body, ContentType):
        self.put[Key] = Body


def make_storage(fake: FakeS3) -> Storage:
    storage = Storage(Settings(upload_part_bytes=PART))
    storage.s3 = fake
    return storage


async def chunks(data: bytes, size: int = 37):
    for i in range(0, len(data), size):
        yield data[i:i + size]
        await asyncio.sleep(0)


async def test_small_upload_is_a_single_put():
    fake = FakeS3()
    storage = make_storage(fake)
    assert await storage.upload_stream("k", chunks(b"x" * 60), max_bytes=10_000, content_type="application/pdf") == 60
    assert fake.put == {"k": b"x" * 60} and not fake.completed


async def test_parts_upload_in_parallel_and_reassemble_in_order():
    fake = FakeS3()
    storage = make_storage(fake)
    data, tee = os.urandom(PART * 12 + 17), io.BytesIO()
    size = await storage.upload_stream("k", chunks(data), max_bytes=10_000, content_type="application/pdf", tee=tee)
    assert size == len(data) and tee.getvalue() == data
    parts = fake.completed["k"]
    assert len(parts) > st.PARTS_IN_FLIGHT  # enough parts for the cap to matter
    assert [p["PartNumber"] for p in parts] == list(range(1, len(parts) + 1))
    assert b"".join(fake.parts[("k", p["PartNumber"])] for p in parts) == data
    assert fake.max_active["k"] == st.PARTS_IN_FLIGHT  # overlapping, but capped per upload
    await asyncio.sleep(0)  # done callbacks (slot releases) run on the next loop tick
    assert storage._part_slots._value == st.PART_SLOTS  # every slot returned


async def test_parts_in_flight_are_capped_across_uploads(monkeypatch):
    monkeypatch.setattr(st, "PART_SLOTS", 4)
    fake = FakeS3()
    storage = make_storage(fake)
    uploads = [storage.upload_stream(f"k{i}", chunks(os.urandom(PART * 10)), max_bytes=10_000,
                                     content_type="application/pdf") for i in range(3)]
    await asyncio.gather(*uploads)
    assert fake.max_total_active == 4
    assert all(v <= st.PARTS_IN_FLIGHT for v in fake.max_active.values())
    assert all(len(fake.completed[f"k{i}"]) == 10 for i in range(3))


async def test_a_failed_part_aborts_the_upload_and_returns_every_slot():
    fake = FakeS3(fail_part=2)
    storage = make_storage(fake)
    with pytest.raises(RuntimeError, match="S3 is down"):
        await storage.upload_stream("k", chunks(os.urandom(PART * 10)), max_bytes=10_000, content_type="application/pdf")
    assert fake.aborted == {"k"} and "k" not in fake.completed
    await asyncio.sleep(0)  # let cancelled tasks' done callbacks run
    assert storage._part_slots._value == st.PART_SLOTS


async def test_too_large_aborts_and_cancels_parts_in_flight():
    fake = FakeS3(delay=0.5)  # parts still uploading when the limit is hit
    storage = make_storage(fake)
    with pytest.raises(UploadTooLarge):
        await storage.upload_stream("k", chunks(os.urandom(PART * 10)), max_bytes=PART * 4 + 10,
                                    content_type="application/pdf")
    assert fake.aborted == {"k"}
    await asyncio.sleep(0)
    assert storage._part_slots._value == st.PART_SLOTS
