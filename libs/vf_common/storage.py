"""S3 object storage access with bounded memory: uploads are streamed as multipart parts and
downloads are streamed to disk in fixed-size chunks."""

from __future__ import annotations

import json
import logging
from collections.abc import AsyncIterator
from pathlib import Path
from typing import BinaryIO

import aioboto3
from botocore.config import Config
from botocore.exceptions import ClientError

from vf_common.config import Settings

log = logging.getLogger(__name__)

CHUNK = 1024 * 1024


class UploadTooLarge(Exception):
    pass


def result_key(job_id, page_idx: int, name: str) -> str:
    """Deterministic result path (fast | slow | final), so rewriting it is idempotent."""
    return f"results/{job_id}/pages/{page_idx:04d}/{name}.json"


class Storage:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.bucket = settings.s3_bucket
        self._session = aioboto3.Session()
        self._cm = None
        self.s3 = None

    async def start(self) -> "Storage":
        # Local/self-hosted S3 (SeaweedFS) sets an endpoint and static keys. On AWS both are left
        # empty: the real S3 endpoint is used and credentials come from the task's IAM role.
        kwargs: dict = {"region_name": self.settings.s3_region}
        if self.settings.s3_endpoint:
            kwargs["endpoint_url"] = self.settings.s3_endpoint
        if self.settings.s3_access_key:
            kwargs["aws_access_key_id"] = self.settings.s3_access_key
            kwargs["aws_secret_access_key"] = self.settings.s3_secret_key
        style = "path" if self.settings.s3_endpoint else "auto"
        self._cm = self._session.client(
            "s3", config=Config(s3={"addressing_style": style}, retries={"max_attempts": 5}), **kwargs)
        self.s3 = await self._cm.__aenter__()
        return self

    async def close(self) -> None:
        if self._cm is not None:
            await self._cm.__aexit__(None, None, None)

    async def ensure_bucket(self) -> None:
        try:
            await self.s3.head_bucket(Bucket=self.bucket)
        except ClientError:
            await self.s3.create_bucket(Bucket=self.bucket)

    async def upload_stream(self, key: str, chunks: AsyncIterator[bytes], *, max_bytes: int,
                            content_type: str, tee: BinaryIO | None = None) -> int:
        """Stream ``chunks`` into ``key``; holds at most one part in memory.

        ``tee`` also receives every chunk (e.g. a temp file, so the caller can inspect the
        upload on disk without buffering it in memory or downloading it again).
        """
        part_size = self.settings.upload_part_bytes
        buf = bytearray()
        total = 0
        upload_id: str | None = None
        parts: list[dict] = []
        try:
            async for chunk in chunks:
                total += len(chunk)
                if total > max_bytes:
                    raise UploadTooLarge(f"upload exceeds {max_bytes} bytes")
                buf.extend(chunk)
                if tee is not None:
                    tee.write(chunk)
                if len(buf) >= part_size:
                    if upload_id is None:
                        resp = await self.s3.create_multipart_upload(
                            Bucket=self.bucket, Key=key, ContentType=content_type)
                        upload_id = resp["UploadId"]
                    parts.append(await self._upload_part(key, upload_id, len(parts) + 1, bytes(buf)))
                    buf.clear()
            if upload_id is None:
                await self.s3.put_object(Bucket=self.bucket, Key=key, Body=bytes(buf),
                                         ContentType=content_type)
            else:
                if buf:
                    parts.append(await self._upload_part(key, upload_id, len(parts) + 1, bytes(buf)))
                await self.s3.complete_multipart_upload(
                    Bucket=self.bucket, Key=key, UploadId=upload_id, MultipartUpload={"Parts": parts})
            return total
        except BaseException:
            if upload_id is not None:
                try:
                    await self.s3.abort_multipart_upload(Bucket=self.bucket, Key=key, UploadId=upload_id)
                except Exception:  # noqa: BLE001 - best effort cleanup
                    log.warning("failed to abort multipart upload %s", key)
            raise

    async def _upload_part(self, key: str, upload_id: str, number: int, body: bytes) -> dict:
        resp = await self.s3.upload_part(
            Bucket=self.bucket, Key=key, UploadId=upload_id, PartNumber=number, Body=body)
        return {"ETag": resp["ETag"], "PartNumber": number}

    async def download_to_file(self, key: str, path: Path) -> int:
        resp = await self.s3.get_object(Bucket=self.bucket, Key=key)
        body = resp["Body"]  # aiobotocore StreamingBody; `async with` only releases the connection
        size = 0
        async with body:
            with path.open("wb") as fh:
                async for chunk in body.iter_chunks(CHUNK):
                    fh.write(chunk)
                    size += len(chunk)
        return size

    async def delete(self, key: str) -> None:
        await self.s3.delete_object(Bucket=self.bucket, Key=key)

    async def put_bytes(self, key: str, data: bytes, content_type: str) -> None:
        await self.s3.put_object(Bucket=self.bucket, Key=key, Body=data, ContentType=content_type)

    async def get_bytes(self, key: str) -> bytes:
        resp = await self.s3.get_object(Bucket=self.bucket, Key=key)
        body = resp["Body"]
        async with body:
            return await body.read()

    async def put_json(self, key: str, data) -> None:
        await self.put_bytes(key, json.dumps(data).encode(), "application/json")

    async def get_json(self, key: str):
        return json.loads(await self.get_bytes(key))
