"""SQS access (ElasticMQ locally, Amazon SQS in production).

Three work queues, each with a dead-letter queue:

  <prefix>-split   uploaded documents waiting to be split into pages
  <prefix>-fast    pages waiting for the fast (layout) model
  <prefix>-slow    pages waiting for the slow (VLM) model

A message is deleted only after the work it describes is durably committed. If a consumer
dies first, the message becomes visible again after its visibility timeout and another
consumer picks it up. A consumer gives up on a message delivered more than ``MAX_DELIVERIES``
times (it keeps crashing its consumers); as a backstop, SQS itself moves a message received
``MAX_RECEIVES`` times to the queue's DLQ.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Any

import aioboto3

from vf_common.config import Settings

log = logging.getLogger(__name__)
QUEUES = ("split", "fast", "slow")
VISIBILITY_S = 60  # consumers extend this while they work, so it only bounds crash recovery
MAX_DELIVERIES = 3
MAX_RECEIVES = 5


@dataclass
class Message:
    body: dict[str, Any]
    receipt: str
    receive_count: int


class Queues:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._session = aioboto3.Session()
        self._cm = None
        self.sqs = None
        self.urls: dict[str, str] = {}

    async def start(self) -> "Queues":
        self._cm = self._session.client(
            "sqs", endpoint_url=self.settings.sqs_endpoint, region_name=self.settings.s3_region,
            aws_access_key_id="visionforge", aws_secret_access_key="visionforge")
        self.sqs = await self._cm.__aenter__()
        for attempt in range(30):  # the queue service may still be starting
            try:
                await self._ensure_queues()
                return self
            except Exception as exc:  # noqa: BLE001
                log.warning("SQS not ready (%s), retry %d", exc, attempt)
                await asyncio.sleep(1)
        raise RuntimeError("could not reach SQS")

    async def close(self) -> None:
        if self._cm is not None:
            await self._cm.__aexit__(None, None, None)

    def _name(self, queue: str) -> str:
        return f"{self.settings.queue_prefix}-{queue}"

    async def _ensure_queues(self) -> None:
        """Create each queue and its DLQ (idempotent: CreateQueue returns an existing queue)."""
        for queue in QUEUES:
            dlq = (await self.sqs.create_queue(QueueName=self._name(queue) + "-dlq"))["QueueUrl"]
            arn = (await self.sqs.get_queue_attributes(QueueUrl=dlq, AttributeNames=["QueueArn"]))["Attributes"]["QueueArn"]
            url = (await self.sqs.create_queue(QueueName=self._name(queue), Attributes={
                "VisibilityTimeout": str(VISIBILITY_S),
                "RedrivePolicy": json.dumps({"deadLetterTargetArn": arn, "maxReceiveCount": str(MAX_RECEIVES)}),
            }))["QueueUrl"]
            self.urls[queue], self.urls[queue + "-dlq"] = url, dlq

    async def send_batch(self, queue: str, messages: list[tuple[dict, int]]) -> None:
        """Send ``(body, delay_seconds)`` pairs, 10 per SQS call."""
        for i in range(0, len(messages), 10):
            chunk = messages[i:i + 10]
            resp = await self.sqs.send_message_batch(QueueUrl=self.urls[queue], Entries=[
                {"Id": str(n), "MessageBody": json.dumps(body), "DelaySeconds": min(900, max(0, int(delay)))}
                for n, (body, delay) in enumerate(chunk)])
            if resp.get("Failed"):
                raise RuntimeError(f"SQS rejected {len(resp['Failed'])} message(s): {resp['Failed'][:1]}")

    async def receive(self, queue: str, wait_s: int = 5) -> Message | None:
        """Long-poll for one message; it stays invisible to others for VISIBILITY_S."""
        resp = await self.sqs.receive_message(
            QueueUrl=self.urls[queue], MaxNumberOfMessages=1, WaitTimeSeconds=wait_s,
            AttributeNames=["ApproximateReceiveCount"])
        for m in resp.get("Messages", []):
            return Message(json.loads(m["Body"]), m["ReceiptHandle"], int(m["Attributes"]["ApproximateReceiveCount"]))
        return None

    async def delete(self, queue: str, msg: Message) -> None:
        await self.sqs.delete_message(QueueUrl=self.urls[queue], ReceiptHandle=msg.receipt)

    async def extend(self, queue: str, msg: Message, seconds: int = VISIBILITY_S) -> None:
        """Keep a message invisible while it is still being worked on (or delay its retry)."""
        await self.sqs.change_message_visibility(
            QueueUrl=self.urls[queue], ReceiptHandle=msg.receipt, VisibilityTimeout=min(43_200, int(seconds)))

    async def depth(self, queue: str) -> int:
        """Messages waiting (visible + delayed); excludes the ones being processed."""
        attrs = (await self.sqs.get_queue_attributes(QueueUrl=self.urls[queue], AttributeNames=[
            "ApproximateNumberOfMessages", "ApproximateNumberOfMessagesDelayed"]))["Attributes"]
        return int(attrs["ApproximateNumberOfMessages"]) + int(attrs.get("ApproximateNumberOfMessagesDelayed", 0))
