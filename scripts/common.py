"""Shared helpers for the load, fairness and chaos scripts (async HTTP, SSE, docker)."""

from __future__ import annotations

import asyncio
import json
import os
import random
import subprocess
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx
import pikepdf

from client.cli import Assembler

ROOT = Path(__file__).resolve().parent.parent
COMPOSE = ["docker", "compose", "-f", str(ROOT / "deploy" / "docker-compose.yml")]
PG_DSN = os.environ.get("VF_E2E_DATABASE_URL", "postgresql://vf:vf@localhost:5432/vf")
REDIS_URL = os.environ.get("VF_E2E_REDIS_URL", "redis://localhost:6379/0")
SQS_URL = os.environ.get("VF_E2E_SQS_URL", "http://localhost:9324")
MOCK_URLS = {"layout": os.environ.get("VF_E2E_LAYOUT_URL", "http://localhost:8081"),
             "vlm": os.environ.get("VF_E2E_VLM_URL", "http://localhost:8082")}


# --- fixtures -----------------------------------------------------------------------

def make_pdf(path: Path, pages: int, page_kb: int = 0, seed: int = 0) -> Path:
    """Text PDF; ``page_kb`` pads each page with an incompressible image XObject."""
    rng = random.Random(seed)
    pdf = pikepdf.new()
    font = pdf.make_indirect(pikepdf.Dictionary(Type=pikepdf.Name.Font, Subtype=pikepdf.Name.Type1,
                                                BaseFont=pikepdf.Name.Helvetica))
    for i in range(pages):
        pdf.add_blank_page(page_size=(612, 792))
        page = pdf.pages[-1]
        resources = pikepdf.Dictionary(Font=pikepdf.Dictionary(F1=font))
        ops = f"BT /F1 18 Tf 72 720 Td (VisionForge fixture page {i + 1} of {pages}) Tj ET\n"
        if page_kb:
            side = int((page_kb * 1024) ** 0.5)
            img = pdf.make_stream(rng.randbytes(side * side), Type=pikepdf.Name.XObject,
                                  Subtype=pikepdf.Name.Image, Width=side, Height=side,
                                  ColorSpace=pikepdf.Name.DeviceGray, BitsPerComponent=8)
            resources.XObject = pikepdf.Dictionary(Im0=img)
            ops += "q 200 0 0 200 72 400 cm /Im0 Do Q\n"
        page.Resources = resources
        page.Contents = pdf.make_stream(ops.encode())
    path.parent.mkdir(parents=True, exist_ok=True)
    pdf.save(path)
    return path


def fixture(pages: int, page_kb: int = 0) -> Path:
    path = ROOT / "fixtures" / f"doc{pages}p_{page_kb}kb.pdf"
    return path if path.exists() else make_pdf(path, pages, page_kb)


# --- HTTP / SSE -----------------------------------------------------------------------

async def submit(http: httpx.AsyncClient, base: str, client_id: str, path: Path) -> httpx.Response:
    async def chunks():
        with path.open("rb") as fh:
            while chunk := fh.read(1024 * 1024):
                yield chunk

    return await http.post(f"{base}/jobs", content=chunks(), timeout=300,
                           headers={"X-Client-ID": client_id, "Content-Type": "application/pdf",
                                    "Content-Length": str(path.stat().st_size)})


FrameHook = Callable[[str, dict[str, Any], float], None]


async def stream(http: httpx.AsyncClient, base: str, client_id: str, job_id: str, *,
                 on_frame: FrameHook | None = None, timeout_s: float = 3600) -> Assembler:
    """Consume a job's SSE stream to completion, reconnecting with Last-Event-ID."""
    asm = Assembler()
    deadline = time.monotonic() + timeout_s
    while not (asm.complete or asm.failed):
        if time.monotonic() > deadline:
            raise TimeoutError(f"job {job_id} did not complete")
        headers = {"X-Client-ID": client_id, "Last-Event-ID": str(asm.last_seq)}
        try:
            async with http.stream("GET", f"{base}/jobs/{job_id}/stream", headers=headers,
                                   timeout=httpx.Timeout(10.0, read=60.0)) as resp:
                resp.raise_for_status()
                kind, data = "message", []
                async for line in resp.aiter_lines():
                    if line.startswith("event:"):
                        kind = line[6:].strip()
                    elif line.startswith("data:"):
                        data.append(line[5:].lstrip())
                    elif line == "" and data:
                        frame = json.loads("\n".join(data))
                        data = []
                        header = frame["header"]
                        if header["seq"] > asm.last_seq + 1:
                            raise ConnectionError("seq gap")
                        asm.accept(kind, frame)
                        if on_frame:
                            on_frame(kind, frame, time.monotonic())
                        if asm.complete or asm.failed:
                            break
        except (httpx.HTTPError, ConnectionError):
            await asyncio.sleep(1)
    return asm


# --- docker / infra -------------------------------------------------------------------

def compose(*args: str, check: bool = True) -> str:
    return subprocess.run([*COMPOSE, *args], check=check, capture_output=True, text=True).stdout


def container_ids(service: str) -> list[str]:
    return [c for c in compose("ps", "-q", service).split() if c]


def docker(*args: str) -> str:
    return subprocess.run(["docker", *args], check=True, capture_output=True, text=True).stdout


_UNITS = {"B": 1, "KiB": 1024, "MiB": 1024 ** 2, "GiB": 1024 ** 3, "kB": 1000, "MB": 1000 ** 2, "GB": 1000 ** 3}


def _bytes(value: str) -> float:
    for unit in sorted(_UNITS, key=len, reverse=True):
        if value.endswith(unit):
            return float(value[: -len(unit)]) * _UNITS[unit]
    return float(value)


async def sample_docker_memory(stop: asyncio.Event, samples: dict[str, list[tuple[float, float]]],
                               interval_s: float = 1.0) -> None:
    """Record (t, bytes) per container name until ``stop`` is set."""
    t0 = time.monotonic()
    while not stop.is_set():
        proc = await asyncio.create_subprocess_exec(
            "docker", "stats", "--no-stream", "--format", "{{json .}}",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
        out, _ = await proc.communicate()
        now = time.monotonic() - t0
        for line in out.decode().splitlines():
            row = json.loads(line)
            if not row["Name"].startswith("visionforge-"):
                continue
            used = row["MemUsage"].split("/")[0].strip()
            samples.setdefault(row["Name"], []).append((now, _bytes(used)))
        try:
            await asyncio.wait_for(stop.wait(), interval_s)
        except TimeoutError:
            pass


def memory_report(samples: dict[str, list[tuple[float, float]]], warmup_frac: float = 0.25) -> list[dict]:
    """Peak and post-warm-up slope (MiB/min) per container."""
    rows = []
    for name, pts in sorted(samples.items()):
        peak = max(b for _, b in pts)
        steady = pts[int(len(pts) * warmup_frac):]
        slope = 0.0
        if len(steady) >= 2:
            n = len(steady)
            mt = sum(t for t, _ in steady) / n
            mb = sum(b for _, b in steady) / n
            var = sum((t - mt) ** 2 for t, _ in steady) or 1.0
            slope = sum((t - mt) * (b - mb) for t, b in steady) / var
        rows.append({"container": name, "peak_mib": round(peak / 2 ** 20, 1),
                     "slope_mib_per_min": round(slope * 60 / 2 ** 20, 2)})
    return rows
