# One image for every VisionForge service; docker-compose picks the entrypoint.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PATH=/opt/venv/bin:$PATH

COPY --from=ghcr.io/astral-sh/uv:0.11 /uv /usr/local/bin/uv
WORKDIR /app

# Dependencies first so source edits don't bust the layer cache.
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project

COPY libs libs
COPY services services
RUN uv sync --frozen --no-dev --no-editable

COPY client client
COPY scripts scripts

RUN useradd --create-home --uid 10001 vf
USER vf
