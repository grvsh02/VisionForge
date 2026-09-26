COMPOSE := docker compose -f deploy/docker-compose.yml
BASE_URL ?= http://localhost:8080

.PHONY: build up fast-up down logs ps test integration e2e fixtures load chaos backpressure

build:
	docker build -t visionforge:latest .

up: build
	$(COMPOSE) up -d

# Mock models 10x faster (rate limits unchanged) for quicker local iteration.
fast-up: build
	VF_LATENCY_SCALE=0.1 $(COMPOSE) up -d

down:
	$(COMPOSE) down -v

logs:
	$(COMPOSE) logs -f --tail=100

ps:
	$(COMPOSE) ps

test:
	uv run pytest tests/unit -q

# Repo, outbox, worker and gateway tests on the compose postgres + ElasticMQ (throwaway databases/queues).
integration:
	VF_TEST_DATABASE_URL=postgresql://vf:vf@localhost:5432/vf VF_TEST_SQS_URL=http://localhost:9324 \
		uv run pytest tests/integration -q -m "not e2e"

e2e:
	VF_E2E_BASE_URL=$(BASE_URL) uv run pytest tests/integration -q -m e2e

fixtures:
	uv run python scripts/gen_fixtures.py

load:
	uv run python scripts/loadtest.py --base-url $(BASE_URL)

chaos:
	uv run python scripts/chaos_sigkill.py --base-url $(BASE_URL)

backpressure:
	uv run python scripts/backpressure_demo.py --base-url $(BASE_URL)
