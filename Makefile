# ===========================================================================
# SPaaS MDP flattener harness — task runner.
# Run `make help` for the catalog. Python (generator + demo) runs in a host
# venv to keep it off the memory-constrained Docker VM.
# ===========================================================================
SHELL := /bin/bash
COMPOSE := docker compose --env-file docker/.env -f docker/docker-compose.yml
VENV := .venv
PY := $(VENV)/bin/python
PIP := $(VENV)/bin/pip

# generator/demo knobs (override on the CLI, e.g. `make produce N=500`)
N ?= 100
RATE ?= 50
TOPIC ?= mdp.mfg.raw
BOOTSTRAP ?= localhost:29092
MALFORMED ?= 0
DUP ?= 0

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z0-9_-]+:.*?## ' $(MAKEFILE_LIST) | \
	  awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

# --------------------------------------------------------------- infrastructure
.PHONY: up
up: ## Bring the stack up and wait until ready
	$(COMPOSE) up -d
	bash scripts/wait-for-stack.sh

.PHONY: up-obs
up-obs: ## Bring the stack up WITH Prometheus + Grafana
	$(COMPOSE) --profile observability up -d
	bash scripts/wait-for-stack.sh

.PHONY: down
down: ## Stop the stack (keep volumes)
	$(COMPOSE) down

.PHONY: down-v
down-v: ## Stop the stack and DELETE all volumes (full reset)
	$(COMPOSE) down -v

.PHONY: ps
ps: ## Show container status
	$(COMPOSE) ps

.PHONY: logs
logs: ## Tail logs (SVC=trino to scope)
	$(COMPOSE) logs -f --tail=100 $(SVC)

.PHONY: wait
wait: ## Wait for the stack to be ready
	bash scripts/wait-for-stack.sh

# ------------------------------------------------------------------------ trino
.PHONY: trino-sql
trino-sql: ## Run SQL against Trino (SQL="SELECT 1")
	bash scripts/trino.sh -e "$(SQL)"

.PHONY: create-tables
create-tables: ## Create the mdp namespace + Iceberg tables via Trino
	bash scripts/trino.sh -f scripts/sql/create_tables.sql

.PHONY: smoke
smoke: ## M0 smoke test: create + query an empty Iceberg table
	bash scripts/smoke-test.sh

# ----------------------------------------------------------------- processor job
.PHONY: build-job
build-job: ## Build the Flink fat jar (Gradle in Docker, no host Gradle needed)
	bash scripts/build-job.sh

.PHONY: submit-job
submit-job: ## Submit the flattener job to the Flink cluster
	bash scripts/submit-job.sh

.PHONY: jobs
jobs: ## List running Flink jobs
	$(COMPOSE) exec -T flink-jobmanager flink list

.PHONY: cancel-jobs
cancel-jobs: ## Cancel all running Flink jobs
	bash scripts/cancel-jobs.sh

.PHONY: test
test: ## Run the JUnit + Testcontainers test suite (Gradle in Docker)
	bash scripts/test.sh

# ------------------------------------------------------------------------ python
.PHONY: venv
venv: ## Create the host Python venv and install generator + demo deps
	python3 -m venv $(VENV)
	$(PIP) install --upgrade pip >/dev/null
	$(PIP) install -r generator/requirements.txt -r demo-api/requirements.txt

.PHONY: produce
produce: ## Produce N synthetic messages (MALFORMED=1 DUP=1 to inject)
	$(PY) generator/generate.py --bootstrap $(BOOTSTRAP) --topic $(TOPIC) \
	  --count $(N) --rate $(RATE) --malformed-pct $(MALFORMED) --dup-pct $(DUP)

.PHONY: burst
burst: ## Produce a burst of N messages as fast as possible
	$(PY) generator/generate.py --bootstrap $(BOOTSTRAP) --topic $(TOPIC) \
	  --count $(N) --rate 0 --malformed-pct $(MALFORMED) --dup-pct $(DUP)

# ----------------------------------------------------------------- freshness/demo
.PHONY: freshness
freshness: ## Produce a burst, poll Trino, report P50/P95/P99 latency
	$(PY) scripts/freshness.py --count $(N)

.PHONY: demo
demo: ## Start the FastAPI demo API on http://localhost:8000
	$(PY) -m uvicorn app:app --app-dir demo-api --host 0.0.0.0 --port 8000

.PHONY: demo-full
demo-full: ## One command: up + build + submit + produce + demo URL
	bash scripts/demo-full.sh

.PHONY: clean
clean: ## Remove build artifacts and the venv
	rm -rf $(VENV) processor/build
