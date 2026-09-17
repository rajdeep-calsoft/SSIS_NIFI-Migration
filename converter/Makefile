# SSIS2NIFI -- every operation. `make help` lists them.
#
# Tests run in Docker because this machine has no pip or python3-venv, and
# because a tool meant to ship should have a reproducible test environment
# rather than one that depends on what happens to be installed.

PY      ?= python3
PKG     ?= python:3.12-slim
FILE     ?= corpus/packages/L1.dtsx
BINDINGS ?= bindings/L1.bindings.yml
# This repo's OWN stack (docker-compose.yml) -- 8081/5435, not NIFI-FLOW's
# 8080/5433, so the two can never collide even if both happen to be up.
NIFI     ?= http://localhost:8081
NIFI_CONTAINER ?= ssis2nifi-engine
WAREHOUSE ?= ssis2nifi-warehouse
CORPUS  := corpus/packages
# Bind-mounted straight from this repo's own docker-compose.yml -- ./data,
# not another project's.
LANDING  ?= $(PWD)/data/landing
REJECTS  ?= $(PWD)/data/rejects

.DEFAULT_GOAL := help

.PHONY: help setup up down ps logs analyze ir convert verify-import deploy check verify-behavior corpus test json clean

help:  ## list these targets
	@grep -hE '^[a-z-]+:.*?##' $(MAKEFILE_LIST) \
	  | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

setup:  ## first-time only: .env + JDBC driver
	@test -f .env || (cp .env.example .env && echo "created .env")
	@test -f drivers/postgresql.jar || ( \
	  echo "fetching Postgres JDBC driver..." && \
	  curl -sSL -o drivers/postgresql.jar \
	    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar )
	@echo "ready -- now run: make up"

up: setup  ## start this repo's OWN NiFi + Postgres (no other folder involved)
	docker compose up -d
	@echo "waiting for NiFi to answer..."
	@for i in $$(seq 1 60); do \
	  curl -sf $(NIFI)/nifi-api/system-diagnostics >/dev/null 2>&1 && break; \
	  sleep 2; \
	done
	@echo "up: NiFi at $(NIFI), Postgres at localhost:5435 (db ssis2nifi, user etl)"

down:  ## stop this stack, keep the database volume
	docker compose down

ps:  ## this repo's own containers only
	docker compose ps

# Exit 3 means "parsed, needs a human" -- a real outcome, not a build failure.
# Only 4 (refused) should stop make. Anything else propagates unchanged.
analyze:  ## report one package: make analyze FILE=corpus/packages/L4.dtsx
	@$(PY) -m ssis2nifi analyze $(FILE); c=$$?; [ $$c -eq 3 ] && exit 0 || exit $$c

json:  ## emit the IR as JSON for one package
	@$(PY) -m ssis2nifi analyze $(FILE) --json; c=$$?; [ $$c -eq 3 ] && exit 0 || exit $$c

ir:  ## write the IR: make ir FILE=corpus/packages/L4.dtsx
	@mkdir -p out
	@$(PY) -m ssis2nifi ir $(FILE) -o out/$$(basename $(FILE) .dtsx).ir.yaml; \
	  c=$$?; [ $$c -eq 3 ] && exit 0 || exit $$c

convert:  ## generate a flow: make convert FILE=... BINDINGS=bindings/L1.bindings.yml
	@mkdir -p out
	@$(PY) -m ssis2nifi convert $(FILE) -b $(BINDINGS); \
	  c=$$?; [ $$c -eq 3 ] && exit 0 || exit $$c

verify-import:  ## import the generated flow into a live NiFi and check it is valid
	@$(PY) -m ssis2nifi verify out/$$(basename $(FILE) .dtsx).flow.json --nifi $(NIFI)

deploy:  ## import into NiFi, inject secrets from .env, start
	@set -a; . ./.env; set +a; \
	SSIS2NIFI_DB_MAIN_PASSWORD="$$POSTGRES_PASSWORD" $(PY) -m ssis2nifi deploy \
	  out/$$(basename $(FILE) .dtsx).flow.json \
	  --nifi $(NIFI) --group-name ssis2nifi-$$(basename $(FILE) .dtsx)

check:  ## rows loaded vs rejected, after a feed
	@docker exec -i $(WAREHOUSE) psql -U etl -d ssis2nifi -c \
	  "select (select count(*) from dbo.newfactcurrencyrate) as rows_loaded;"
	@docker exec $(NIFI_CONTAINER) sh -c \
	  'echo "reject files: $$(ls -1 /opt/nifi/data/rejects 2>/dev/null | wc -l)"'

verify-behavior:  ## M5: feed a batch, check it against an independent expectation
	@$(PY) -m ssis2nifi verify-behavior $(FILE) -b $(BINDINGS) \
	  --landing $(LANDING) --reject-dir $(REJECTS) \
	  --container $(WAREHOUSE) --db ssis2nifi --user etl \
	  --fact-table dbo.newfactcurrencyrate --wait 15

precision:  ## the global checker: score every genuine .dtsx in the repo, exit 0 only when no addable gaps remain
	@$(PY) scripts/precision_check.py

corpus:  ## run every package and show its exit code
	@printf "%-34s %-6s %s\n" PACKAGE EXIT RESULT
	@for f in $(CORPUS)/*.dtsx; do \
	  out=$$($(PY) -m ssis2nifi analyze "$$f" --no-graph 2>&1); code=$$?; \
	  printf "%-34s %-6s %s\n" "$$(basename $$f)" "$$code" \
	    "$$(printf '%s' "$$out" | grep -E '^(coverage|refused)' | head -1 | cut -c1-60)"; \
	done

test:  ## run the suite in Docker
	@docker run --rm -v "$$PWD":/w -w /w $(PKG) \
	  sh -c "pip install -q -r requirements-dev.txt 2>/dev/null && python -m pytest tests/ -q"

clean:  ## remove generated output and caches
	@rm -rf out/* .pytest_cache
	@find . -name __pycache__ -type d -exec rm -rf {} + 2>/dev/null || true
