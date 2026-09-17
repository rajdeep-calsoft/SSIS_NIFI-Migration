# NiFi E-commerce ETL demo
# `make help` lists everything.

SHELL := /bin/bash
COMPOSE := docker compose
PSQL := $(COMPOSE) exec -T postgres psql -U etl -d etldemo
# The other engine's repo. Not a git repository and never staged from here;
# only ever addressed through docker compose -f.
SSIS_REPO ?= $(abspath ..)/source

.DEFAULT_GOAL := help
.PHONY: help setup up down reset logs ps smoke inject scenarios psql \
        build-flow export-flow reprovision dashboards check-dashboards watch \
        spec spec-check spec-verify capture compare rules-diff \
        dims fixture fixture-inject parallel-run \
        stop-flow start-flow stop-generator stats demo-reset up-both down-both validate-feed

help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
	  | awk 'BEGIN{FS=":.*?## "}{printf "  \033[1m%-18s\033[0m %s\n", $$1, $$2}'
	@echo
	@echo "  NiFi      http://localhost:8080/nifi   (no login)"
	@echo "  Grafana   http://localhost:3000        (admin/admin)"
	@echo "  Postgres  localhost:5433               (etl/etlpass, db etldemo)"

setup: ## First-time setup: .env + JDBC driver
	@test -f .env || (cp .env.example .env && echo "created .env")
	@test -f nifi/drivers/postgresql.jar || ( \
	  echo "fetching Postgres JDBC driver..." && \
	  curl -sSL -o nifi/drivers/postgresql.jar \
	    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar )
	@echo "ready - now run: make up"

up: setup ## Start the whole stack (NiFi, Postgres, Grafana, generator, monitor)
	$(COMPOSE) up -d --build
	@echo
	@echo "Waiting for the flow to be provisioned..."
	@timeout 240 $(COMPOSE) logs -f bootstrap 2>/dev/null | sed '/provision\] flow/q' || true
	@$(MAKE) --no-print-directory smoke

up-both: ## Start BOTH stacks (this one and the SSIS one) - the demo starting point
	@# His stack has no `make up`, and the comparison views error rather than
	@# return zero when his warehouse is unreachable -- so "did I remember to
	@# start his side" is a question worth not having to ask.
	@$(MAKE) --no-print-directory up
	@echo
	@echo "starting the SSIS stack..."
	@docker compose -f $(SSIS_REPO)/docker-compose.yml up -d
	@# orders_gen is behind the `solo` profile and stays down on purpose:
	@# live-compare needs exactly ONE generator feeding both inboxes.
	@echo
	@echo "both stacks up. next:  make live-compare"

down-both: ## Stop BOTH stacks, keep all data
	@docker compose -f $(SSIS_REPO)/docker-compose.yml down
	@$(COMPOSE) down

down: ## Stop the stack, keep all data
	$(COMPOSE) down

reset: ## Destroy EVERYTHING (volumes, generated data) for a clean cold start
	$(COMPOSE) down -v
	rm -f data/landing/*.json data/archive/*.json data/dlq/*.json
	@echo "clean. run: make up"

demo-reset: ## Wipe the fact tables only - fresh numbers, stack stays up
	@$(PSQL) -c "TRUNCATE orders, order_items, alerts, quarantine_records, job_runs, nifi_metrics, nifi_bulletins;"
	@echo "fact tables cleared (dimensions kept)"

ps: ## Show container status
	$(COMPOSE) ps

logs: ## Tail logs (make logs S=generator for one service)
	$(COMPOSE) logs -f $(S)

stats: ## Live container resource usage
	docker stats --no-stream $$($(COMPOSE) ps -q)

smoke: ## Health check everything - run this before demoing
	@bash scripts/smoke.sh

scenarios: ## List the demo scenarios you can inject
	@$(COMPOSE) run --rm --no-deps generator python -m gen inject

inject: ## Inject a scenario: make inject SCENARIO=fraud_burst
	@test -n "$(SCENARIO)" || (echo "usage: make inject SCENARIO=<name>  (see: make scenarios)"; exit 1)
	@$(COMPOSE) run --rm --no-deps generator python -m gen inject $(SCENARIO)

watch: ## Live row counts for all 9 tables, refreshing every 2s
	@bash scripts/watch_tables.sh

psql: ## Open a psql shell on the warehouse
	@$(COMPOSE) exec postgres psql -U etl -d etldemo

build-flow: ## Rebuild the NiFi flow from code (destroys the canvas group)
	$(COMPOSE) run --rm --no-deps bootstrap python -m gen build-flow

export-flow: ## Save the live canvas back to nifi/flow/*.json
	$(COMPOSE) run --rm --no-deps bootstrap python -m gen export-flow
	@sudo chown $$(id -u):$$(id -g) nifi/flow/*.json 2>/dev/null || true

reprovision: ## Re-import and restart the flow (after editing the JSON)
	$(COMPOSE) run --rm bootstrap python -m gen provision

stop-flow: ## Stop the NiFi flow (leave the stack up)
	@bash scripts/flow_state.sh STOPPED

start-flow: ## Start the NiFi flow
	@bash scripts/flow_state.sh RUNNING

stop-generator: ## Stop the generator container (stopping the flow alone does not stop new files landing)
	$(COMPOSE) stop generator

dashboards: ## Regenerate the Grafana dashboard JSON (both repos, both Grafanas)
	python3 scripts/generate_dashboards.py
	$(COMPOSE) restart grafana
	@# One of the four boards is written into the SSIS repo, so his Grafana
	@# has to be restarted too. Without this the generator reported success
	@# while :3001 kept serving the previous board until provisioning next
	@# rescanned -- which reads as "the change did not work".
	@docker restart ssis-grafana >/dev/null 2>&1 \
	  && echo "restarted ssis-grafana (:3001)" \
	  || echo "ssis-grafana not running - his board updates when it starts"

check-dashboards: ## Run every dashboard panel query and report failures
	python3 scripts/check_dashboards.py

fixture: ## Build a frozen dataset: make fixture TIER=tier1-smoke (or TIER=all)
	@python3 spec/fixtures/make_fixture.py $(if $(filter all,$(TIER)),--all,$(TIER))

parallel-run: ## Clean both engines, feed them one fixture, run both: make parallel-run TIER=tier1-smoke
	@bash scripts/parallel_run.sh $(or $(TIER),tier1-smoke)

live-compare: ## LIVE side-by-side: one generator feeds both engines continuously
	@bash scripts/live_compare.sh

live-compare-keep: ## Same, but do NOT wipe the warehouses first
	@bash scripts/live_compare.sh --keep

watch-both: ## Both engines' counters side by side in the terminal, refreshing
	@bash scripts/watch_both.sh $(or $(INTERVAL),2)

feed: ## Push ONE file of your own through BOTH engines: make feed FILE=my.json
	@test -n "$(FILE)" || (echo "usage: make feed FILE=path/to/file.json"; exit 1)
	@bash scripts/feed_file.sh "$(FILE)" $(if $(KEEP),--keep-generator,)

validate-feed: ## Check a file BEFORE feeding it: make validate-feed FILE=my.json
	@test -n "$(FILE)" || (echo "usage: make validate-feed FILE=path/to/file.json"; exit 1)
	@python3 scripts/validate_feed.py "$(FILE)"

errors: ## Every rejected record, both engines, newest first (N=50)
	@bash scripts/show_errors.sh $(or $(N),50)

errors-export: ## Dump both engines' rejects to data/exports/ as CSV + JSON
	@bash scripts/export_errors.sh

fdw: ## (Re)build the cross-engine views that read the SSIS warehouse
	@$(PSQL) -q -f /docker-entrypoint-initdb.d/04_ssis_fdw.sql && \
	  echo "cross-engine views rebuilt"

fixture-inject: ## Feed one fixture to BOTH engines: make fixture-inject TIER=tier1-smoke
	@test -n "$(TIER)" || (echo "usage: make fixture-inject TIER=tier1-smoke"; exit 1)
	python3 spec/fixtures/inject_fixture.py $(TIER) $(if $(ONLY),--only $(ONLY))

dims: ## Regenerate the catalogue shared by both engines (NiFi + SSIS)
	python3 spec/fixtures/dims.py

spec: ## Regenerate spec/pipeline.yml + spec/engines/nifi.yml from the flow
	python3 spec/extract_nifi.py

spec-check: ## Fail if the committed spec has drifted from the flow (CI gate)
	python3 spec/extract_nifi.py --check

spec-verify: ## Prove the RUNNING pipeline matches spec/pipeline.yml
	python3 scripts/verify_spec.py

rules-diff: ## Compare this pipeline's RULES against another engine's spec
	python3 scripts/rules_diff.py $(if $(ENGINE),--engine $(ENGINE))

capture: ## Snapshot an engine's results: make capture ENGINE=nifi [LABEL=before]
	@test -n "$(ENGINE)" || (echo "usage: make capture ENGINE=nifi|ssis [LABEL=before]"; exit 1)
	python3 spec/compare/capture.py $(ENGINE) $(if $(LABEL),--label $(LABEL))

compare: ## Diff captures: make compare  (newest nifi vs ssis)  or  LEFT=.. RIGHT=..
	python3 spec/compare/diff.py $(LEFT) $(RIGHT)
