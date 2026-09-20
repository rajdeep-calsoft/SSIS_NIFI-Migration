# ssis-nifi-telecom-migrator -- a dynamic SSIS -> NiFi migration tool,
# proven against a 50,000-record telecom Call Detail Record (CDR) job.
# `make help` lists every target.
#
# Two Docker Compose stacks, deliberately non-colliding with the reference
# repo's own ports:
#   source       (telecom warehouse + batch generator)   :5436 postgres
#   destination  (NiFi + telecom warehouse)               :8085 nifi, :5437 postgres

SHELL := /bin/bash
.DEFAULT_GOAL := help
.PHONY: help setup venv up down up-source down-source up-destination down-destination \
        analyze migrate deploy generate-data compare-report test smoke clean

JOB     ?= telecom_cdr
ROWS    ?= 50000
VENV    := .venv
PY      := $(VENV)/bin/python3
PIP     := $(VENV)/bin/pip

# Some machines only expose the Docker socket to root (no `docker` group set
# up) -- detected once per `make` invocation so `make up` (run as yourself,
# no sudo) still works: only the docker/compose calls escalate, never the
# venv/pip steps, which is what matters -- running THOSE under sudo is what
# left .venv root-owned and broke every later non-sudo `make test`/`make
# migrate` last time. Override explicitly with `make DOCKER_COMPOSE="docker
# compose" up` if this detection ever guesses wrong.
DOCKER_COMPOSE := $(shell docker info >/dev/null 2>&1 && echo "docker compose" || echo "sudo docker compose")

help:  ## list these targets
	@grep -hE '^[a-zA-Z][a-zA-Z0-9_-]*:.*?##' $(MAKEFILE_LIST) \
	  | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

venv:  ## create the local venv migrator/ and report/ run under
	@test -d $(VENV) || python3 -m venv $(VENV)
	@$(PIP) install -q -r migrator/requirements.txt -r report/requirements.txt

setup: venv  ## first-time only: .env files + the destination NiFi's Postgres JDBC driver
	@test -f source/.env || (cp source/.env.example source/.env && echo "created source/.env")
	@test -f destination/.env || (cp destination/.env.example destination/.env && echo "created destination/.env")
	@mkdir -p destination/nifi/drivers destination/data/landing
	@test -f destination/nifi/drivers/postgresql.jar || ( \
	  echo "fetching Postgres JDBC driver..." && \
	  curl -sSL -o destination/nifi/drivers/postgresql.jar \
	    https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar )
	@echo "ready -- now run: make up"

up: setup  ## start both stacks
	$(MAKE) up-source
	$(MAKE) up-destination
	@echo "up: source postgres :5436   destination nifi :8085 / postgres :5437"

down:  ## stop both stacks, keep all data
	-$(MAKE) down-destination
	-$(MAKE) down-source

up-source:  ## start only the source (telecom warehouse) stack
	$(DOCKER_COMPOSE) -f source/docker-compose.yml up -d

down-source:
	$(DOCKER_COMPOSE) -f source/docker-compose.yml down

up-destination:  ## start only the destination (NiFi + warehouse) stack
	$(DOCKER_COMPOSE) -f destination/docker-compose.yml up -d

down-destination:
	$(DOCKER_COMPOSE) -f destination/docker-compose.yml down

smoke:  ## health check both stacks
	@echo "--- source ---";      $(DOCKER_COMPOSE) -f source/docker-compose.yml ps
	@echo "--- destination ---"; $(DOCKER_COMPOSE) -f destination/docker-compose.yml ps

# ---------------------------------------------------------------------------
# The migration tool itself -- generic, no telecom-specific code (see
# migrator/jobconfig.py's docstring for what makes that true).
# ---------------------------------------------------------------------------

analyze: venv  ## report what a job's .dtsx contains, offline: make analyze JOB=telecom_cdr
	$(PY) migrator/cli.py analyze jobs/$(JOB)

migrate: venv  ## convert AND deploy a job onto the destination NiFi: make migrate JOB=telecom_cdr
	set -a && source destination/.env && set +a && \
	$(PY) migrator/cli.py deploy jobs/$(JOB)

deploy: migrate  ## alias

# ---------------------------------------------------------------------------
# The telecom CDR demo job
# ---------------------------------------------------------------------------

generate-data: venv  ## write ROWS synthetic CDRs + ground truth: make generate-data JOB=telecom_cdr ROWS=50000
	set -a && source source/.env && set +a && \
	$(DOCKER_COMPOSE) -f source/docker-compose.yml run --rm generator \
	  bulk --rows $(ROWS) --landing-dir /data/landing

compare-report: venv  ## generate the standalone SSIS-vs-NiFi report: make compare-report JOB=telecom_cdr
	set -a && source source/.env 2>/dev/null; source destination/.env 2>/dev/null && set +a && \
	TELECOM_DB_PASSWORD=$${TELECOM_DB_PASSWORD:-$$POSTGRES_PASSWORD} \
	  $(PY) report/cli.py compare-report --job jobs/$(JOB)

# ---------------------------------------------------------------------------

test: venv  ## run every unit test (no Docker required)
	$(PY) -m pytest migrator/tests report/tests -q

clean:  ## remove generated output and the venv
	rm -rf out $(VENV)
	find . -name __pycache__ -type d -exec rm -rf {} + 2>/dev/null || true
