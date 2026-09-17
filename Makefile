# Final -- the whole SSIS -> NiFi migration demo, one entry point.
#
# Nothing here duplicates logic. Every target delegates into converter/,
# destination/, source/ or api/, which each keep their own working Makefile /
# docker-compose.yml. `make help` lists every target.
#
# Three stacks, deliberately non-colliding ports:
#   source       (SSIS)       :5434 postgres, :3001 grafana
#   destination  (NiFi)       :8080 nifi, :5433 postgres, :3000 grafana
#   converter's own stack     :8081 nifi, :5435 postgres  (SSIS2NIFI's sandbox)
#   api (the curl front door) :8088

.DEFAULT_GOAL := help

.PHONY: help setup up-all down-all smoke \
        analyze ir convert verify-import corpus test-converter \
        api-up api-down api-logs convert-via-api deploy-via-api \
        use-generated use-handbuilt which-flow \
        fixture bulk-run capture compare verify-behavior check-dashboards \
        up-source down-source up-destination down-destination up-converter down-converter

help:  ## list these targets
	@grep -hE '^[a-zA-Z][a-zA-Z0-9_-]*:.*?##' $(MAKEFILE_LIST) \
	  | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

setup:  ## first-time only: .env files + JDBC drivers, every stack
	$(MAKE) -C destination setup
	$(MAKE) -C converter setup
	@test -f source/.env || (cp source/.env.example source/.env && echo "created source/.env")
	@test -f api/.env || (cp api/.env.example api/.env && echo "created api/.env")

up-all: setup  ## start source + destination + converter's own stack + api
	$(MAKE) up-source
	$(MAKE) -C destination up
	$(MAKE) -C converter up
	$(MAKE) api-up
	@echo "up: source :5434/:3001  destination :8080/:5433/:3000  converter :8081/:5435  api :8088"

down-all:  ## stop everything, keep all data
	-$(MAKE) api-down
	-$(MAKE) -C converter down
	-$(MAKE) -C destination down
	-$(MAKE) down-source

up-source:  ## start only the SSIS (source) stack
	test -f source/.env || cp source/.env.example source/.env
	docker compose -f source/docker-compose.yml up -d

down-source:  ## stop only the SSIS (source) stack
	docker compose -f source/docker-compose.yml down

up-destination:  ## start only the NiFi (destination) stack
	$(MAKE) -C destination up

down-destination:  ## stop only the NiFi (destination) stack
	$(MAKE) -C destination down

up-converter:  ## start only the converter's own sandbox NiFi+PG
	$(MAKE) -C converter up

down-converter:  ## stop only the converter's own sandbox NiFi+PG
	$(MAKE) -C converter down

smoke:  ## health check every stack
	$(MAKE) -C destination smoke
	@echo "--- source ---";    docker compose -f source/docker-compose.yml ps
	@echo "--- converter ---"; docker compose -f converter/docker-compose.yml ps
	@curl -sf http://localhost:8088/health >/dev/null 2>&1 && echo "api: ok" || echo "api: DOWN (make api-up)"

# ---------------------------------------------------------------------------
# The converter (SSIS2NIFI): .dtsx -> IR -> flow.json, offline, no curl needed
# ---------------------------------------------------------------------------
FILE     ?= corpus/packages/L1.dtsx
BINDINGS ?= bindings/L1.bindings.yml

analyze:  ## report one .dtsx: make analyze FILE=../source/ssis_packages/pkg_orders_etl.dtsx
	$(MAKE) -C converter analyze FILE=$(FILE)

ir:  ## write the IR: make ir FILE=...
	$(MAKE) -C converter ir FILE=$(FILE)

convert:  ## generate a flow.json: make convert FILE=... BINDINGS=...
	$(MAKE) -C converter convert FILE=$(FILE) BINDINGS=$(BINDINGS)

verify-import:  ## import the generated flow into converter's OWN NiFi and validate
	$(MAKE) -C converter verify-import FILE=$(FILE)

corpus:  ## run every corpus package, show exit codes
	$(MAKE) -C converter corpus

test-converter:  ## the converter's unit suite (98+ tests, in Docker)
	$(MAKE) -C converter test

# ---------------------------------------------------------------------------
# The middle layer -- curl-triggered, calls the converter above underneath
# ---------------------------------------------------------------------------
PACKAGE ?= pkg_orders_etl.dtsx

api-up:  ## start the curl-triggered conversion service (:8088)
	docker compose -f api/docker-compose.yml up -d --build
	@for i in $$(seq 1 30); do curl -sf http://localhost:8088/health >/dev/null 2>&1 && break; sleep 1; done
	@echo "api up: http://localhost:8088"

api-down:  ## stop it
	docker compose -f api/docker-compose.yml down

api-logs:  ## tail the middle layer's logs
	docker compose -f api/docker-compose.yml logs -f

ui:  ## open the control panel (click buttons instead of curling)
	@echo "control panel: http://localhost:8088"
	@xdg-open http://localhost:8088 2>/dev/null || open http://localhost:8088 2>/dev/null || true

convert-via-api:  ## curl it: make convert-via-api PACKAGE=pkg_orders_etl.dtsx
	curl -sX POST localhost:8088/convert -H 'content-type: application/json' \
	  -d '{"package":"$(PACKAGE)"}' | python3 -m json.tool

deploy-via-api:  ## curl it to deploy onto the DESTINATION NiFi: make deploy-via-api PACKAGE=...
	curl -sX POST localhost:8088/deploy -H 'content-type: application/json' \
	  -d '{"package":"$(PACKAGE)"}' | python3 -m json.tool

# ---------------------------------------------------------------------------
# Which flow is live on the destination NiFi -- hand-built or generated,
# never both (they write the same tables)
# ---------------------------------------------------------------------------
use-generated:  ## deploy SSIS2NIFI's generated flow onto destination NiFi (stops hand-built)
	./scripts/use-generated.sh

use-handbuilt:  ## restore NIFI-FLOW's hand-built flow onto destination NiFi
	./scripts/use-handbuilt.sh

which-flow:  ## print which flow is currently live on destination NiFi
	./scripts/which-flow.sh

clear-canvas:  ## delete every process group from destination NiFi's canvas (fresh start)
	./scripts/clear-canvas.sh

# ---------------------------------------------------------------------------
# The 50k / 1-lakh proof, via destination/spec/compare (engine-neutral)
# ---------------------------------------------------------------------------
TIER   ?= tier3-bulk
ENGINE ?= nifi

fixture:  ## build a frozen dataset: make fixture TIER=tier3-bulk (or TIER=all)
	$(MAKE) -C destination fixture TIER=$(TIER)

bulk-run:  ## clean both engines, feed them one fixture, run both: make bulk-run TIER=tier3-bulk
	$(MAKE) -C destination parallel-run TIER=$(TIER)

capture:  ## snapshot an engine's results: make capture ENGINE=nifi
	$(MAKE) -C destination capture ENGINE=$(ENGINE)

compare:  ## diff the newest nifi and ssis captures (exit 0 == they agree)
	$(MAKE) -C destination compare

verify-behavior:  ## converter's own independent-oracle gate, at scale
	$(MAKE) -C converter verify-behavior FILE=$(FILE) BINDINGS=$(BINDINGS)

check-dashboards:  ## run every Grafana panel query on both boards, report failures
	$(MAKE) -C destination check-dashboards
