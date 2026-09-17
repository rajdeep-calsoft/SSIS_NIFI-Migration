#!/usr/bin/env bash
# Pre-demo health check. Every line either says ok or tells you what to fix.
# Exits non-zero if anything is broken, so it works in CI too.
set -uo pipefail

NIFI="${NIFI_URL:-http://localhost:8080}"
GRAFANA="${GRAFANA_URL:-http://localhost:3000}"
PG="docker compose exec -T postgres psql -U etl -d etldemo -tAc"

pass=0; fail=0
ok()   { echo -e "  \033[32mok\033[0m    $1"; pass=$((pass+1)); }
bad()  { echo -e "  \033[31mFAIL\033[0m  $1"; fail=$((fail+1)); }
warn() { echo -e "  \033[33mwarn\033[0m  $1"; }

echo "== containers =="
for c in nifi-engine nifi-warehouse nifi-grafana shared-data-generator nifi-monitor; do
  status=$(docker inspect -f '{{.State.Status}}' "$c" 2>/dev/null || echo missing)
  [ "$status" = "running" ] && ok "$c running" || bad "$c is '$status'"
done

echo "== nifi =="
if curl -sf -m 5 "$NIFI/nifi-api/system-diagnostics" >/dev/null; then
  ok "REST API reachable at $NIFI"
else
  bad "NiFi API unreachable at $NIFI"
fi

FLOW=$(curl -sf -m 5 "$NIFI/nifi-api/flow/process-groups/root" 2>/dev/null | python3 -c "
import json,sys
try:
    groups=json.load(sys.stdin)['processGroupFlow']['flow']['processGroups']
    g=next(x for x in groups if x['component']['name']=='ecommerce_etl')
    # NB: running/stopped/invalid counts live on the ENTITY, not inside status
    s=g['status']['aggregateSnapshot']
    print(f\"{g['id']} {g.get('runningCount',0)} {g.get('stoppedCount',0)} {g.get('invalidCount',0)} {s.get('flowFilesQueued',0)}\")
except Exception: print('none 0 0 0 0')
" 2>/dev/null)
read -r GID RUNNING STOPPED INVALID QUEUED <<< "$FLOW"

if [ "$GID" = "none" ]; then
  bad "process group 'ecommerce_etl' not on the canvas - run: make reprovision"
else
  # A STOPPED processor is a failure, not a note. One stopped processor in the
  # middle of the flow stalls everything behind it: the records queue up
  # (nothing is lost -- that is the design) but nothing loads, and the reject
  # path keeps working, so the dashboards show rejects climbing while orders
  # sit flat. This check once reported "30 running, 1 stopped" as ok and a
  # stalled pipeline passed a pre-demo health check.
  if [ "$STOPPED" -eq 0 ]; then
    ok "flow present, all $RUNNING processors running"
  else
    bad "$STOPPED of $((RUNNING + STOPPED)) processors are STOPPED - the flow is stalled"
    echo "        run: make start-flow      (queued records will drain once started)"
  fi
  [ "$INVALID" -eq 0 ] && ok "no invalid processors" || bad "$INVALID invalid processors"
  [ "$RUNNING" -gt 0 ] && ok "flow is running" || bad "flow is not running - run: make start-flow"
  [ "$QUEUED" -lt 1000 ] && ok "queue depth $QUEUED" || warn "queue depth $QUEUED - backpressure"
fi

echo "== postgres =="
if $PG "select 1" >/dev/null 2>&1; then
  ok "accepting connections"
  TABLES=$($PG "select count(*) from information_schema.tables where table_schema='public' and table_type='BASE TABLE'" 2>/dev/null)
  [ "${TABLES:-0}" -ge 9 ] && ok "$TABLES tables present" || bad "only ${TABLES:-0} tables - schema did not load"
  DIMS=$($PG "select (select count(*) from customers)+(select count(*) from products)" 2>/dev/null)
  # 60 customers + 18 products: the catalogue SHARED with the SSIS engine.
  # Both warehouses must hold identical rows -- see spec/fixtures/dims.py.
  [ "${DIMS:-0}" -eq 78 ] && ok "dimensions seeded (60 customers, 18 products)" || bad "dimensions look wrong ($DIMS rows, expected 78)"
  ORDERS=$($PG "select count(*) from orders" 2>/dev/null)
  [ "${ORDERS:-0}" -gt 0 ] && ok "$ORDERS orders loaded" || warn "no orders yet - give the generator a minute, or: make inject SCENARIO=clean"
  FRESH=$($PG "select case when max(sampled_at) > now() - interval '30 seconds' then 1 else 0 end from nifi_metrics" 2>/dev/null)
  [ "${FRESH:-0}" = "1" ] && ok "monitor is sampling NiFi" || bad "no fresh nifi_metrics - check: make logs S=monitor"
else
  bad "cannot reach Postgres"
fi

echo "== grafana =="
if curl -sf -m 5 "$GRAFANA/api/health" >/dev/null; then
  ok "reachable at $GRAFANA"
  DASH=$(curl -sf -m 5 -u admin:admin "$GRAFANA/api/search?type=dash-db" | python3 -c "import json,sys; print(len(json.load(sys.stdin)))" 2>/dev/null)
  [ "${DASH:-0}" -ge 2 ] && ok "$DASH dashboards provisioned" || bad "expected 2 dashboards, found ${DASH:-0}"
else
  bad "Grafana unreachable at $GRAFANA"
fi

echo
if [ "$fail" -eq 0 ]; then
  echo -e "\033[32mAll $pass checks passed - ready to demo.\033[0m"
else
  echo -e "\033[31m$fail check(s) failed\033[0m ($pass passed)"
fi
exit $((fail > 0))
