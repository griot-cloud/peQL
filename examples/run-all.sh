#!/usr/bin/env bash
# Run every example workspace from a clean state. Exits non-zero on the first failure.
#   PEQL=path/to/peql examples/run-all.sh
set -euo pipefail
P=${PEQL:-peql}
here=$(cd "$(dirname "$0")" && pwd)

echo "== quickstart"
cd "$here/quickstart"
rm -rf data _peql
"$P" write contracts/orders.yaml --input incoming/orders.csv --type msisdn=utf8
"$P" register contracts/orders_ea.yaml --schema incoming/orders.csv --type msisdn=utf8
"$P" publish sales/orders --to globex
"$P" publish sales/orders_ea --to globex
"$P" list
"$P" query 'SELECT region, COUNT(*) AS orders, SUM(amount_cents) AS revenue FROM "sales/orders" GROUP BY region ORDER BY region' \
  --caller callers/globex-analyst.yaml
"$P" query 'SELECT region, COUNT(*) AS n FROM "sales/orders_ea" GROUP BY region ORDER BY region' --caller callers/globex-analyst.yaml
"$P" query 'SELECT order_id, email FROM "sales/orders" ORDER BY order_id LIMIT 3' --caller callers/acme-admin.yaml
if "$P" query 'SELECT COUNT(*) FROM "sales/orders"' --caller callers/marketing.yaml; then
  echo "marketing should have been refused" >&2; exit 1
fi
"$P" query 'SELECT COUNT(*) AS n FROM "sales/orders"' --caller callers/acme-admin.yaml --format json
"$P" validate sales/orders > /dev/null
test -s _peql/audit.jsonl

echo "== utility (tenant WebAssembly functions)"
cd "$here/utility"
rm -rf data _peql
for f in is_meter_serial units county; do
  "$P" function register functions/meter_serial.wasm --manifest "functions/$f.yaml" --owner kplc
done
"$P" write contracts/tokens.yaml --input incoming/tokens.csv
"$P" query 'SELECT meter AS county, COUNT(*) AS tokens FROM "kplc/tokens" GROUP BY meter ORDER BY county' --caller callers/kisumu-analyst.yaml

echo "== handoff: parcel compiles, peql runs"
if command -v "${PARCEL:-parcel}" >/dev/null 2>&1; then
  cd "$here/quickstart"
  work=$(mktemp -d)
  trap 'rm -rf "$work"' EXIT
  "${PARCEL:-parcel}" check contracts/orders.yaml --data incoming/orders.csv --type msisdn=utf8 --json > "$work/check.json"
  "${PARCEL:-parcel}" compile contracts/orders.yaml --schema incoming/orders.csv --type msisdn=utf8 -o "$work/orders.parcel.json" > /dev/null
  "$P" --root "$work" register "$work/orders.parcel.json"
  mkdir -p "$work/data" && cp -r data/orders "$work/data/"
  "$P" --root "$work" publish sales/orders --to globex
  "$P" --root "$work" query 'SELECT COUNT(*) AS n FROM "sales/orders"' --caller callers/globex-analyst.yaml
else
  echo "(parcel not on PATH; skipped)"
fi

echo "== all examples passed"
