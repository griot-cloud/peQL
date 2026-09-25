# Quickstart

This walks through the quickstart workspace in the repository: a contract for orders shared
between tenants, one data file, and three callers.

## 1. Install

On Linux or macOS:

```bash
curl -LsSf https://github.com/griot-cloud/peql/releases/latest/download/install.sh | sh
curl -LsSf https://github.com/griot-cloud/parcel/releases/latest/download/install.sh | sh   # optional: authoring and checks
```

On Windows, in PowerShell:

```powershell
powershell -ExecutionPolicy ByPass -c "irm https://github.com/griot-cloud/peql/releases/latest/download/install.ps1 | iex"
powershell -ExecutionPolicy ByPass -c "irm https://github.com/griot-cloud/parcel/releases/latest/download/install.ps1 | iex"
```

The installers check the download's sha256; `PEQL_VERSION` picks a release and
`PEQL_INSTALL_DIR` the destination. Every [release](https://github.com/griot-cloud/peql/releases)
carries the binaries for Linux (x86_64, arm64), macOS (Apple silicon, Intel) and Windows (x64).
To build from source instead (Rust 1.94 or newer):
`cargo install --git https://github.com/griot-cloud/peql peql`.

Then, from a clone of the repository:

```bash
cd examples/quickstart
```

The workspace holds a contract (`contracts/orders.yaml`), a CSV of orders
(`incoming/orders.csv`), and three callers (`callers/*.yaml`). The contract is parcel YAML; its
rules read:

| Rule | What it does for a caller |
| --- | --- |
| `analytics_only` (decide) | Refuses any purpose but analytics and reporting. |
| `own_or_admin` (admit) | Rows of the caller's own tenant, or all rows for admins. |
| `amount_consistent` (assert, drop) | Drops rows whose amount does not add up. |
| `mask_email` (transform) | Hashes email for everyone but `acme`. |
| `ids_present` (guarantee, deny) | Refuses to serve data with too many missing customer ids. |
| `small_cells` (shape: suppress) | Removes groups smaller than five, except for `acme`. |

## 2. Write the data under the contract

```bash
peql write contracts/orders.yaml --input incoming/orders.csv --type msisdn=utf8
```

```text
wrote 600 rows to sales/orders (2 files): valid
  amount_consistent: 15 rows fail
  msisdn_format: 24 rows fail
```

peQL compiled the contract with parcel, computed a flag column per assertion, partitioned the
files by region, stamped each file with the contract's hash, ran the validation plan, and wrote
a manifest. `acme` owns the contract; share it with `globex`:

```bash
peql publish sales/orders --to globex
```

## 3. Query as three callers

```bash
peql query 'SELECT region, COUNT(*) AS orders FROM "sales/orders" GROUP BY region ORDER BY region' \
  --caller callers/globex-analyst.yaml
```

A globex analyst sees only globex's consistent rows, grouped, with small groups removed. The
acme admin sees every consistent row and emails in clear:

```bash
peql query 'SELECT order_id, email FROM "sales/orders" ORDER BY order_id LIMIT 3' \
  --caller callers/acme-admin.yaml
```

A marketing caller is refused before any file is opened:

```bash
peql query 'SELECT COUNT(*) FROM "sales/orders"' --caller callers/marketing.yaml
# refused: denied by `sales/orders` rule `analytics_only`
```

Add `--envelope` to see which rules applied, what the scan read, and the attestation hashes.
Every query, refused or answered, is recorded in `_peql/audit.jsonl`.

## 4. Hand over a compiled contract

parcel can compile a contract into a bundle; peQL registers the bundle after recompiling it
and checking that it gets the same hash:

```bash
parcel compile contracts/orders.yaml --schema incoming/orders.csv --type msisdn=utf8 -o orders.parcel.json
peql register orders.parcel.json
```

Next: {doc}`USAGE` covers the Rust and Python APIs, and {doc}`concepts` explains what happened
inside each query.
