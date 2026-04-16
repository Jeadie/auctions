# auctions (Rust)

CLI scrapers for [lloydsonline.com.au](https://www.lloydsonline.com.au).

- `auctions`: one-shot fetcher (`list`, `lots`, `completions`)
- `auctions-sync`: continuous DB updater for auctions + lots + lot price history

## Build

```bash
cargo build
cargo run -- --help
cargo run --bin auctions-sync -- --help
```

## Spice Flight SQL target (local)

This repo includes `spicepod.yaml` with a writable Cayenne catalog:

- catalog: `foo`
- access: `read_write_create`

Start Spice runtime in one terminal:

```bash
spice run
```

(Optional) pre-create schema manually in Spice SQL:

```sql
CREATE SCHEMA IF NOT EXISTS foo.public;
```

Then write to it from this CLI (it will also auto-run `CREATE SCHEMA IF NOT EXISTS` + table DDL):

```bash
cargo run --bin auctions-sync -- --once \
  --adbc-uri grpc://localhost:50051 \
  --adbc-driver adbc_driver_flightsql \
  --catalog foo \
  --schema public
```

## `auctions` behavior

- If **`--adbc-uri`** or **`--adbc-driver`** is provided, writes to DB via ADBC.
- Otherwise, prints JSON to stdout.

### List auctions

```bash
# JSON to stdout
auctions list

# Write to DB
auctions list --adbc-uri grpc://localhost:50051
```

### List lots

```bash
# JSON to stdout
auctions lots --aid 67956

# Write to DB
auctions lots --aid 67956 --adbc-driver adbc_driver_flightsql
```

### Shell completions

```bash
auctions completions zsh > ~/.zsh/completions/_auctions
```

## `auctions-sync` behavior

`auctions-sync` is intended for long-running ingestion.

Per cycle it:

1. Scrapes auction list and upserts `auctions`.
2. Scrapes lots for each selected auction.
3. Appends **only new lots** into `lots` (existing lot keys are left untouched).
4. Appends a row per lot into `lot_prices` so bid changes are tracked over time.

### Example

```bash
# run forever (default 60s interval)
auctions-sync --adbc-uri grpc://localhost:50051

# run once (useful for cron / smoke testing)
auctions-sync --once

# only track specific auctions
auctions-sync --aid 67956 --aid 67957 --interval-seconds 30
```

## ADBC options

Optional in DB mode:

- `--adbc-options '{"adbc.flight.sql.authorization_header":"mytoken"}'`
- `--catalog foo`
- `--schema auctions_data`

Defaults when DB mode is enabled:

- URI: `grpc://localhost:50051`
- Driver: `adbc_driver_flightsql`
- Schema: `public`

## Tables

- `auctions` primary key: `(auction_id)`
- `lots` primary key: `(auctioneer, auction_id, lot_id)`
  - includes `description`, `location`, and `lot_images` as `List(Utf8)` / `VARCHAR[]`
- `lot_prices`: append-only bid snapshots (no primary key; one row per scrape)

## Developer checks

```bash
cargo fmt
cargo test
cargo clippy --all-targets -- -D warnings
```
