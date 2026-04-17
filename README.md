# auctions

`auctions` ingests auction data from [lloydsonline.com.au](https://www.lloydsonline.com.au) and writes it into any ADBC compatible database.

## Database

Writes into 3 tables:

1. **`auctions`**: Auctions currently present
2. **`lots`**: Per-lot detailed information, pricing and image locations.
3. **`lot_prices`**: Bid pricing, per lot.

`auctions-sync` is designed for repeated runs, or one off updates to the database.

---

## Build

```bash
cargo build
cargo run -- --help
cargo run --bin auctions-sync -- --help
```

---

## Quick usage

### 1) One-shot JSON output (no DB writes)

```bash
# auction list as JSON
cargo run -- list

# lots for one auction as JSON
cargo run -- lots --aid 67956
```

### 2) One-shot DB sync

```bash
cargo run --bin auctions-sync -- --once \
  --adbc-uri grpc://localhost:50051 \
  --adbc-driver adbc_driver_flightsql \
  --catalog foo \
  --schema public
```

### 3) Continuous sync loop

```bash
cargo run --bin auctions-sync -- \
  --interval-seconds 60 \
  --adbc-uri grpc://localhost:50051 \
  --adbc-driver adbc_driver_flightsql \
  --catalog foo \
  --schema public
```


## Table schema (created automatically if missing)

> `setup()` runs `CREATE SCHEMA IF NOT EXISTS` and `CREATE TABLE IF NOT EXISTS`.

### `auctions`

Primary key: `(auction_id)`

- `auction_id VARCHAR NOT NULL`
- `title VARCHAR`
- `date VARCHAR`
- `state VARCHAR`
- `auctioneer VARCHAR`
- `auction_type VARCHAR`
- `is_live BOOLEAN`
- `image_url VARCHAR`
- `details_url VARCHAR`
- `lots_url VARCHAR`
- `scraped_at TIMESTAMP`

### `lots`

Primary key: `(auctioneer, auction_id, lot_id)`

- `lot_id VARCHAR NOT NULL`
- `auction_id VARCHAR NOT NULL`
- `auctioneer VARCHAR NOT NULL`
- `lot_number VARCHAR`
- `title VARCHAR`
- `image_url VARCHAR`
- `description VARCHAR`
- `location VARCHAR`
- `lot_images VARCHAR[]` (Arrow `List(Utf8)`)
- `url VARCHAR`
- `scraped_at TIMESTAMP`

### `lot_prices`

Append-only snapshots

- `auctioneer VARCHAR NOT NULL`
- `auction_id VARCHAR NOT NULL`
- `lot_id VARCHAR NOT NULL`
- `bid DOUBLE`
- `scraped_at TIMESTAMP NOT NULL`

---

## ADBC options

- `--adbc-options '{"adbc.flight.sql.authorization_header":"<token>"}'`
- `--catalog <name>`
- `--schema <name>`

Defaults when DB mode is enabled:

- URI: `grpc://localhost:50051`
- Driver: `adbc_driver_flightsql`
- Schema: `public`

---

## Developer checks

```bash
cargo fmt
cargo test
cargo clippy --all-targets -- -D warnings
```
