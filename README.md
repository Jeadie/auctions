# auctions (Rust)

CLI scraper for [lloydsonline.com.au](https://www.lloydsonline.com.au).

## Behavior

- If **`--adbc-uri`** or **`--adbc-driver`** is provided, the command writes to the database via ADBC.
- Otherwise, the command prints JSON to stdout.

## Build

```bash
cargo build
cargo run -- --help
```

## Usage

### List auctions

```bash
# JSON to stdout
auctions list

# Write to DB (ADBC enabled by --adbc-uri)
auctions list --adbc-uri grpc://localhost:50051
```

### List lots

```bash
# JSON to stdout
auctions lots --aid 67956

# Write to DB (ADBC enabled by --adbc-driver)
auctions lots --aid 67956 --adbc-driver adbc_driver_flightsql
```

### ADBC options

Optional with DB mode:

- `--adbc-options '{"adbc.flight.sql.authorization_header":"mytoken"}'`
- `--catalog spice`
- `--schema auctions_data`

Write behavior is always key-based upsert:

- delete rows that match incoming primary keys
- insert fresh rows

Primary keys used by the schema:

- `auctions`: `(auction_id)`
- `lots`: `(auctioneer, auction_id, lot_id)`

Defaults when DB mode is enabled:

- URI: `grpc://localhost:50051`
- Driver: `adbc_driver_flightsql`
- Schema: `public`

### Shell completions

```bash
auctions completions zsh > ~/.zsh/completions/_auctions
```

## Developer checks

```bash
cargo fmt
cargo test
cargo clippy --all-targets -- -D warnings
```
