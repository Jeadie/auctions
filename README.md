# lloydsonline scraper

Scrapes auctions and lots from [lloydsonline.com.au](https://www.lloydsonline.com.au) and optionally writes them to a database via [ADBC](https://arrow.apache.org/adbc/).

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Usage

### Auctions

```bash
# Print table to stdout
python lloydsonline.py auctions

# Save as JSON
python lloydsonline.py auctions --out auctions.json

# Write to database
python lloydsonline.py auctions --write-db
```

### Lots

```bash
# Print lots for a specific auction
python lloydsonline.py lots --aid 67956

# Save as JSON
python lloydsonline.py lots --aid 67956 --out lots.json

# Write to database
python lloydsonline.py lots --aid 67956 --write-db
```

## Data

### `auctions` table

| Column | Type | Description |
|---|---|---|
| `auction_id` | VARCHAR | Unique auction ID |
| `title` | VARCHAR | Auction title |
| `date` | VARCHAR | Auction date/time string |
| `state` | VARCHAR | Australian state |
| `auctioneer` | VARCHAR | Auctioneer name |
| `auction_type` | VARCHAR | e.g. "Internet & Absentee Bidding Only" |
| `is_live` | BOOLEAN | Whether the auction is currently live |
| `image_url` | VARCHAR | Thumbnail image URL |
| `details_url` | VARCHAR | Link to auction details page |
| `lots_url` | VARCHAR | Link to auction lots page |
| `scraped_at` | TIMESTAMP | UTC timestamp of scrape |

### `lots` table

| Column | Type | Description |
|---|---|---|
| `lot_id` | VARCHAR | Unique lot ID |
| `auction_id` | VARCHAR | Parent auction ID |
| `lot_number` | VARCHAR | Lot number within the auction |
| `title` | VARCHAR | Lot title/description |
| `current_bid` | VARCHAR | Current bid amount (e.g. `$330`) |
| `time_remaining` | VARCHAR | Human-readable time remaining |
| `seconds_remaining` | BIGINT | Seconds until lot closes |
| `image_url` | VARCHAR | Thumbnail image URL |
| `url` | VARCHAR | Link to lot details page |
| `scraped_at` | TIMESTAMP | UTC timestamp of scrape |

## Database (ADBC)

Connection is configured via environment variables. The ADBC FlightSQL driver is used by default (e.g. for [Spice.ai](https://spiceai.org)).

| Variable | Default | Description |
|---|---|---|
| `ADBC_URI` | `grpc://localhost:50051` | Connection URI |
| `ADBC_OPTIONS` | `{}` | JSON object of ADBC init options |
| `ADBC_CATALOG` | *(none)* | Catalog name |
| `ADBC_SCHEMA` | `public` | Schema name |
| `ADBC_DRIVER` | `adbc_driver_flightsql` | Path to ADBC driver `.so`/`.dylib` |

### `ADBC_OPTIONS`

Any ADBC driver option can be passed as a JSON object. For FlightSQL (Spice):

```bash
# Bearer token auth — bare token is auto-prefixed with "Bearer "
export ADBC_OPTIONS='{"adbc.flight.sql.authorization_header": "myapikey"}'

# TLS
export ADBC_URI="grpc+tls://spice.example.com:443"
export ADBC_OPTIONS='{"adbc.flight.sql.authorization_header": "myapikey", "adbc.flight.sql.client_option.tls_skip_verify": "true"}'
```

### Example — writing to Spice

```bash
export ADBC_URI="grpc://localhost:50051"
export ADBC_OPTIONS='{"adbc.flight.sql.authorization_header": "myapikey"}'
export ADBC_CATALOG="spice"
export ADBC_SCHEMA="auctions_data"

# Scrape all auctions and write to DB
python lloydsonline.py auctions --write-db

# Scrape lots for one auction and write to DB
python lloydsonline.py lots --aid 67956 --write-db
```

All flags can also be passed on the command line (overriding env vars):

```bash
python lloydsonline.py auctions --write-db \
  --adbc-uri grpc://localhost:50051 \
  --adbc-options '{"adbc.flight.sql.authorization_header": "myapikey"}' \
  --catalog spice \
  --schema auctions_data
```

### How writes work

On each `--write-db` run, `db.setup()` runs `CREATE SCHEMA IF NOT EXISTS` and `CREATE TABLE IF NOT EXISTS` in the current ADBC session before inserting. This is necessary because Spice's session store keys sessions by Bearer token — DDL run in an external tool (`spice sql`) lives in a different session and is not visible here. The setup is idempotent.

## Files

| File | Description |
|---|---|
| `lloydsonline.py` | Scraper + CLI |
| `db.py` | ADBC database layer |
| `requirements.txt` | Python dependencies |
