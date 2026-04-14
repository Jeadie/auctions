"""
db.py — Pure ADBC database layer for lloydsonline scraper.

Exposes the ADBC interface directly: driver path, URI, and options dict.
Assumes catalog and schema already exist in the target database.

Environment variables:

    ADBC_DRIVER     Path to ADBC driver .so/.dylib (default: adbc_driver_flightsql)
    ADBC_URI        Connection URI  (default: grpc://localhost:50051)
    ADBC_OPTIONS    JSON object of ADBC init options, e.g.
                    '{"adbc.flight.sql.authorization_header": "Bearer mytoken"}'
    ADBC_CATALOG    Catalog name for writes (optional)
    ADBC_SCHEMA     Schema name for writes  (default: public)
"""

import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import pyarrow as pa
import adbc_driver_manager as adbc
import adbc_driver_manager.dbapi
import adbc_driver_flightsql as fl

# ---------------------------------------------------------------------------
# Arrow schemas
# ---------------------------------------------------------------------------

AUCTIONS_SCHEMA = pa.schema([
    pa.field("auction_id",   pa.string(),                  nullable=False),
    pa.field("title",        pa.string()),
    pa.field("date",         pa.string()),
    pa.field("state",        pa.string()),
    pa.field("auctioneer",   pa.string()),
    pa.field("auction_type", pa.string()),
    pa.field("is_live",      pa.bool_()),
    pa.field("image_url",    pa.string()),
    pa.field("details_url",  pa.string()),
    pa.field("lots_url",     pa.string()),
    pa.field("scraped_at",   pa.timestamp("us", tz="UTC"), nullable=False),
])

LOTS_SCHEMA = pa.schema([
    pa.field("lot_id",            pa.string(),                  nullable=False),
    pa.field("auction_id",        pa.string(),                  nullable=False),
    pa.field("lot_number",        pa.string()),
    pa.field("title",             pa.string()),
    pa.field("current_bid",       pa.string()),
    pa.field("time_remaining",    pa.string()),
    pa.field("seconds_remaining", pa.int64()),
    pa.field("image_url",         pa.string()),
    pa.field("url",               pa.string()),
    pa.field("scraped_at",        pa.timestamp("us", tz="UTC"), nullable=False),
])

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

def _default_options() -> dict[str, str]:
    raw = os.getenv("ADBC_OPTIONS", "")
    return json.loads(raw) if raw else {}


@dataclass
class DBConfig:
    """ADBC connection config.

    driver  — path to the ADBC driver shared library
    uri     — ADBC connection URI passed to AdbcDatabase
    options — dict of ADBC init options (auth, TLS, etc.)
              passed as **kwargs to AdbcDatabase at construction time
    catalog — catalog name used as the ingest target (optional)
    schema  — schema name used as the ingest target
    """
    driver:  str                = field(default_factory=lambda: fl._driver_path())
    uri:     str                = field(default_factory=lambda: os.getenv("ADBC_URI", "grpc://localhost:50051"))
    options: dict[str, str]     = field(default_factory=_default_options)
    catalog: Optional[str]      = field(default_factory=lambda: os.getenv("ADBC_CATALOG") or None)
    schema:  str                = field(default_factory=lambda: os.getenv("ADBC_SCHEMA", "public"))

    def __str__(self) -> str:
        cat = f"{self.catalog}." if self.catalog else ""
        return f"{self.uri} [{cat}{self.schema}]"


# ---------------------------------------------------------------------------
# Connection
# ---------------------------------------------------------------------------

def connect(config: DBConfig) -> adbc.dbapi.Connection:
    """Open an ADBC connection.

    Constructs AdbcDatabase(driver, uri, **options), applies post-init
    tunable options via set_options(), and wraps in a DBAPI connection.

    The authorization_header option accepts either a bare token ("mytoken")
    or a fully qualified value ("Bearer mytoken"). Bare values are
    automatically prefixed with "Bearer ".
    """
    AUTH_KEY = fl.DatabaseOptions.AUTHORIZATION_HEADER.value
    options = dict(config.options)  # don't mutate the config
    if AUTH_KEY in options:
        val = options[AUTH_KEY]
        if not val.lower().startswith(("bearer ", "basic ")):
            options[AUTH_KEY] = f"Bearer {val}"

    db = adbc.AdbcDatabase(
        driver=config.driver,
        uri=config.uri,
        **options,
    )

    # Post-init: set any options the driver accepts after construction.
    # For FlightSQL these are the timeout options; auth/TLS must be in
    # config.options (passed at construction time above).
    post_init = {k: v for k, v in options.items() if k.startswith("adbc.flight.sql.rpc.timeout")}
    if post_init:
        db.set_options(**post_init)

    raw_conn = adbc.AdbcConnection(db)
    return adbc.dbapi.Connection(db, raw_conn)


# ---------------------------------------------------------------------------
# DDL — schema + table setup via executescript() (CommandStatementUpdate)
#
# ADBC has no schema-creation API. More importantly, Spice's session store
# keys sessions by Bearer token value: every request with the same token
# reuses the same SessionContext. executescript() (CommandStatementUpdate)
# runs through QueryBuilder which uses that session context — so CREATE
# SCHEMA and CREATE TABLE here are visible to adbc_ingest() below because
# they share the same session. DDL from external tools (spice sql, etc.)
# runs in a different session and is NOT visible here.
# ---------------------------------------------------------------------------

def _schema_ref(config: DBConfig) -> str:
    if config.catalog:
        return f"{config.catalog}.{config.schema}"
    return config.schema


def setup(conn: adbc.dbapi.Connection, config: DBConfig) -> None:
    """Create schema and tables in the current session if they don't exist.

    Must be called on every new connection before writing — DDL is
    session-scoped and does not persist across connections.
    """
    schema_ref = _schema_ref(config)
    with conn.cursor() as cur:
        cur.executescript(f"CREATE SCHEMA IF NOT EXISTS {schema_ref}")
        cur.executescript(f"""
            CREATE TABLE IF NOT EXISTS {schema_ref}.auctions (
                auction_id   VARCHAR NOT NULL,
                title        VARCHAR,
                date         VARCHAR,
                state        VARCHAR,
                auctioneer   VARCHAR,
                auction_type VARCHAR,
                is_live      BOOLEAN,
                image_url    VARCHAR,
                details_url  VARCHAR,
                lots_url     VARCHAR,
                scraped_at   TIMESTAMP
            )
        """)
        cur.executescript(f"""
            CREATE TABLE IF NOT EXISTS {schema_ref}.lots (
                lot_id            VARCHAR NOT NULL,
                auction_id        VARCHAR NOT NULL,
                lot_number        VARCHAR,
                title             VARCHAR,
                current_bid       VARCHAR,
                time_remaining    VARCHAR,
                seconds_remaining BIGINT,
                image_url         VARCHAR,
                url               VARCHAR,
                scraped_at        TIMESTAMP
            )
        """)


# ---------------------------------------------------------------------------
# Arrow conversion
# ---------------------------------------------------------------------------

def _now() -> datetime:
    return datetime.now(tz=timezone.utc)


def auctions_to_arrow(auctions: list[dict], scraped_at: datetime) -> pa.Table:
    return pa.table(
        {
            "auction_id":   pa.array([a.get("auction_id")    for a in auctions], type=pa.string()),
            "title":        pa.array([a.get("title")          for a in auctions], type=pa.string()),
            "date":         pa.array([a.get("date")           for a in auctions], type=pa.string()),
            "state":        pa.array([a.get("state")          for a in auctions], type=pa.string()),
            "auctioneer":   pa.array([a.get("auctioneer")     for a in auctions], type=pa.string()),
            "auction_type": pa.array([a.get("auction_type")   for a in auctions], type=pa.string()),
            "is_live":      pa.array([bool(a.get("is_live"))  for a in auctions], type=pa.bool_()),
            "image_url":    pa.array([a.get("image_url")      for a in auctions], type=pa.string()),
            "details_url":  pa.array([a.get("details_url")    for a in auctions], type=pa.string()),
            "lots_url":     pa.array([a.get("lots_url")       for a in auctions], type=pa.string()),
            "scraped_at":   pa.array([scraped_at] * len(auctions), type=pa.timestamp("us", tz="UTC")),
        },
        schema=AUCTIONS_SCHEMA,
    )


def lots_to_arrow(lots: list[dict], auction_id: str, scraped_at: datetime) -> pa.Table:
    return pa.table(
        {
            "lot_id":            pa.array([l.get("lot_id")            for l in lots], type=pa.string()),
            "auction_id":        pa.array([auction_id] * len(lots),                   type=pa.string()),
            "lot_number":        pa.array([l.get("lot_number")        for l in lots], type=pa.string()),
            "title":             pa.array([l.get("title")             for l in lots], type=pa.string()),
            "current_bid":       pa.array([l.get("current_bid")       for l in lots], type=pa.string()),
            "time_remaining":    pa.array([l.get("time_remaining")    for l in lots], type=pa.string()),
            "seconds_remaining": pa.array([l.get("seconds_remaining") for l in lots], type=pa.int64()),
            "image_url":         pa.array([l.get("image_url")         for l in lots], type=pa.string()),
            "url":               pa.array([l.get("url")               for l in lots], type=pa.string()),
            "scraped_at":        pa.array([scraped_at] * len(lots),                   type=pa.timestamp("us", tz="UTC")),
        },
        schema=LOTS_SCHEMA,
    )


# ---------------------------------------------------------------------------
# Write
# ---------------------------------------------------------------------------

def _sql_val(v) -> str:
    """Render a Python value as a SQL literal."""
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, int):
        return str(v)
    if isinstance(v, datetime):
        return f"'{v.strftime('%Y-%m-%d %H:%M:%S')}'"
    return "'" + str(v).replace("'", "''") + "'"


def _insert(conn: adbc.dbapi.Connection, table_ref: str, columns: list[str], rows: list[list], batch_size: int = 200) -> int:
    """Execute batched INSERT INTO via executescript (CommandStatementUpdate → session context)."""
    col_list = ", ".join(columns)
    total = 0
    with conn.cursor() as cur:
        for i in range(0, len(rows), batch_size):
            batch = rows[i : i + batch_size]
            values = ", ".join("(" + ", ".join(_sql_val(v) for v in row) + ")" for row in batch)
            cur.executescript(f"INSERT INTO {table_ref} ({col_list}) VALUES {values}")
            total += len(batch)
    return total


def write_auctions(conn: adbc.dbapi.Connection, config: DBConfig, auctions: list[dict], mode: str = "append") -> int:
    """Bulk-insert auctions via executescript (INSERT INTO). Returns row count written."""
    if not auctions:
        return 0
    scraped_at = _now()
    table_ref = f"{_schema_ref(config)}.auctions"
    columns = ["auction_id", "title", "date", "state", "auctioneer", "auction_type",
               "is_live", "image_url", "details_url", "lots_url", "scraped_at"]
    rows = [[
        a.get("auction_id"), a.get("title"), a.get("date"), a.get("state"),
        a.get("auctioneer"), a.get("auction_type"), bool(a.get("is_live")),
        a.get("image_url"), a.get("details_url"), a.get("lots_url"), scraped_at,
    ] for a in auctions]
    return _insert(conn, table_ref, columns, rows)


def write_lots(conn: adbc.dbapi.Connection, config: DBConfig, lots: list[dict], auction_id: str, mode: str = "append") -> int:
    """Bulk-insert lots via executescript (INSERT INTO). Returns row count written."""
    if not lots:
        return 0
    scraped_at = _now()
    table_ref = f"{_schema_ref(config)}.lots"
    columns = ["lot_id", "auction_id", "lot_number", "title", "current_bid",
               "time_remaining", "seconds_remaining", "image_url", "url", "scraped_at"]
    rows = [[
        l.get("lot_id"), auction_id, l.get("lot_number"), l.get("title"),
        l.get("current_bid"), l.get("time_remaining"), l.get("seconds_remaining"),
        l.get("image_url"), l.get("url"), scraped_at,
    ] for l in lots]
    return _insert(conn, table_ref, columns, rows)
