use std::collections::{BTreeSet, HashMap};

use adbc_core::{
    Connection, Database, Driver, LOAD_FLAG_DEFAULT, Statement,
    options::{AdbcVersion, OptionDatabase, OptionValue},
};
use adbc_driver_manager::ManagedDriver;
use chrono::Utc;

use crate::error::{Error, Result};
use crate::models::{Auction, Lot};

const AUTH_HEADER_KEY: &str = "adbc.flight.sql.authorization_header";
const DEFAULT_ADBC_DRIVER: &str = "adbc_driver_flightsql";
const DEFAULT_ADBC_URI: &str = "grpc://localhost:50051";
const DEFAULT_SCHEMA: &str = "public";
const BATCH_SIZE: usize = 200;

pub struct DbConfig {
    pub driver: String,
    pub uri: String,
    pub options: HashMap<String, String>,
    pub catalog: Option<String>,
    pub schema: String,
}

impl DbConfig {
    pub fn from_parts(
        adbc_driver: Option<&str>,
        adbc_uri: Option<&str>,
        adbc_options_json: Option<&str>,
        catalog: Option<&str>,
        schema: Option<&str>,
    ) -> Result<Self> {
        let options = match adbc_options_json.filter(|json| !json.trim().is_empty()) {
            None => HashMap::new(),
            Some(json) => {
                serde_json::from_str(json).map_err(|e| Error::AdbcOptionsJson { source: e })?
            }
        };

        Ok(Self {
            driver: adbc_driver
                .filter(|value| !value.trim().is_empty())
                .unwrap_or(DEFAULT_ADBC_DRIVER)
                .to_owned(),
            uri: adbc_uri
                .filter(|value| !value.trim().is_empty())
                .unwrap_or(DEFAULT_ADBC_URI)
                .to_owned(),
            options,
            catalog: catalog
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_owned),
            schema: schema
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .unwrap_or(DEFAULT_SCHEMA)
                .to_owned(),
        })
    }

    fn schema_ref(&self) -> String {
        match &self.catalog {
            Some(cat) => format!("{cat}.{}", self.schema),
            None => self.schema.clone(),
        }
    }

    fn schema_ref_quoted(&self) -> String {
        match &self.catalog {
            Some(cat) => format!("{}.{}", quote_ident(cat), quote_ident(&self.schema)),
            None => quote_ident(&self.schema),
        }
    }

    fn table_ref_quoted(&self, table_name: &str) -> String {
        format!("{}.{}", self.schema_ref_quoted(), quote_ident(table_name))
    }

    fn display(&self) -> String {
        let cat = self
            .catalog
            .as_deref()
            .map(|c| format!("{c}."))
            .unwrap_or_default();
        format!("{} [{cat}{}]", self.uri, self.schema)
    }
}

pub struct Db {
    conn: adbc_driver_manager::ManagedConnection,
    config: DbConfig,
}

impl Db {
    /// Open an ADBC connection. Bare tokens in `authorization_header` are
    /// automatically prefixed with `Bearer `.
    pub fn connect(mut config: DbConfig) -> Result<Self> {
        if let Some(val) = config.options.get_mut(AUTH_HEADER_KEY) {
            let lower = val.to_lowercase();
            if !lower.starts_with("bearer ") && !lower.starts_with("basic ") {
                *val = format!("Bearer {val}");
            }
        }

        tracing::debug!(uri = %config.uri, driver = %config.driver, "opening ADBC connection");

        let mut all_opts = HashMap::new();
        all_opts.insert("uri".to_owned(), config.uri.clone());
        all_opts.extend(config.options.clone());

        let adbc_opts: Vec<(OptionDatabase, OptionValue)> = all_opts
            .iter()
            .map(|(k, v)| {
                (
                    OptionDatabase::from(k.as_str()),
                    OptionValue::from(v.as_str()),
                )
            })
            .collect();

        let mut driver = ManagedDriver::load_from_name(
            &config.driver,
            None,
            AdbcVersion::V100,
            LOAD_FLAG_DEFAULT,
            None,
        )
        .map_err(|e| Error::DbDriver {
            driver: config.driver.clone(),
            message: e.to_string(),
        })?;

        let db = driver
            .new_database_with_opts(adbc_opts)
            .map_err(|e| Error::DbConnect {
                uri: config.uri.clone(),
                message: e.to_string(),
            })?;

        let conn = db.new_connection().map_err(|e| Error::DbConnect {
            uri: config.uri.clone(),
            message: e.to_string(),
        })?;

        Ok(Self { conn, config })
    }

    fn execute_update(&mut self, sql: &str) -> std::result::Result<u64, String> {
        let mut stmt = self.conn.new_statement().map_err(|e| e.to_string())?;
        stmt.set_sql_query(sql).map_err(|e| e.to_string())?;
        let affected = stmt.execute_update().map_err(|e| e.to_string())?;
        Ok(affected.and_then(|n| u64::try_from(n).ok()).unwrap_or(0))
    }

    fn execute_setup(&mut self, sql: &str) -> Result<()> {
        self.execute_update(sql).map_err(|message| Error::DbSetup {
            query: truncate_sql(sql),
            message,
        })?;
        Ok(())
    }

    pub fn setup(&mut self) -> Result<()> {
        let schema_ref = self.config.schema_ref_quoted();
        tracing::debug!(schema = %self.config.schema_ref(), "running setup DDL");

        self.execute_setup(&format!("CREATE SCHEMA IF NOT EXISTS {schema_ref}"))?;

        self.execute_setup(&format!(
            "CREATE TABLE IF NOT EXISTS {} (
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
                scraped_at   TIMESTAMP,
                PRIMARY KEY (auction_id)
            )",
            self.config.table_ref_quoted("auctions")
        ))?;

        self.execute_setup(&format!(
            "CREATE TABLE IF NOT EXISTS {} (
                lot_id       VARCHAR NOT NULL,
                auction_id   VARCHAR NOT NULL,
                auctioneer   VARCHAR NOT NULL,
                lot_number   VARCHAR,
                title        VARCHAR,
                image_url    VARCHAR,
                description  VARCHAR,
                location     VARCHAR,
                lot_images   VARCHAR[],
                url          VARCHAR,
                scraped_at   TIMESTAMP,
                PRIMARY KEY (auctioneer, auction_id, lot_id)
            )",
            self.config.table_ref_quoted("lots")
        ))?;

        self.execute_setup(&format!(
            "CREATE TABLE IF NOT EXISTS {} (
                auctioneer  VARCHAR NOT NULL,
                auction_id  VARCHAR NOT NULL,
                lot_id      VARCHAR NOT NULL,
                bid         DOUBLE,
                scraped_at  TIMESTAMP NOT NULL
            )",
            self.config.table_ref_quoted("lot_prices")
        ))?;

        Ok(())
    }

    pub fn write_auctions(&mut self, auctions: &[Auction]) -> Result<usize> {
        if auctions.is_empty() {
            return Ok(0);
        }

        let table_ref = self.config.table_ref_quoted("auctions");
        let table_label = format!("{}.auctions", self.config.schema_ref());

        let cols = "auction_id, title, date, state, auctioneer, auction_type, is_live, image_url, details_url, lots_url, scraped_at";
        let now = Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();

        for chunk in auctions.chunks(BATCH_SIZE) {
            if let Some(delete_sql) = delete_auction_keys_sql(&table_ref, chunk) {
                self.execute_update(&delete_sql)
                    .map_err(|message| Error::DbWrite {
                        rows: chunk.len(),
                        table: table_label.clone(),
                        message,
                    })?;
            }

            let values: Vec<String> = chunk
                .iter()
                .map(|a| {
                    format!(
                        "({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, '{now}')",
                        lit(&a.auction_id),
                        lit_opt(a.title.as_deref()),
                        lit_opt(a.date.as_deref()),
                        lit_opt(a.state.as_deref()),
                        lit_opt(a.auctioneer.as_deref()),
                        lit_opt(a.auction_type.as_deref()),
                        lit_bool(a.is_live),
                        lit_opt(a.image_url.as_deref()),
                        lit(&a.details_url),
                        lit(&a.lots_url)
                    )
                })
                .collect();

            self.execute_update(&format!(
                "INSERT INTO {table_ref} ({cols}) VALUES {}",
                values.join(", ")
            ))
            .map_err(|message| Error::DbWrite {
                rows: chunk.len(),
                table: table_label.clone(),
                message,
            })?;
        }

        tracing::info!(rows = auctions.len(), table = %table_label, "wrote auctions");
        Ok(auctions.len())
    }

    pub fn write_lots(&mut self, lots: &[Lot]) -> Result<usize> {
        if lots.is_empty() {
            return Ok(0);
        }

        let table_ref = self.config.table_ref_quoted("lots");
        let table_label = format!("{}.lots", self.config.schema_ref());

        let cols = "lot_id, auction_id, auctioneer, lot_number, title, image_url, description, location, lot_images, url, scraped_at";
        let now = Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();

        for chunk in lots.chunks(BATCH_SIZE) {
            let validated = chunk
                .iter()
                .map(|lot| {
                    let auctioneer = ensure_lot_has_auctioneer(lot, &table_label)?;
                    Ok((lot, auctioneer))
                })
                .collect::<Result<Vec<_>>>()?;

            if let Some(delete_sql) = delete_lot_keys_sql(&table_ref, chunk) {
                self.execute_update(&delete_sql)
                    .map_err(|message| Error::DbWrite {
                        rows: chunk.len(),
                        table: table_label.clone(),
                        message,
                    })?;
            }

            let values = validated
                .iter()
                .map(|(lot, auctioneer)| format_lot_values(lot, auctioneer, &now))
                .collect::<Vec<_>>();

            self.execute_update(&format!(
                "INSERT INTO {table_ref} ({cols}) VALUES {}",
                values.join(", ")
            ))
            .map_err(|message| Error::DbWrite {
                rows: chunk.len(),
                table: table_label.clone(),
                message,
            })?;
        }

        tracing::info!(rows = lots.len(), table = %table_label, "wrote lots");
        Ok(lots.len())
    }

    /// Append lots without deleting existing rows.
    ///
    /// On runtimes that enforce the `(auctioneer, auction_id, lot_id)` primary key,
    /// existing lots are naturally ignored while new lots are inserted.
    pub fn append_new_lots(&mut self, lots: &[Lot]) -> Result<usize> {
        if lots.is_empty() {
            return Ok(0);
        }

        let table_ref = self.config.table_ref_quoted("lots");
        let table_label = format!("{}.lots", self.config.schema_ref());
        let cols = "lot_id, auction_id, auctioneer, lot_number, title, image_url, description, location, lot_images, url, scraped_at";
        let now = Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();

        let mut inserted = 0usize;

        for chunk in lots.chunks(BATCH_SIZE) {
            let values = chunk
                .iter()
                .map(|lot| {
                    let auctioneer = ensure_lot_has_auctioneer(lot, &table_label)?;
                    Ok(format_lot_values(lot, auctioneer, &now))
                })
                .collect::<Result<Vec<_>>>()?;

            let affected = self
                .execute_update(&format!(
                    "INSERT INTO {table_ref} ({cols}) VALUES {}",
                    values.join(", ")
                ))
                .map_err(|message| Error::DbWrite {
                    rows: chunk.len(),
                    table: table_label.clone(),
                    message,
                })?;

            inserted += affected as usize;
        }

        tracing::info!(
            attempted = lots.len(),
            inserted,
            table = %table_label,
            "appended new lots"
        );
        Ok(inserted)
    }

    /// Append a price snapshot for every lot in this scrape pass.
    pub fn append_lot_prices(&mut self, lots: &[Lot]) -> Result<usize> {
        if lots.is_empty() {
            return Ok(0);
        }

        let table_ref = self.config.table_ref_quoted("lot_prices");
        let table_label = format!("{}.lot_prices", self.config.schema_ref());
        let cols = "auctioneer, auction_id, lot_id, bid, scraped_at";
        let now = Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();

        for chunk in lots.chunks(BATCH_SIZE) {
            let values = chunk
                .iter()
                .map(|lot| {
                    let auctioneer = ensure_lot_has_auctioneer(lot, &table_label)?;

                    Ok(format!(
                        "({}, {}, {}, {}, '{now}')",
                        lit(auctioneer),
                        lit(&lot.auction_id),
                        lit(&lot.lot_id),
                        lit_opt_f64(lot.current_bid),
                    ))
                })
                .collect::<Result<Vec<_>>>()?;

            self.execute_update(&format!(
                "INSERT INTO {table_ref} ({cols}) VALUES {}",
                values.join(", ")
            ))
            .map_err(|message| Error::DbWrite {
                rows: chunk.len(),
                table: table_label.clone(),
                message,
            })?;
        }

        tracing::info!(rows = lots.len(), table = %table_label, "wrote lot price snapshots");
        Ok(lots.len())
    }
}

impl std::fmt::Display for DbConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.display())
    }
}

fn ensure_lot_has_auctioneer<'a>(lot: &'a Lot, table_label: &str) -> Result<&'a str> {
    lot.auctioneer
        .as_deref()
        .filter(|auctioneer| !auctioneer.is_empty())
        .ok_or_else(|| Error::MissingAuctioneer {
            table: table_label.to_owned(),
            auction_id: lot.auction_id.clone(),
            lot_id: lot.lot_id.clone(),
        })
}

fn format_lot_values(lot: &Lot, auctioneer: &str, now: &str) -> String {
    format!(
        "({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, '{now}')",
        lit(&lot.lot_id),
        lit(&lot.auction_id),
        lit(auctioneer),
        lit_opt(lot.lot_number.as_deref()),
        lit_opt(lot.title.as_deref()),
        lit_opt(lot.image_url.as_deref()),
        lit_opt(lot.description.as_deref()),
        lit_opt(lot.location.as_deref()),
        lit_opt_array_of_strings(&lot.lot_images),
        lit(&lot.url)
    )
}

fn delete_auction_keys_sql(table_ref: &str, auctions: &[Auction]) -> Option<String> {
    let ids: BTreeSet<String> = auctions.iter().map(|a| a.auction_id.clone()).collect();
    if ids.is_empty() {
        return None;
    }

    let quoted_ids = ids.into_iter().map(|id| lit(&id)).collect::<Vec<_>>();
    Some(format!(
        "DELETE FROM {table_ref} WHERE auction_id IN ({})",
        quoted_ids.join(", ")
    ))
}

fn delete_lot_keys_sql(table_ref: &str, lots: &[Lot]) -> Option<String> {
    let keys: BTreeSet<(String, String, String)> = lots
        .iter()
        .filter_map(|l| {
            l.auctioneer
                .as_ref()
                .map(|auctioneer| (auctioneer.clone(), l.auction_id.clone(), l.lot_id.clone()))
        })
        .collect();
    if keys.is_empty() {
        return None;
    }

    let predicates = keys
        .into_iter()
        .map(|(auctioneer, auction_id, lot_id)| {
            format!(
                "(auctioneer = {} AND auction_id = {} AND lot_id = {})",
                lit(&auctioneer),
                lit(&auction_id),
                lit(&lot_id)
            )
        })
        .collect::<Vec<_>>();

    Some(format!(
        "DELETE FROM {table_ref} WHERE {}",
        predicates.join(" OR ")
    ))
}

fn quote_ident(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn lit(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn lit_opt(s: Option<&str>) -> String {
    match s {
        None => "NULL".to_owned(),
        Some(s) => lit(s),
    }
}

fn lit_opt_f64(n: Option<f64>) -> String {
    match n {
        None => "NULL".to_owned(),
        Some(n) => n.to_string(),
    }
}

fn lit_opt_array_of_strings(values: &[String]) -> String {
    if values.is_empty() {
        return "NULL".to_owned();
    }

    let elements = values
        .iter()
        .map(|value| lit(value))
        .collect::<Vec<_>>()
        .join(", ");

    format!("array({elements})")
}

fn lit_bool(b: bool) -> &'static str {
    if b { "TRUE" } else { "FALSE" }
}

fn truncate_sql(sql: &str) -> String {
    let trimmed = sql.trim();
    if trimmed.len() > 80 {
        format!("{}…", &trimmed[..80])
    } else {
        trimmed.to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DbConfig, delete_auction_keys_sql, delete_lot_keys_sql, ensure_lot_has_auctioneer,
        format_lot_values, lit, lit_bool, lit_opt, lit_opt_array_of_strings, lit_opt_f64,
        quote_ident,
    };
    use crate::error::Error;
    use crate::models::{Auction, Lot};

    #[test]
    fn db_config_from_parts_applies_defaults() {
        let cfg = DbConfig::from_parts(None, None, None, None, None).expect("config should build");
        assert_eq!(cfg.driver, "adbc_driver_flightsql");
        assert_eq!(cfg.uri, "grpc://localhost:50051");
        assert_eq!(cfg.schema, "public");
        assert!(cfg.catalog.is_none());
    }

    #[test]
    fn db_config_from_parts_parses_options_json() {
        let cfg = DbConfig::from_parts(
            None,
            None,
            Some("{\"adbc.flight.sql.authorization_header\":\"abc\"}"),
            Some("spice"),
            Some("auctions_data"),
        )
        .expect("config should build");

        assert_eq!(
            cfg.options.get("adbc.flight.sql.authorization_header"),
            Some(&"abc".to_owned())
        );
        assert_eq!(cfg.catalog.as_deref(), Some("spice"));
        assert_eq!(cfg.schema, "auctions_data");
    }

    #[test]
    fn quote_ident_escapes_double_quotes() {
        assert_eq!(quote_ident("public"), "\"public\"");
        assert_eq!(quote_ident("my\"schema"), "\"my\"\"schema\"");
    }

    #[test]
    fn string_literals_escape_single_quotes() {
        assert_eq!(lit("bob's"), "'bob''s'");
        assert_eq!(lit_opt(Some("ok")), "'ok'");
        assert_eq!(lit_opt(None), "NULL");
    }

    #[test]
    fn string_array_literals_render_as_arrow_list_expressions() {
        let lit = lit_opt_array_of_strings(&["a".to_owned(), "b".to_owned()]);
        assert_eq!(lit, "array('a', 'b')");
        assert_eq!(lit_opt_array_of_strings(&[]), "NULL");
    }

    #[test]
    fn boolean_literals_render_cleanly() {
        assert_eq!(lit_bool(true), "TRUE");
        assert_eq!(lit_bool(false), "FALSE");
        assert_eq!(lit_opt_f64(Some(12.5)), "12.5");
        assert_eq!(lit_opt_f64(None), "NULL");
    }

    #[test]
    fn delete_auction_keys_sql_deduplicates_ids() {
        let auctions = vec![
            Auction {
                auction_id: "100".to_owned(),
                title: None,
                date: None,
                state: None,
                auctioneer: None,
                auction_type: None,
                is_live: false,
                image_url: None,
                details_url: "https://example.com/a/100".to_owned(),
                lots_url: "https://example.com/l/100".to_owned(),
            },
            Auction {
                auction_id: "100".to_owned(),
                title: None,
                date: None,
                state: None,
                auctioneer: None,
                auction_type: None,
                is_live: false,
                image_url: None,
                details_url: "https://example.com/a/100".to_owned(),
                lots_url: "https://example.com/l/100".to_owned(),
            },
            Auction {
                auction_id: "200".to_owned(),
                title: None,
                date: None,
                state: None,
                auctioneer: None,
                auction_type: None,
                is_live: false,
                image_url: None,
                details_url: "https://example.com/a/200".to_owned(),
                lots_url: "https://example.com/l/200".to_owned(),
            },
        ];

        let sql = delete_auction_keys_sql("\"s\".\"auctions\"", &auctions)
            .expect("sql should be generated");
        assert_eq!(
            sql,
            "DELETE FROM \"s\".\"auctions\" WHERE auction_id IN ('100', '200')"
        );
    }

    #[test]
    fn delete_lot_keys_sql_builds_composite_predicates() {
        let lots = vec![
            Lot {
                lot_id: "1".to_owned(),
                auction_id: "A".to_owned(),
                auctioneer: Some("Lloyds".to_owned()),
                lot_number: None,
                title: None,
                current_bid: None,
                time_remaining: None,
                seconds_remaining: None,
                image_url: None,
                description: None,
                location: None,
                lot_images: Vec::new(),
                url: "https://example.com/l/1".to_owned(),
            },
            Lot {
                lot_id: "2".to_owned(),
                auction_id: "A".to_owned(),
                auctioneer: Some("Lloyds".to_owned()),
                lot_number: None,
                title: None,
                current_bid: None,
                time_remaining: None,
                seconds_remaining: None,
                image_url: None,
                description: None,
                location: None,
                lot_images: Vec::new(),
                url: "https://example.com/l/2".to_owned(),
            },
        ];

        let sql = delete_lot_keys_sql("\"s\".\"lots\"", &lots).expect("sql should be generated");
        assert_eq!(
            sql,
            "DELETE FROM \"s\".\"lots\" WHERE (auctioneer = 'Lloyds' AND auction_id = 'A' AND lot_id = '1') OR (auctioneer = 'Lloyds' AND auction_id = 'A' AND lot_id = '2')"
        );
    }

    #[test]
    fn ensure_lot_has_auctioneer_returns_typed_error() {
        let lot = Lot {
            lot_id: "123".to_owned(),
            auction_id: "A1".to_owned(),
            auctioneer: None,
            lot_number: Some("12".to_owned()),
            title: Some("Vintage Guitar".to_owned()),
            current_bid: Some(100.0),
            time_remaining: Some("1h".to_owned()),
            seconds_remaining: Some(3600),
            image_url: Some("https://example.com/l.png".to_owned()),
            description: None,
            location: None,
            lot_images: Vec::new(),
            url: "https://example.com/lot/123".to_owned(),
        };

        let err = ensure_lot_has_auctioneer(&lot, "public.lots").expect_err("should fail");
        assert!(matches!(
            err,
            Error::MissingAuctioneer {
                table,
                auction_id,
                lot_id
            } if table == "public.lots" && auction_id == "A1" && lot_id == "123"
        ));
    }

    #[test]
    fn format_lot_values_renders_array_literal_for_images() {
        let lot = Lot {
            lot_id: "123".to_owned(),
            auction_id: "A1".to_owned(),
            auctioneer: Some("Lloyds".to_owned()),
            lot_number: Some("12".to_owned()),
            title: Some("Vintage Guitar".to_owned()),
            current_bid: Some(100.0),
            time_remaining: Some("1h".to_owned()),
            seconds_remaining: Some(3600),
            image_url: Some("https://example.com/l.png".to_owned()),
            description: Some("Near-new with extras".to_owned()),
            location: Some("Melbourne, VIC".to_owned()),
            lot_images: vec![
                "https://example.com/l-1.png".to_owned(),
                "https://example.com/l-2.png".to_owned(),
            ],
            url: "https://example.com/lot/123".to_owned(),
        };

        let sql_values = format_lot_values(&lot, "Lloyds", "2026-04-16 12:00:00");

        assert!(
            sql_values
                .contains("array('https://example.com/l-1.png', 'https://example.com/l-2.png')")
        );
        assert!(sql_values.contains("'Near-new with extras'"));
        assert!(sql_values.contains("'Melbourne, VIC'"));
    }
}
