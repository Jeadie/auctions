use std::collections::{BTreeSet, HashMap};

use adbc_core::{
    Connection, Database, Driver, LOAD_FLAG_DEFAULT, Statement,
    options::{AdbcVersion, OptionDatabase, OptionValue},
};
use adbc_driver_manager::ManagedDriver;
use chrono::Utc;

use crate::cli::DbArgs;
use crate::error::{Error, Result};
use crate::models::{Auction, Lot};

const AUTH_HEADER_KEY: &str = "adbc.flight.sql.authorization_header";
const BATCH_SIZE: usize = 200;

pub struct DbConfig {
    pub driver: String,
    pub uri: String,
    pub options: HashMap<String, String>,
    pub catalog: Option<String>,
    pub schema: String,
}

impl DbConfig {
    pub fn from_args(args: &DbArgs) -> Result<Self> {
        let options = match &args.adbc_options {
            None => HashMap::new(),
            Some(json) => {
                serde_json::from_str(json).map_err(|e| Error::AdbcOptionsJson { source: e })?
            }
        };

        Ok(Self {
            driver: args
                .adbc_driver
                .clone()
                .unwrap_or_else(|| "adbc_driver_flightsql".to_owned()),
            uri: args
                .adbc_uri
                .clone()
                .unwrap_or_else(|| "grpc://localhost:50051".to_owned()),
            options,
            catalog: args.catalog.clone(),
            schema: args.schema.clone(),
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

    fn execute_update(&mut self, sql: &str) -> std::result::Result<(), String> {
        let mut stmt = self.conn.new_statement().map_err(|e| e.to_string())?;
        stmt.set_sql_query(sql).map_err(|e| e.to_string())?;
        stmt.execute_update().map_err(|e| e.to_string())?;
        Ok(())
    }

    fn execute_setup(&mut self, sql: &str) -> Result<()> {
        self.execute_update(sql).map_err(|message| Error::DbSetup {
            query: truncate_sql(sql),
            message,
        })
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
                lot_id            VARCHAR NOT NULL,
                auction_id        VARCHAR NOT NULL,
                auctioneer        VARCHAR NOT NULL,
                lot_number        VARCHAR,
                title             VARCHAR,
                current_bid       VARCHAR,
                time_remaining    VARCHAR,
                seconds_remaining BIGINT,
                image_url         VARCHAR,
                url               VARCHAR,
                scraped_at        TIMESTAMP,
                PRIMARY KEY (auctioneer, auction_id, lot_id)
            )",
            self.config.table_ref_quoted("lots")
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

        let cols = "lot_id, auction_id, auctioneer, lot_number, title, current_bid, time_remaining, seconds_remaining, image_url, url, scraped_at";
        let now = Utc::now().format("%Y-%m-%d %H:%M:%S").to_string();

        for chunk in lots.chunks(BATCH_SIZE) {
            for lot in chunk {
                if lot.auctioneer.as_deref().is_none_or(str::is_empty) {
                    return Err(Error::DbWrite {
                        rows: 1,
                        table: table_label.clone(),
                        message: format!(
                            "lot {} is missing auctioneer, required for primary key",
                            lot.lot_id
                        ),
                    });
                }
            }

            if let Some(delete_sql) = delete_lot_keys_sql(&table_ref, chunk) {
                self.execute_update(&delete_sql)
                    .map_err(|message| Error::DbWrite {
                        rows: chunk.len(),
                        table: table_label.clone(),
                        message,
                    })?;
            }

            let values: Vec<String> = chunk
                .iter()
                .map(|l| {
                    let auctioneer = match l.auctioneer.as_deref() {
                        Some(auctioneer) if !auctioneer.is_empty() => auctioneer,
                        _ => "",
                    };

                    format!(
                        "({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, '{now}')",
                        lit(&l.lot_id),
                        lit(&l.auction_id),
                        lit(auctioneer),
                        lit_opt(l.lot_number.as_deref()),
                        lit_opt(l.title.as_deref()),
                        lit_opt(l.current_bid.as_deref()),
                        lit_opt(l.time_remaining.as_deref()),
                        lit_opt_i64(l.seconds_remaining),
                        lit_opt(l.image_url.as_deref()),
                        lit(&l.url)
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

        tracing::info!(rows = lots.len(), table = %table_label, "wrote lots");
        Ok(lots.len())
    }
}

impl std::fmt::Display for DbConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.display())
    }
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

fn lit_bool(b: bool) -> &'static str {
    if b { "TRUE" } else { "FALSE" }
}

fn lit_opt_i64(n: Option<i64>) -> String {
    match n {
        None => "NULL".to_owned(),
        Some(n) => n.to_string(),
    }
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
        delete_auction_keys_sql, delete_lot_keys_sql, lit, lit_bool, lit_opt, lit_opt_i64,
        quote_ident,
    };
    use crate::models::{Auction, Lot};

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
    fn numeric_and_boolean_literals_render_cleanly() {
        assert_eq!(lit_bool(true), "TRUE");
        assert_eq!(lit_bool(false), "FALSE");
        assert_eq!(lit_opt_i64(Some(42)), "42");
        assert_eq!(lit_opt_i64(None), "NULL");
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
                url: "https://example.com/l/2".to_owned(),
            },
        ];

        let sql = delete_lot_keys_sql("\"s\".\"lots\"", &lots).expect("sql should be generated");
        assert_eq!(
            sql,
            "DELETE FROM \"s\".\"lots\" WHERE (auctioneer = 'Lloyds' AND auction_id = 'A' AND lot_id = '1') OR (auctioneer = 'Lloyds' AND auction_id = 'A' AND lot_id = '2')"
        );
    }
}
