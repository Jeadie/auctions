use std::collections::HashSet;
use std::thread;
use std::time::{Duration, Instant};

use auctions::db;
use auctions::error::Result;
use auctions::scraper;
use clap::{Args, Parser};
use tracing::level_filters::LevelFilter;

#[derive(Debug, Parser)]
#[command(
    name = "auctions-sync",
    about = "Continuously scrape auctions and lots into the database",
    version,
    after_help = "
EXAMPLES:
  auctions-sync
  auctions-sync --interval-seconds 30
  auctions-sync --aid 67956 --aid 67957
  auctions-sync --adbc-uri grpc://localhost:50051 --schema auctions_data"
)]
struct Cli {
    /// Increase log verbosity (-v = INFO, -vv = DEBUG)
    #[arg(short, long, action = clap::ArgAction::Count, global = true)]
    verbose: u8,

    /// Suppress progress messages on stderr
    #[arg(short, long, global = true)]
    quiet: bool,

    /// Run one cycle and exit
    #[arg(long)]
    once: bool,

    /// Seconds between scrape cycles
    #[arg(long, default_value_t = 60, value_name = "SECONDS", value_parser = clap::value_parser!(u64).range(1..))]
    interval_seconds: u64,

    /// Maximum lots to fetch per page
    #[arg(long, default_value_t = 100, value_name = "N", value_parser = clap::value_parser!(u32).range(1..=500))]
    page_size: u32,

    /// Restrict processing to one or more auction IDs
    #[arg(long, value_name = "AID")]
    aid: Vec<u64>,

    /// Optional cap on number of auctions processed each cycle
    #[arg(long, value_name = "N")]
    max_auctions: Option<usize>,

    #[command(flatten)]
    db: DbArgs,
}

#[derive(Debug, Args)]
struct DbArgs {
    /// ADBC connection URI
    #[arg(long, value_name = "URI")]
    adbc_uri: Option<String>,

    /// ADBC driver name or path
    #[arg(long, value_name = "DRIVER")]
    adbc_driver: Option<String>,

    /// ADBC init options as a JSON object
    #[arg(long, value_name = "JSON")]
    adbc_options: Option<String>,

    /// Catalog name for writes
    #[arg(long, value_name = "CATALOG")]
    catalog: Option<String>,

    /// Schema name for writes
    #[arg(long, value_name = "SCHEMA", default_value = "public")]
    schema: String,
}

#[derive(Debug, Default)]
struct CycleStats {
    auctions_written: usize,
    auctions_processed: usize,
    auctions_skipped: usize,
    lots_seen: usize,
    lots_appended: usize,
    lot_price_rows: usize,
}

fn main() {
    let cli = Cli::parse();

    let level = match cli.verbose {
        0 => LevelFilter::WARN,
        1 => LevelFilter::INFO,
        _ => LevelFilter::DEBUG,
    };

    tracing_subscriber::fmt()
        .with_max_level(level)
        .with_writer(std::io::stderr)
        .without_time()
        .init();

    if let Err(e) = run(cli) {
        eprintln!("{}: {e}", styled_error());
        std::process::exit(1);
    }
}

fn styled_error() -> &'static str {
    if std::io::IsTerminal::is_terminal(&std::io::stderr())
        && std::env::var_os("NO_COLOR").is_none()
    {
        "\x1b[1;31merror\x1b[0m"
    } else {
        "error"
    }
}

fn status(quiet: bool, message: impl std::fmt::Display) {
    if !quiet {
        eprintln!("{message}");
    }
}

fn run(cli: Cli) -> Result<()> {
    let cfg = db::DbConfig::from_parts(
        cli.db.adbc_driver.as_deref(),
        cli.db.adbc_uri.as_deref(),
        cli.db.adbc_options.as_deref(),
        cli.db.catalog.as_deref(),
        Some(&cli.db.schema),
    )?;
    let target = cfg.to_string();
    status(cli.quiet, format!("Connecting to {target} …"));

    let mut database = db::Db::connect(cfg)?;
    database.setup()?;

    let client = scraper::LloydsClient::new()?;
    let interval = Duration::from_secs(cli.interval_seconds);

    loop {
        let cycle_started = Instant::now();

        match run_cycle(&client, &mut database, &cli) {
            Ok(stats) => {
                status(
                    cli.quiet,
                    format!(
                        "Cycle complete: {} auctions updated, {} auctions processed, {} skipped, {} lots seen, {} new lots appended, {} lot-price rows written",
                        stats.auctions_written,
                        stats.auctions_processed,
                        stats.auctions_skipped,
                        stats.lots_seen,
                        stats.lots_appended,
                        stats.lot_price_rows
                    ),
                );
            }
            Err(err) => {
                status(cli.quiet, format!("Cycle failed: {err}"));
            }
        }

        if cli.once {
            break;
        }

        let elapsed = cycle_started.elapsed();
        if elapsed < interval {
            thread::sleep(interval - elapsed);
        }
    }

    Ok(())
}

fn run_cycle(
    client: &scraper::LloydsClient,
    database: &mut db::Db,
    cli: &Cli,
) -> Result<CycleStats> {
    status(cli.quiet, "Fetching auction list …");
    let auctions = client.scrape_auctions()?;

    let aid_filter = auction_id_filter(&cli.aid);
    let mut selected = auctions
        .into_iter()
        .filter(|auction| {
            aid_filter
                .as_ref()
                .is_none_or(|set| set.contains(&auction.auction_id))
        })
        .collect::<Vec<_>>();

    if let Some(limit) = cli.max_auctions {
        selected.truncate(limit);
    }

    let auctions_written = database.write_auctions(&selected)?;

    let mut stats = CycleStats {
        auctions_written,
        ..CycleStats::default()
    };

    for auction in selected {
        let Some(auctioneer) = auction.auctioneer.filter(|value| !value.trim().is_empty()) else {
            stats.auctions_skipped += 1;
            tracing::warn!(auction_id = %auction.auction_id, "skipping lots scrape: auctioneer missing");
            continue;
        };

        let aid = match auction.auction_id.parse::<u64>() {
            Ok(aid) => aid,
            Err(_) => {
                stats.auctions_skipped += 1;
                tracing::warn!(auction_id = %auction.auction_id, "skipping lots scrape: auction ID is not numeric");
                continue;
            }
        };

        let scraped = match client.scrape_lots(aid, cli.page_size) {
            Ok(scraped) => scraped,
            Err(err) => {
                stats.auctions_skipped += 1;
                tracing::warn!(auction_id = %auction.auction_id, error = %err, "failed scraping lots; continuing");
                continue;
            }
        };

        let mut lots = scraped.lots;
        for lot in &mut lots {
            lot.auctioneer = Some(auctioneer.clone());
        }

        stats.lots_seen += lots.len();
        stats.lots_appended += database.append_new_lots(&lots)?;
        stats.lot_price_rows += database.append_lot_prices(&lots)?;
        stats.auctions_processed += 1;
    }

    Ok(stats)
}

fn auction_id_filter(aid: &[u64]) -> Option<HashSet<String>> {
    if aid.is_empty() {
        None
    } else {
        Some(aid.iter().map(u64::to_string).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::{Cli, auction_id_filter};
    use clap::Parser;

    #[test]
    fn aid_filter_is_none_when_no_ids_are_provided() {
        assert!(auction_id_filter(&[]).is_none());
    }

    #[test]
    fn aid_filter_contains_stringified_ids() {
        let filter = auction_id_filter(&[67956, 67957]).expect("filter should exist");
        assert!(filter.contains("67956"));
        assert!(filter.contains("67957"));
    }

    #[test]
    fn cli_defaults_are_stable() {
        let cli = Cli::parse_from(["auctions-sync"]);
        assert_eq!(cli.interval_seconds, 60);
        assert_eq!(cli.page_size, 100);
        assert_eq!(cli.db.schema, "public");
    }
}
