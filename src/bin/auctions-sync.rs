use std::collections::HashSet;
use std::thread;
use std::time::{Duration, Instant};

use auctions::db;
use auctions::error::Result;
use auctions::models::Lot;
use auctions::scraper;
use clap::{Args, Parser};
use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;

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

    init_tracing(level);

    if let Err(e) = run(cli) {
        eprintln!("{}: {e}", styled_error());
        std::process::exit(1);
    }
}

fn init_tracing(level: LevelFilter) {
    let env_filter = EnvFilter::builder()
        .with_default_directive(level.into())
        .from_env_lossy()
        .add_directive(
            "html5ever::tree_builder=error"
                .parse()
                .expect("valid html5ever directive"),
        );

    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_writer(std::io::stderr)
        .without_time()
        .init();
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
        let cycle_result = run_cycle(&client, &mut database, &cli);
        let elapsed = cycle_started.elapsed();
        handle_cycle_result(cycle_result, cli.quiet, cli.once, elapsed)?;

        if cli.once {
            break;
        }

        if elapsed < interval {
            thread::sleep(interval - elapsed);
        }
    }

    Ok(())
}

fn handle_cycle_result(
    cycle_result: Result<CycleStats>,
    quiet: bool,
    once: bool,
    elapsed: Duration,
) -> Result<()> {
    match cycle_result {
        Ok(stats) => {
            status(
                quiet,
                format!(
                    "Cycle complete in {:.1}s: {} auctions updated, {} auctions processed, {} skipped, {} lots seen, {} new lots appended, {} lot-price rows written",
                    elapsed.as_secs_f64(),
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
            if once {
                return Err(err);
            }
            status(
                quiet,
                format!("Cycle failed after {:.1}s: {err}", elapsed.as_secs_f64()),
            );
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
    let total_auctions = auctions.len();

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

    status(
        cli.quiet,
        format!(
            "Fetched {} auctions; processing {} this cycle",
            total_auctions,
            selected.len()
        ),
    );

    let auctions_written = database.write_auctions(&selected)?;

    let mut stats = CycleStats {
        auctions_written,
        ..CycleStats::default()
    };

    let total_selected = selected.len();
    for (index, auction) in selected.into_iter().enumerate() {
        let ordinal = index + 1;
        if ordinal == 1 || ordinal % 25 == 0 || ordinal == total_selected {
            status(
                cli.quiet,
                format!(
                    "Processing auction {}/{} (aid={})",
                    ordinal, total_selected, auction.auction_id
                ),
            );
        }

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

        let scraped = match client.scrape_lots_light(aid, cli.page_size) {
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

        let existing_detail_state =
            database.lot_detail_state_for_auction(&auctioneer, &auction.auction_id)?;

        let mut new_lots = Vec::new();
        let mut refreshed_lots = Vec::new();
        let mut detail_scrape_attempts = 0usize;

        for lot in &mut lots {
            let existing = existing_detail_state.get(&lot.lot_id);
            let is_new_lot = existing.is_none();
            let should_scrape = lot_needs_detail_scrape(existing);
            let mut detail_scrape_ok = false;

            if should_scrape {
                detail_scrape_attempts += 1;
                match client.enrich_lot_with_details(&auction.auction_id, lot) {
                    Ok(()) => detail_scrape_ok = true,
                    Err(error) => {
                        tracing::warn!(
                            auction_id = %auction.auction_id,
                            lot_id = %lot.lot_id,
                            error = %error,
                            "failed to scrape lot detail; keeping list-page fields"
                        );
                    }
                }
            }

            if is_new_lot {
                new_lots.push(lot.clone());
            } else if should_scrape
                && detail_scrape_ok
                && let Some(existing) = existing
            {
                if can_refresh_existing_lot(existing, lot) {
                    refreshed_lots.push(lot.clone());
                } else {
                    tracing::debug!(
                        auction_id = %auction.auction_id,
                        lot_id = %lot.lot_id,
                        "skipping lot refresh to avoid overwriting existing detail fields"
                    );
                }
            }
        }

        stats.lots_seen += lots.len();
        stats.lots_appended += database.append_new_lots(&new_lots)?;
        if !refreshed_lots.is_empty() {
            let refreshed = database.write_lots(&refreshed_lots)?;
            tracing::info!(auction_id = %auction.auction_id, rows = refreshed, "refreshed lots missing detail fields");
        }

        tracing::debug!(
            auction_id = %auction.auction_id,
            total_lots = lots.len(),
            new_lots = new_lots.len(),
            detail_scrape_attempts,
            detail_refreshes = refreshed_lots.len(),
            "auction lot processing summary"
        );

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

fn lot_needs_detail_scrape(state: Option<&db::LotDetailState>) -> bool {
    match state {
        None => true,
        Some(state) => !state.has_description || !state.has_images,
    }
}

fn can_refresh_existing_lot(existing: &db::LotDetailState, scraped: &Lot) -> bool {
    if existing.has_description && scraped.description.is_none() {
        return false;
    }

    if existing.has_images && scraped.lot_images.is_empty() {
        return false;
    }

    true
}

#[cfg(test)]
mod tests {
    use super::{
        Cli, CycleStats, auction_id_filter, can_refresh_existing_lot, handle_cycle_result,
        lot_needs_detail_scrape,
    };
    use auctions::db::LotDetailState;
    use auctions::error::Error;
    use auctions::models::Lot;
    use clap::Parser;
    use std::time::Duration;

    fn sample_lot() -> Lot {
        Lot {
            lot_id: "1".to_owned(),
            auction_id: "A".to_owned(),
            auctioneer: Some("Lloyds".to_owned()),
            lot_number: None,
            title: None,
            current_bid: Some(100.0),
            time_remaining: None,
            seconds_remaining: None,
            image_url: None,
            description: Some("desc".to_owned()),
            location: None,
            lot_images: vec!["https://example.com/a.jpg".to_owned()],
            url: "https://example.com/lot/1".to_owned(),
        }
    }

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

    #[test]
    fn once_mode_propagates_cycle_errors() {
        let err = handle_cycle_result(
            Err(Error::ParseAuctions {
                message: "boom".to_owned(),
            }),
            true,
            true,
            Duration::from_secs(1),
        )
        .expect_err("once mode should return cycle errors");

        assert!(matches!(err, Error::ParseAuctions { .. }));
    }

    #[test]
    fn continuous_mode_swallows_cycle_errors_and_keeps_running() {
        handle_cycle_result(
            Err(Error::ParseAuctions {
                message: "boom".to_owned(),
            }),
            true,
            false,
            Duration::from_secs(1),
        )
        .expect("continuous mode should continue after cycle errors");
    }

    #[test]
    fn lot_detail_scrape_policy_targets_new_or_incomplete_rows() {
        assert!(lot_needs_detail_scrape(None));
        assert!(lot_needs_detail_scrape(Some(&LotDetailState {
            has_description: false,
            has_images: true,
        })));
        assert!(lot_needs_detail_scrape(Some(&LotDetailState {
            has_description: true,
            has_images: false,
        })));
        assert!(!lot_needs_detail_scrape(Some(&LotDetailState {
            has_description: true,
            has_images: true,
        })));
    }

    #[test]
    fn refresh_guard_prevents_overwriting_existing_detail_fields_with_nulls() {
        let existing = LotDetailState {
            has_description: true,
            has_images: true,
        };

        let mut scraped = sample_lot();
        scraped.description = None;
        scraped.lot_images.clear();
        assert!(!can_refresh_existing_lot(&existing, &scraped));

        scraped.description = Some("desc".to_owned());
        scraped.lot_images = vec!["https://example.com/a.jpg".to_owned()];
        assert!(can_refresh_existing_lot(&existing, &scraped));
    }

    #[test]
    fn successful_cycle_always_returns_ok() {
        let stats = CycleStats {
            auctions_written: 1,
            auctions_processed: 1,
            auctions_skipped: 0,
            lots_seen: 10,
            lots_appended: 2,
            lot_price_rows: 10,
        };

        handle_cycle_result(Ok(stats), true, true, Duration::from_secs(1))
            .expect("success should pass in once mode");
        handle_cycle_result(
            Ok(CycleStats {
                auctions_written: 1,
                ..CycleStats::default()
            }),
            true,
            false,
            Duration::from_secs(1),
        )
        .expect("success should pass in continuous mode");
    }
}
