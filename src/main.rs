mod cli;
mod db;
mod error;
mod models;
mod scraper;

use clap::{CommandFactory, Parser};
use clap_complete::generate;
use snafu::ResultExt;
use tracing::level_filters::LevelFilter;

use crate::cli::{Cli, Command};
use crate::error::{Error, JsonSnafu, Result};
use crate::models::{AuctionList, LotList};

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
    match cli.command {
        Command::Completions(args) => {
            let mut cmd = Cli::command();
            generate(args.shell, &mut cmd, "auctions", &mut std::io::stdout());
        }

        Command::List(args) => {
            let client = scraper::LloydsClient::new()?;
            status(cli.quiet, "Fetching auction list…");

            let auctions = client.scrape_auctions()?;
            let list = AuctionList {
                total: auctions.len(),
                auctions,
            };

            if args.db.writes_to_db() {
                let cfg = db::DbConfig::from_args(&args.db)?;
                status(cli.quiet, format!("Connecting to {cfg} …"));
                let mut db = db::Db::connect(cfg)?;
                db.setup()?;
                let rows = db.write_auctions(&list.auctions)?;
                status(
                    cli.quiet,
                    format!(
                        "Wrote {rows} auctions → {}",
                        db::DbConfig::from_args(&args.db)?
                    ),
                );
            } else {
                write_json_output(&list)?;
            }
        }

        Command::Lots(args) => {
            let client = scraper::LloydsClient::new()?;
            status(
                cli.quiet,
                format!("Fetching lots for auction {} …", args.aid),
            );

            let scraped = client.scrape_lots(args.aid, args.page_size)?;
            let mut list = LotList {
                auction_id: args.aid.to_string(),
                page_title: scraped.page_title,
                page_info: scraped.page_info,
                total_lots: scraped.lots.len(),
                lots: scraped.lots,
            };

            if args.db.writes_to_db() {
                let auctioneer = resolve_auctioneer(&client, args.aid)?;
                for lot in &mut list.lots {
                    lot.auctioneer = Some(auctioneer.clone());
                }

                let cfg = db::DbConfig::from_args(&args.db)?;
                status(cli.quiet, format!("Connecting to {cfg} …"));
                let mut db = db::Db::connect(cfg)?;
                db.setup()?;
                let rows = db.write_lots(&list.lots)?;
                status(
                    cli.quiet,
                    format!("Wrote {rows} lots → {}", db::DbConfig::from_args(&args.db)?),
                );
            } else {
                write_json_output(&list)?;
            }
        }
    }

    Ok(())
}

fn resolve_auctioneer(client: &scraper::LloydsClient, auction_id: u64) -> Result<String> {
    let auction_id_s = auction_id.to_string();
    let auctions = client.scrape_auctions()?;

    let maybe_auction = auctions.into_iter().find(|a| a.auction_id == auction_id_s);
    let auction = maybe_auction.ok_or_else(|| Error::ParseLots {
        auction_id: auction_id_s.clone(),
        message: "auction ID not found in auction list while resolving auctioneer".to_owned(),
    })?;

    let auctioneer = auction
        .auctioneer
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| Error::ParseLots {
            auction_id: auction_id_s,
            message: "auctioneer is missing for this auction; required for lot primary key"
                .to_owned(),
        })?;

    Ok(auctioneer)
}

fn write_json_output<T: serde::Serialize>(data: &T) -> Result<()> {
    let json = serde_json::to_string_pretty(data).context(JsonSnafu)?;
    println!("{json}");
    Ok(())
}
