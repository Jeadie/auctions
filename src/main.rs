use auctions::cli::{Cli, Command};
use auctions::error::{Error, Result};
use auctions::models::{AuctionList, LotList};
use auctions::{db, scraper};
use clap::{CommandFactory, Parser};
use clap_complete::generate;
use tracing::level_filters::LevelFilter;
use tracing_subscriber::EnvFilter;

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
                let cfg = db_config_from_args(&args.db)?;
                let target = cfg.to_string();
                status(cli.quiet, format!("Connecting to {target} …"));
                let mut db = db::Db::connect(cfg)?;
                db.setup()?;
                let rows = db.write_auctions(&list.auctions)?;
                status(cli.quiet, format!("Wrote {rows} auctions → {target}"));
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

                let cfg = db_config_from_args(&args.db)?;
                let target = cfg.to_string();
                status(cli.quiet, format!("Connecting to {target} …"));
                let mut db = db::Db::connect(cfg)?;
                db.setup()?;
                let rows = db.write_lots(&list.lots)?;
                status(cli.quiet, format!("Wrote {rows} lots → {target}"));
            } else {
                write_json_output(&list)?;
            }
        }
    }

    Ok(())
}

fn db_config_from_args(args: &auctions::cli::DbArgs) -> Result<db::DbConfig> {
    db::DbConfig::from_parts(
        args.adbc_driver.as_deref(),
        args.adbc_uri.as_deref(),
        args.adbc_options.as_deref(),
        args.catalog.as_deref(),
        Some(&args.schema),
    )
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
    let json = serde_json::to_string_pretty(data).map_err(|source| Error::Json { source })?;
    println!("{json}");
    Ok(())
}
