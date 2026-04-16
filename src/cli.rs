use clap::{Args, Parser, Subcommand};
use clap_complete::Shell;

#[derive(Debug, Parser)]
#[command(
    name = "auctions",
    about = "Scrape lloydsonline.com.au auction data",
    version,
    after_help = "\
EXAMPLES:
  auctions list
  auctions lots --aid 67956

  auctions list --adbc-uri grpc://localhost:50051
  auctions lots --aid 67956 --adbc-driver adbc_driver_flightsql

  auctions completions zsh > ~/.zsh/completions/_auctions"
)]
pub struct Cli {
    /// Increase log verbosity (-v = INFO, -vv = DEBUG)
    #[arg(short, long, action = clap::ArgAction::Count, global = true)]
    pub verbose: u8,

    /// Suppress progress messages on stderr
    #[arg(short, long, global = true)]
    pub quiet: bool,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// List all current auctions on lloydsonline.com.au
    #[command(visible_alias = "auctions")]
    List(AuctionArgs),

    /// List lots for a specific auction
    Lots(LotArgs),

    /// Generate shell completions
    Completions(CompletionArgs),
}

#[derive(Debug, Args)]
pub struct CompletionArgs {
    /// Shell to generate completions for
    pub shell: Shell,
}

#[derive(Debug, Args)]
pub struct AuctionArgs {
    #[command(flatten)]
    pub db: DbArgs,
}

#[derive(Debug, Args)]
pub struct LotArgs {
    /// Auction ID to fetch lots for
    #[arg(long, value_name = "ID")]
    pub aid: u64,

    /// Maximum lots to fetch per page
    #[arg(long, default_value_t = 100, value_name = "N", value_parser = clap::value_parser!(u32).range(1..=500))]
    pub page_size: u32,

    #[command(flatten)]
    pub db: DbArgs,
}

#[derive(Debug, Args)]
pub struct DbArgs {
    /// ADBC connection URI (enables database write mode)
    #[arg(long, value_name = "URI")]
    pub adbc_uri: Option<String>,

    /// ADBC driver name or path (enables database write mode)
    #[arg(long, value_name = "DRIVER")]
    pub adbc_driver: Option<String>,

    /// ADBC init options as a JSON object
    #[arg(long, value_name = "JSON")]
    pub adbc_options: Option<String>,

    /// Catalog name for writes
    #[arg(long, value_name = "CATALOG")]
    pub catalog: Option<String>,

    /// Schema name for writes
    #[arg(long, value_name = "SCHEMA", default_value = "public")]
    pub schema: String,
}

impl DbArgs {
    pub fn writes_to_db(&self) -> bool {
        self.adbc_uri.is_some() || self.adbc_driver.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::DbArgs;

    #[test]
    fn writes_to_db_is_enabled_only_when_uri_or_driver_is_set() {
        let base = DbArgs {
            adbc_uri: None,
            adbc_driver: None,
            adbc_options: None,
            catalog: None,
            schema: "public".to_owned(),
        };

        assert!(!base.writes_to_db());

        let with_uri = DbArgs {
            adbc_uri: Some("grpc://localhost:50051".to_owned()),
            ..base
        };
        assert!(with_uri.writes_to_db());
    }
}
