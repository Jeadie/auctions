use snafu::Snafu;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("HTTP request to {url} failed: {source}"))]
    Http { url: String, source: reqwest::Error },

    #[snafu(display("Failed to parse auction list: {message}"))]
    ParseAuctions { message: String },

    #[snafu(display("Failed to parse lots for auction {auction_id}: {message}"))]
    ParseLots { auction_id: String, message: String },

    #[snafu(display("Invalid static CSS selector '{selector}': {message}"))]
    ParseSelector { selector: String, message: String },

    #[snafu(display("Failed to load ADBC driver '{driver}': {message}"))]
    DbDriver { driver: String, message: String },

    #[snafu(display("Failed to connect to {uri}: {message}"))]
    DbConnect { uri: String, message: String },

    #[snafu(display("Database setup failed ({query}): {message}"))]
    DbSetup { query: String, message: String },

    #[snafu(display("Failed to write {rows} rows to {table}: {message}"))]
    DbWrite {
        rows: usize,
        table: String,
        message: String,
    },

    #[snafu(display(
        "Lot {auction_id}/{lot_id} is missing auctioneer; required for writes to {table}"
    ))]
    MissingAuctioneer {
        table: String,
        auction_id: String,
        lot_id: String,
    },

    #[snafu(display("Invalid ADBC options JSON: {source}"))]
    AdbcOptionsJson { source: serde_json::Error },

    #[snafu(display("JSON serialization failed: {source}"))]
    Json { source: serde_json::Error },
}
