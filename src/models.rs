use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Auction {
    pub auction_id: String,
    pub title: Option<String>,
    pub date: Option<String>,
    pub state: Option<String>,
    pub auctioneer: Option<String>,
    pub auction_type: Option<String>,
    pub is_live: bool,
    pub image_url: Option<String>,
    pub details_url: String,
    pub lots_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Lot {
    pub lot_id: String,
    pub auction_id: String,
    pub auctioneer: Option<String>,
    pub lot_number: Option<String>,
    pub title: Option<String>,
    pub current_bid: Option<f64>,
    pub time_remaining: Option<String>,
    pub seconds_remaining: Option<i64>,
    pub image_url: Option<String>,
    pub description: Option<String>,
    pub location: Option<String>,
    #[serde(default)]
    pub lot_images: Vec<String>,
    pub url: String,
}

/// Structured response for the `list` command.
#[derive(Debug, Serialize)]
pub struct AuctionList {
    pub total: usize,
    pub auctions: Vec<Auction>,
}

/// Structured response for the `lots` command.
#[derive(Debug, Serialize)]
pub struct LotList {
    pub auction_id: String,
    pub page_title: Option<String>,
    pub page_info: Option<String>,
    pub total_lots: usize,
    pub lots: Vec<Lot>,
}

#[derive(Debug, Clone)]
pub struct ScrapedLots {
    pub page_title: Option<String>,
    pub page_info: Option<String>,
    pub lots: Vec<Lot>,
}
