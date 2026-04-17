use std::collections::HashSet;

use scraper::{ElementRef, Html, Selector};
use snafu::ensure;

use crate::error::{Error, ParseAuctionsSnafu, Result};
use crate::models::{Auction, Lot, ScrapedLots};

const BASE_URL: &str = "https://www.lloydsonline.com.au";
const USER_AGENT: &str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) \
    AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36";

pub struct LloydsClient {
    client: reqwest::blocking::Client,
}

struct ScrapedLotDetails {
    description: Option<String>,
    location: Option<String>,
    lot_images: Vec<String>,
}

impl LloydsClient {
    pub fn new() -> Result<Self> {
        let client = reqwest::blocking::Client::builder()
            .user_agent(USER_AGENT)
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| Error::Http {
                url: BASE_URL.to_owned(),
                source: e,
            })?;
        Ok(Self { client })
    }

    fn get_html(&self, path: &str, params: &[(&str, &str)]) -> Result<Html> {
        let url = format!("{BASE_URL}/{path}");
        let text = self
            .client
            .get(&url)
            .query(params)
            .send()
            .and_then(|r| r.error_for_status())
            .and_then(|r| r.text())
            .map_err(|e| Error::Http {
                url: url.clone(),
                source: e,
            })?;
        Ok(Html::parse_document(&text))
    }

    /// Fetch and parse all auctions from /AuctionList.aspx.
    pub fn scrape_auctions(&self) -> Result<Vec<Auction>> {
        let doc = self.get_html("AuctionList.aspx", &[])?;
        parse_auctions(&doc)
    }

    /// Fetch and parse all lots for a given auction ID.
    pub fn scrape_lots(&self, aid: u64, page_size: u32) -> Result<ScrapedLots> {
        let aid_s = aid.to_string();
        let pgs_s = page_size.to_string();
        let doc = self.get_html(
            "AuctionLots.aspx",
            &[("smode", "0"), ("aid", &aid_s), ("pgs", &pgs_s)],
        )?;

        let mut scraped = parse_lots(&doc, &aid_s)?;

        for lot in &mut scraped.lots {
            match self.scrape_lot_details(&aid_s, &lot.lot_id) {
                Ok(details) => {
                    lot.description = details.description;
                    lot.location = details.location;
                    if lot.image_url.is_none() {
                        lot.image_url = details.lot_images.first().cloned();
                    }
                    lot.lot_images = details.lot_images;
                }
                Err(error) => {
                    tracing::warn!(
                        auction_id = %aid_s,
                        lot_id = %lot.lot_id,
                        error = %error,
                        "failed to scrape lot detail; keeping list-page fields"
                    );
                }
            }
        }

        Ok(scraped)
    }

    fn scrape_lot_details(&self, auction_id: &str, lot_id: &str) -> Result<ScrapedLotDetails> {
        let doc = self.get_html(
            "LotDetails.aspx",
            &[("smode", "0"), ("aid", auction_id), ("lid", lot_id)],
        )?;
        parse_lot_details(&doc, auction_id, lot_id)
    }
}

fn parse_selector_for_auctions(css: &str) -> Result<Selector> {
    Selector::parse(css).map_err(|e| Error::ParseAuctions {
        message: format!("invalid selector '{css}': {e}"),
    })
}

fn parse_selector_for_lots(css: &str, auction_id: &str) -> Result<Selector> {
    Selector::parse(css).map_err(|e| Error::ParseLots {
        auction_id: auction_id.to_owned(),
        message: format!("invalid selector '{css}': {e}"),
    })
}

fn inner_text(el: ElementRef<'_>) -> String {
    el.text()
        .collect::<Vec<_>>()
        .join(" ")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn opt_text(el: Option<ElementRef<'_>>) -> Option<String> {
    el.map(inner_text).filter(|s| !s.is_empty())
}

fn absolute_url(url: &str) -> String {
    if url.starts_with("http://") || url.starts_with("https://") {
        url.to_owned()
    } else {
        format!("{BASE_URL}/{}", url.trim_start_matches('/'))
    }
}

/// Extract a query-string parameter value from a URL fragment.
fn query_param<'a>(url: &'a str, key: &str) -> Option<&'a str> {
    let needle = format!("{key}=");
    let start = url.find(needle.as_str())? + needle.len();
    let end = url[start..]
        .find(['&', '#'])
        .map(|i| start + i)
        .unwrap_or(url.len());
    Some(&url[start..end])
}

fn parse_lot_details(doc: &Html, auction_id: &str, lot_id: &str) -> Result<ScrapedLotDetails> {
    let description_sel = parse_selector_for_lots("div.label + div.value", auction_id)?;
    let strong_sel = parse_selector_for_lots("strong", auction_id)?;
    let carousel_img_sel = parse_selector_for_lots(".carousel-inner img", auction_id)?;

    let description = doc
        .select(&description_sel)
        .next()
        .map(|el| clean_html_block(&el.inner_html()))
        .filter(|text| !text.is_empty());

    let location = extract_location(doc, &strong_sel)
        .or_else(|| description.as_deref().and_then(location_from_description));

    let lot_images = extract_lot_images(doc, &carousel_img_sel);

    if description.is_none() && location.is_none() && lot_images.is_empty() {
        return Err(Error::ParseLots {
            auction_id: auction_id.to_owned(),
            message: format!(
                "lot detail page for lot {lot_id} did not contain description, location, or images"
            ),
        });
    }

    Ok(ScrapedLotDetails {
        description,
        location,
        lot_images,
    })
}

fn extract_location(doc: &Html, strong_sel: &Selector) -> Option<String> {
    for strong in doc.select(strong_sel) {
        if inner_text(strong).trim() != "Location of item:" {
            continue;
        }

        let Some(parent) = strong.parent().and_then(ElementRef::wrap) else {
            continue;
        };
        let full_text = inner_text(parent);
        let location = full_text
            .trim()
            .strip_prefix("Location of item:")?
            .trim()
            .to_owned();

        if !location.is_empty() {
            return Some(location);
        }
    }

    None
}

fn location_from_description(description: &str) -> Option<String> {
    let marker = "Location of item:";
    let start = description.find(marker)? + marker.len();
    let tail = &description[start..];
    let location = tail
        .split(" Thinking of financing?")
        .next()
        .unwrap_or(tail)
        .trim()
        .trim_end_matches('.')
        .to_owned();

    if location.is_empty() {
        None
    } else {
        Some(location)
    }
}

fn extract_lot_images(doc: &Html, carousel_img_sel: &Selector) -> Vec<String> {
    let mut images = Vec::new();
    let mut seen = HashSet::new();

    for img in doc.select(carousel_img_sel) {
        for attr in ["data-src", "src"] {
            let Some(raw) = img.value().attr(attr) else {
                continue;
            };

            let url = absolute_url(raw);
            if url.contains("preloader.gif") || url.is_empty() {
                continue;
            }

            if seen.insert(url.clone()) {
                images.push(url);
            }
        }
    }

    images
}

fn clean_html_block(html: &str) -> String {
    html.replace("<br>", "\n")
        .replace("<br/>", "\n")
        .replace("<br />", "\n")
        .replace("\r", "")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_owned()
}

fn parse_bid_amount(raw: Option<&str>) -> Option<f64> {
    let raw = raw?.trim();
    if raw.is_empty() {
        return None;
    }

    let mut token = String::new();
    let mut started = false;

    for ch in raw.chars() {
        if ch.is_ascii_digit() || ch == ',' || ch == '.' {
            token.push(ch);
            started = true;
            continue;
        }

        if started {
            break;
        }
    }

    if token.is_empty() {
        return None;
    }

    token.replace(',', "").parse::<f64>().ok()
}

fn parse_auctions(doc: &Html) -> Result<Vec<Auction>> {
    let auction_item_sel = parse_selector_for_auctions(".auction_list_item")?;
    let details_sel = parse_selector_for_auctions("a[href*='AuctionDetails.aspx']")?;
    let image_sel = parse_selector_for_auctions("img.auct_img")?;
    let live_sel = parse_selector_for_auctions(".auctionList_onLive")?;
    let date_sel = parse_selector_for_auctions(".auct_date")?;
    let title_sel = parse_selector_for_auctions(".auct_title h1")?;
    let loc_sel = parse_selector_for_auctions(".auct_location img")?;
    let auct_type_sel = parse_selector_for_auctions("img.auct_type_image")?;

    let mut auctions = Vec::new();

    for item in doc.select(&auction_item_sel) {
        let Some(a) = item.select(&details_sel).next() else {
            continue;
        };

        let href = a.value().attr("href").unwrap_or("");
        let Some(aid) = query_param(href, "aid") else {
            continue;
        };

        let image_url = item
            .select(&image_sel)
            .next()
            .and_then(|img| img.value().attr("src"))
            .map(absolute_url);

        let is_live = item.select(&live_sel).next().is_some();
        let date = opt_text(item.select(&date_sel).next());
        let title = opt_text(item.select(&title_sel).next());

        let mut state = None;
        let mut auctioneer = None;
        for img in item.select(&loc_sel) {
            let src = img.value().attr("src").unwrap_or("");
            let title_attr = img.value().attr("title").map(str::to_owned);
            if src.contains("s_") {
                state = title_attr;
            } else if src.contains("a_") {
                auctioneer = title_attr;
            }
        }

        let auction_type = item
            .select(&auct_type_sel)
            .next()
            .and_then(|img| img.value().attr("title"))
            .map(str::to_owned);

        auctions.push(Auction {
            auction_id: aid.to_owned(),
            title,
            date,
            state,
            auctioneer,
            auction_type,
            is_live,
            image_url,
            details_url: absolute_url(href),
            lots_url: format!("{BASE_URL}/AuctionLots.aspx?smode=0&aid={aid}"),
        });
    }

    ensure!(
        !auctions.is_empty(),
        ParseAuctionsSnafu {
            message: "no auction items found — the page structure may have changed"
        }
    );

    Ok(auctions)
}

fn parse_lots(doc: &Html, auction_id: &str) -> Result<ScrapedLots> {
    let lot_link_sel = parse_selector_for_lots("a[href*='LotDetails.aspx']", auction_id)?;
    let lot_num_sel = parse_selector_for_lots("[class*='lot_num']", auction_id)?;
    let lot_img_sel = parse_selector_for_lots("[class*='lot_img']", auction_id)?;
    let lot_title_sel = parse_selector_for_lots("[class*='lot_desc'] h1", auction_id)?;
    let bid_sel = parse_selector_for_lots("[class*='current_bid_amount_']", auction_id)?;
    let time_sel = parse_selector_for_lots("[class*='time_rem_val_']", auction_id)?;
    let page_info_sel = parse_selector_for_lots("[class*='next_prev_page']", auction_id)?;
    let title_sel = parse_selector_for_lots("title", auction_id)?;

    let mut lots = Vec::new();

    for a in doc.select(&lot_link_sel) {
        let href = a.value().attr("href").unwrap_or("");
        let Some(lid) = query_param(href, "lid") else {
            continue;
        };

        let lot_number = opt_text(a.select(&lot_num_sel).next());

        let image_url = a
            .select(&lot_img_sel)
            .next()
            .and_then(|img| img.value().attr("src"))
            .map(absolute_url);

        let title = opt_text(a.select(&lot_title_sel).next());

        let current_bid = parse_bid_amount(opt_text(a.select(&bid_sel).next()).as_deref());

        let time_span = a.select(&time_sel).next();
        let time_remaining = opt_text(time_span);
        let seconds_remaining = time_span
            .and_then(|el| el.value().attr("data-seconds_rem"))
            .and_then(|s| s.parse::<i64>().ok());

        lots.push(Lot {
            lot_id: lid.to_owned(),
            auction_id: auction_id.to_owned(),
            auctioneer: None,
            lot_number,
            title,
            current_bid,
            time_remaining,
            seconds_remaining,
            image_url,
            description: None,
            location: None,
            lot_images: Vec::new(),
            url: absolute_url(href),
        });
    }

    let page_info = opt_text(doc.select(&page_info_sel).next());
    let page_title = opt_text(doc.select(&title_sel).next());

    if lots.is_empty() {
        return Err(Error::ParseLots {
            auction_id: auction_id.to_owned(),
            message: "no lot items found — verify auction ID and page structure".to_owned(),
        });
    }

    Ok(ScrapedLots {
        page_title,
        page_info,
        lots,
    })
}

#[cfg(test)]
mod tests {
    use super::{parse_auctions, parse_bid_amount, parse_lot_details, parse_lots, query_param};
    use scraper::Html;

    #[test]
    fn query_param_extracts_expected_values() {
        let href = "AuctionDetails.aspx?smode=0&aid=67956#x";
        assert_eq!(query_param(href, "aid"), Some("67956"));
        assert_eq!(query_param(href, "missing"), None);
    }

    #[test]
    fn parse_bid_amount_strips_currency_and_commas() {
        assert_eq!(parse_bid_amount(Some("$1,250")), Some(1250.0));
        assert_eq!(parse_bid_amount(Some("AUD 99.95 incl GST")), Some(99.95));
        assert_eq!(parse_bid_amount(Some("No bids")), None);
    }

    #[test]
    fn parse_auctions_extracts_core_fields() {
        let html = r#"
        <div class="auction_list_item">
            <a href="AuctionDetails.aspx?aid=67956">
                <img class="auct_img" src="/images/a.png" />
                <div class="auctionList_onLive"></div>
                <div class="auct_date">Mon 01-Jan-2026 10:00</div>
                <div class="auct_title"><h1>Test Auction</h1></div>
                <div class="auct_location">
                    <img src="s_qld.png" title="Queensland" />
                    <img src="a_lloyds.png" title="Lloyds Auctioneers and Valuers" />
                </div>
                <img class="auct_type_image" title="Internet &amp; Absentee Bidding Only" />
            </a>
        </div>
        "#;

        let doc = Html::parse_document(html);
        let parsed = parse_auctions(&doc).expect("auction parsing should succeed");

        assert_eq!(parsed.len(), 1);
        let a = &parsed[0];
        assert_eq!(a.auction_id, "67956");
        assert_eq!(a.title.as_deref(), Some("Test Auction"));
        assert_eq!(a.state.as_deref(), Some("Queensland"));
        assert!(a.is_live);
        assert_eq!(
            a.image_url.as_deref(),
            Some("https://www.lloydsonline.com.au/images/a.png")
        );
    }

    #[test]
    fn parse_lots_extracts_metadata_and_rows() {
        let html = r#"
        <html>
          <head><title>Auction Lots Test</title></head>
          <body>
            <div class="next_prev_page">Page 1 of 3</div>
            <a href="LotDetails.aspx?lid=1234">
                <div class="lot_num_1">12</div>
                <img class="lot_img_1" src="/images/l1.png" />
                <div class="lot_desc_1"><h1>Vintage guitar</h1></div>
                <span class="current_bid_amount_1">$1,250</span>
                <span class="time_rem_val_1" data-seconds_rem="3600">1h</span>
            </a>
          </body>
        </html>
        "#;

        let doc = Html::parse_document(html);
        let parsed = parse_lots(&doc, "67956").expect("lot parsing should succeed");

        assert_eq!(parsed.page_title.as_deref(), Some("Auction Lots Test"));
        assert_eq!(parsed.page_info.as_deref(), Some("Page 1 of 3"));
        assert_eq!(parsed.lots.len(), 1);

        let l = &parsed.lots[0];
        assert_eq!(l.lot_id, "1234");
        assert_eq!(l.auction_id, "67956");
        assert_eq!(l.lot_number.as_deref(), Some("12"));
        assert_eq!(l.current_bid, Some(1250.0));
        assert_eq!(l.seconds_remaining, Some(3600));
        assert_eq!(l.description, None);
        assert_eq!(l.location, None);
        assert!(l.lot_images.is_empty());
    }

    #[test]
    fn parse_lot_details_extracts_description_location_and_images() {
        let html = r#"
        <html>
          <body>
            <div class="label">Description:</div>
            <div class="value">
              <p><b>Great caravan</b><br>Near new.</p>
              <p><strong>Location of item:</strong> Melbourne, VIC, Australia</p>
            </div>
            <div class="carousel-inner">
              <img data-src="https://example.com/image-1.jpg" />
              <img data-src="https://example.com/image-2.jpg" />
              <img src="https://example.com/image-2.jpg" />
            </div>
          </body>
        </html>
        "#;

        let doc = Html::parse_document(html);
        let details =
            parse_lot_details(&doc, "67956", "1234").expect("detail parsing should succeed");

        assert!(
            details
                .description
                .as_deref()
                .is_some_and(|d| d.contains("Great caravan"))
        );
        assert_eq!(
            details.location.as_deref(),
            Some("Melbourne, VIC, Australia")
        );
        assert_eq!(details.lot_images.len(), 2);
    }
}
