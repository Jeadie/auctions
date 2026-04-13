"""
lloydsonline.py — scrape auctions and lots from lloydsonline.com.au

Usage:
    python lloydsonline.py auctions                        # list all auctions
    python lloydsonline.py auctions --json                 # as JSON
    python lloydsonline.py auctions --out auctions.json    # save to file

    python lloydsonline.py lots --aid 67956                # lots for one auction
    python lloydsonline.py lots --aid 67956 --json         # as JSON
    python lloydsonline.py lots --aid 67956 --out lots.json
"""

import argparse
import json
import re
import sys
from datetime import datetime

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.lloydsonline.com.au"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}


def _get(path: str, **params) -> BeautifulSoup:
    url = f"{BASE_URL}/{path}"
    resp = requests.get(url, headers=HEADERS, params=params, timeout=30)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "lxml")


# ---------------------------------------------------------------------------
# Auction list
# ---------------------------------------------------------------------------

def scrape_auction_list() -> list[dict]:
    """Return all auctions from /AuctionList.aspx."""
    soup = _get("AuctionList.aspx")

    auctions = []
    for item in soup.find_all(class_="auction_list_item"):
        a = item.find("a", href=re.compile(r"AuctionDetails\.aspx"))
        if not a:
            continue

        href = a["href"]
        aid_m = re.search(r"aid=(\d+)", href)
        aid = aid_m.group(1) if aid_m else None

        img = a.find("img", class_="auct_img")
        image_url = img["src"] if img else None

        is_live = a.find(class_="auctionList_onLive") is not None

        date_div = a.find(class_="auct_date")
        date_str = date_div.get_text(strip=True) if date_div else None

        title_div = a.find(class_="auct_title")
        title = (
            title_div.find("h1").get_text(strip=True)
            if title_div and title_div.find("h1")
            else None
        )

        state = None
        auctioneer = None
        loc = a.find(class_="auct_location")
        if loc:
            for img_tag in loc.find_all("img"):
                src = img_tag.get("src", "")
                title_attr = img_tag.get("title", "")
                if "s_" in src:
                    state = title_attr
                elif "a_" in src:
                    auctioneer = title_attr

        type_img = a.find("img", class_="auct_type_image")
        auction_type = type_img.get("title") if type_img else None

        auctions.append({
            "auction_id": aid,
            "title": title,
            "date": date_str,
            "state": state,
            "auctioneer": auctioneer,
            "auction_type": auction_type,
            "is_live": is_live,
            "image_url": image_url,
            "details_url": f"{BASE_URL}/{href}",
            "lots_url": f"{BASE_URL}/AuctionLots.aspx?smode=0&aid={aid}",
        })

    return auctions


def print_auction_table(auctions: list[dict]) -> None:
    print(f"\n{'='*80}")
    print(f"  Lloyds Online — {len(auctions)} auctions  (scraped {datetime.now().strftime('%Y-%m-%d %H:%M')})")
    print(f"{'='*80}")
    print(f"  {'ID':<8} {'Date':<22} {'State':<22} {'Live':<6} Title")
    print(f"  {'-'*8} {'-'*22} {'-'*22} {'-'*6} {'-'*35}")
    for a in auctions:
        print(
            f"  {a['auction_id']:<8} "
            f"{(a['date'] or '-'):<22} "
            f"{(a['state'] or '-'):<22} "
            f"{'YES' if a['is_live'] else '':<6} "
            f"{(a['title'] or '-')[:55]}"
        )
    print()


# ---------------------------------------------------------------------------
# Auction lots
# ---------------------------------------------------------------------------

def scrape_auction_lots(aid: int, page_size: int = 100) -> dict:
    """Fetch and parse all lots for a given auction ID."""
    soup = _get("AuctionLots.aspx", smode=0, aid=aid, pgs=page_size)

    pager = soup.find(class_=re.compile(r"next_prev_page"))
    page_info = pager.get_text(strip=True) if pager else "unknown"

    title_tag = soup.find("title")
    page_title = title_tag.get_text(strip=True) if title_tag else ""

    lots = []
    for a in soup.find_all("a", href=re.compile(r"LotDetails\.aspx")):
        href = a.get("href", "")

        lot_id_m = re.search(r"lid=(\d+)", href)
        lot_id = lot_id_m.group(1) if lot_id_m else None

        lot_num_div = a.find("div", class_=lambda c: c and "lot_num" in c)
        lot_number = lot_num_div.get_text(strip=True) if lot_num_div else None

        img = a.find("img", class_=lambda c: c and "lot_img" in c)
        image_url = img.get("src") if img else None

        desc_div = a.find("div", class_=lambda c: c and "lot_desc" in c)
        h1 = desc_div.find("h1") if desc_div else None
        title = h1.get_text(strip=True) if h1 else None

        bid_span = a.find("span", class_=re.compile(r"current_bid_amount_"))
        current_bid = bid_span.get_text(strip=True) if bid_span else None

        time_span = a.find("span", class_=re.compile(r"time_rem_val_"))
        time_remaining = time_span.get_text(strip=True) if time_span else None
        seconds_remaining = (
            int(time_span["data-seconds_rem"])
            if time_span and time_span.get("data-seconds_rem")
            else None
        )

        lots.append({
            "lot_id": lot_id,
            "lot_number": lot_number,
            "title": title,
            "current_bid": current_bid,
            "time_remaining": time_remaining,
            "seconds_remaining": seconds_remaining,
            "image_url": image_url,
            "url": f"{BASE_URL}/{href}",
        })

    return {
        "auction_id": aid,
        "page_title": page_title,
        "page_info": page_info,
        "total_lots": len(lots),
        "lots": lots,
    }


def print_lots_table(data: dict) -> None:
    print(f"\n{'='*70}")
    print(f"  {data['page_title']}")
    print(f"  Auction ID: {data['auction_id']}  |  Lots: {data['total_lots']}  |  Page: {data['page_info']}")
    print(f"{'='*70}")
    print(f"  {'ID':<10} {'#':<6} {'Bid':<14} {'Time Rem':<12} Title")
    print(f"  {'-'*10} {'-'*6} {'-'*14} {'-'*12} {'-'*30}")
    for lot in data["lots"]:
        print(
            f"  {(lot['lot_id'] or '-'):<10} "
            f"{(lot['lot_number'] or '-'):<6} "
            f"{(lot['current_bid'] or '-'):<14} "
            f"{(lot['time_remaining'] or '-'):<12} "
            f"{(lot['title'] or '-')[:50]}"
        )
    print()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Scrape lloydsonline.com.au",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    # auctions subcommand
    p_auctions = sub.add_parser("auctions", help="List all auctions")
    p_auctions.add_argument("--json", action="store_true", help="Print as JSON")
    p_auctions.add_argument("--out", help="Save JSON to file")

    # lots subcommand
    p_lots = sub.add_parser("lots", help="List lots for an auction")
    p_lots.add_argument("--aid", type=int, required=True, help="Auction ID")
    p_lots.add_argument("--page-size", type=int, default=100, help="Items per page (default: 100)")
    p_lots.add_argument("--json", action="store_true", help="Print as JSON")
    p_lots.add_argument("--out", help="Save JSON to file")

    args = parser.parse_args()

    if args.cmd == "auctions":
        print("Fetching auction list...", file=sys.stderr)
        auctions = scrape_auction_list()
        data = {"total": len(auctions), "auctions": auctions}

        if args.out:
            with open(args.out, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            print(f"Saved {len(auctions)} auctions to {args.out}", file=sys.stderr)

        if args.json or args.out:
            if not args.out:
                print(json.dumps(data, indent=2, ensure_ascii=False))
        else:
            print_auction_table(auctions)

    elif args.cmd == "lots":
        print(f"Fetching lots for auction {args.aid}...", file=sys.stderr)
        data = scrape_auction_lots(args.aid, page_size=args.page_size)

        if args.out:
            with open(args.out, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            print(f"Saved {data['total_lots']} lots to {args.out}", file=sys.stderr)

        if args.json or args.out:
            if not args.out:
                print(json.dumps(data, indent=2, ensure_ascii=False))
        else:
            print_lots_table(data)


if __name__ == "__main__":
    main()
