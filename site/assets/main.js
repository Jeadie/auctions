import { SpiceClient } from "https://esm.sh/@spiceai/spice@latest";

// ── Config ────────────────────────────────────────────────

const CONFIG = {
  httpUrl: "https://data.spiceai.io",
  flightUrl: "flight.spiceai.io:443",
  apiKey: "",
  ...(window.AUCTIONS_SITE_CONFIG || {}),
};

// ── State ─────────────────────────────────────────────────

const state = {
  client: null,
  auctions: [],
  auctionDetailCache: {},
  currentAuctionLots: [],
};

// ── Boot ──────────────────────────────────────────────────

state.client = new SpiceClient({
  apiKey: CONFIG.apiKey || undefined,
  httpUrl: CONFIG.httpUrl,
  flightUrl: CONFIG.flightUrl,
  logging: false,
});

wireNav();
wireMainControls();
wireAuctionDetailControls();
handleRoute();
window.addEventListener("hashchange", handleRoute);

loadAuctions();
loadLots("", "bid-desc");

// ── Routing ───────────────────────────────────────────────

function handleRoute() {
  const hash = location.hash;

  const lotMatch = hash.match(/^#lot\/(\d+)\/(\d+)$/);
  const auctionMatch = hash.match(/^#auction\/(\d+)$/);

  if (lotMatch) {
    setView("lot");
    loadLotDetail(lotMatch[1], lotMatch[2]);
  } else if (auctionMatch) {
    setView("auction");
    loadAuctionDetail(auctionMatch[1]);
  } else {
    setView("main");
  }
}

function setView(name) {
  document.getElementById("view-main").hidden = name !== "main";
  document.getElementById("view-auction").hidden = name !== "auction";
  document.getElementById("view-lot").hidden = name !== "lot";
  window.scrollTo(0, 0);
}

// ── Nav ───────────────────────────────────────────────────

function wireNav() {
  document.getElementById("brand-link").addEventListener("click", () => { location.hash = ""; });
  document.getElementById("nav-lots").addEventListener("click", () => {
    location.hash = "";
    setTimeout(() => document.getElementById("lots-section").scrollIntoView({ behavior: "smooth" }), 40);
  });
  document.getElementById("nav-auctions").addEventListener("click", () => {
    location.hash = "";
    setTimeout(() => document.getElementById("auctions-section").scrollIntoView({ behavior: "smooth" }), 40);
  });
  document.getElementById("auction-back-link").addEventListener("click", () => { location.hash = ""; });
  document.getElementById("lot-back-link").addEventListener("click", () => { history.back(); });
}

// ── Main view controls ────────────────────────────────────

let searchDebounce = null;

function wireMainControls() {
  const search = document.getElementById("lot-search");
  const sort = document.getElementById("lot-sort");

  search.addEventListener("input", () => {
    clearTimeout(searchDebounce);
    searchDebounce = setTimeout(() => {
      loadLots(search.value.trim(), sort.value);
    }, 320);
  });

  sort.addEventListener("change", () => {
    clearTimeout(searchDebounce);
    loadLots(search.value.trim(), sort.value);
  });
}

// ── Auction detail controls ───────────────────────────────

function wireAuctionDetailControls() {
  document.getElementById("auction-search").addEventListener("input", (e) => {
    renderAuctionLots(e.target.value.trim().toLowerCase());
  });
}

// ── Data: main lots (dynamic per search/sort) ─────────────

async function loadLots(search, sort) {
  const grid = document.getElementById("lots-grid");
  const count = document.getElementById("lots-count");

  grid.innerHTML = `<p class="loading">Loading…</p>`;
  count.textContent = "";

  try {
    const rows = search.trim()
      ? await searchLots(search.trim(), sort)
      : await queryRows(buildLotsSQL("", sort));

    count.textContent = `${rows.length} lots`;
    grid.innerHTML = rows.length
      ? rows.map((r) => lotCardHTML(r, true)).join("")
      : `<p class="empty">No lots match.</p>`;
  } catch (err) {
    console.error(err);
    grid.innerHTML = `<p class="empty">Could not load lots.</p>`;
  }
}

async function searchLots(text, sort) {
  const url = `${CONFIG.httpUrl}/v1/search`;

  const body = {
    text,
    datasets: ["live_lot_prices"],
    limit: 50,
    additional_columns: [
      "auction_id", "auction_title", "lot_id", "lot_title",
      "image_url", "location", "lot_url", "latest_bid",
    ],
  };

  const resp = await fetch(url, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      ...(CONFIG.apiKey ? { "X-API-Key": CONFIG.apiKey } : {}),
    },
    body: JSON.stringify(body),
  });

  if (!resp.ok) throw new Error(`Search failed: ${resp.status}`);

  const json = await resp.json();

  // Map search results back to the same shape as SQL lot rows
  const rows = (json.results || []).map((r) => {
    const d = { ...r.data, ...r.primary_key };
    return {
      auction_id:    d.auction_id,
      auction_title: d.auction_title,
      lot_id:        d.lot_id,
      lot_title:     d.lot_title,
      image_url:     d.image_url,
      location:      d.location,
      lot_url:       d.lot_url,
      latest_bid:    d.latest_bid,
    };
  });

  // Search returns by relevance; re-sort if user picked a non-default order
  if (sort === "bid-asc")   rows.sort((a, b) => bidVal(a) - bidVal(b));
  if (sort === "title-asc") rows.sort((a, b) => String(a.lot_title || "").localeCompare(String(b.lot_title || "")));
  // bid-desc: leave as-is (search already surfaces most relevant)

  return rows;
}

function buildLotsSQL(search, sort) {
  const order =
    sort === "bid-asc"   ? "ORDER BY COALESCE(latest_bid, 0) ASC" :
    sort === "title-asc" ? "ORDER BY lot_title ASC" :
                           "ORDER BY COALESCE(latest_bid, 0) DESC";

  return `SELECT auction_id, auction_title, lot_id, lot_title, image_url, location, lot_url, latest_bid
FROM live_lot_prices
${order}
LIMIT 50`;
}

// ── Data: auctions list (loaded once) ────────────────────

async function loadAuctions() {
  try {
    const rows = await queryRows(`SELECT auction_id, MAX(auction_title) AS auction_title, COUNT(*) AS lot_count
FROM live_lot_prices
GROUP BY auction_id
ORDER BY lot_count DESC`);

    state.auctions = rows;
    renderAuctions(rows);
  } catch (err) {
    console.error(err);
    document.getElementById("auctions-body").innerHTML =
      `<tr><td colspan="2" class="empty">Could not load auctions.</td></tr>`;
  }
}

// ── Data: auction detail (cached) ────────────────────────

async function loadAuctionDetail(auctionId) {
  const meta = state.auctions.find((a) => String(a.auction_id) === String(auctionId));
  document.getElementById("auction-detail-title").textContent =
    meta?.auction_title || `Auction ${auctionId}`;
  document.getElementById("auction-detail-state").textContent = "";
  document.getElementById("auction-search").value = "";
  document.getElementById("auction-lots-count").textContent = "";

  if (state.auctionDetailCache[auctionId]) {
    state.currentAuctionLots = state.auctionDetailCache[auctionId];
    renderAuctionLots("");
    return;
  }

  document.getElementById("auction-lots-grid").innerHTML = `<p class="loading">Loading lots…</p>`;

  try {
    const safe = String(auctionId).replace(/\D/g, "");
    const rows = await queryRows(`SELECT auction_id, auction_title, lot_id, lot_title, image_url, location, lot_url, latest_bid, description
FROM live_lot_prices
WHERE auction_id = '${safe}'
ORDER BY COALESCE(latest_bid, 0) DESC`);

    state.auctionDetailCache[auctionId] = rows;
    state.currentAuctionLots = rows;
    renderAuctionLots("");
  } catch (err) {
    console.error(err);
    document.getElementById("auction-lots-grid").innerHTML = `<p class="empty">Could not load lots.</p>`;
  }
}

// ── Data: lot detail ──────────────────────────────────────

async function loadLotDetail(auctionId, lotId) {
  const content = document.getElementById("lot-detail-content");
  content.innerHTML = `<p class="loading">Loading lot…</p>`;

  try {
    const safeAid = String(auctionId).replace(/\D/g, "");
    const safeLid = String(lotId).replace(/\D/g, "");

    const [lotRows, bidRows] = await Promise.all([
      queryRows(`SELECT auction_id, auction_title, lot_id, lot_title, image_url, location, lot_url, latest_bid, description, lot_images
FROM live_lot_prices
WHERE lot_id = '${safeLid}' AND auction_id = '${safeAid}'
LIMIT 1`),
      queryRows(`SELECT bid, CAST(scraped_at AS VARCHAR) AS scraped_at
FROM lot_prices
WHERE lot_id = '${safeLid}' AND auction_id = '${safeAid}'
ORDER BY scraped_at DESC
LIMIT 20`),
    ]);

    const lot = lotRows[0];
    if (!lot) {
      content.innerHTML = `<p class="empty">Lot not found.</p>`;
      return;
    }

    renderLotDetail(lot, bidRows);
  } catch (err) {
    console.error(err);
    content.innerHTML = `<p class="empty">Could not load lot details.</p>`;
  }
}

// ── Rendering ─────────────────────────────────────────────

function renderAuctions(rows) {
  const tbody = document.getElementById("auctions-body");

  if (!rows.length) {
    tbody.innerHTML = `<tr><td colspan="3" class="empty">No live auctions.</td></tr>`;
    return;
  }

  tbody.innerHTML = rows
    .map((row) => `
      <tr data-id="${escapeHtml(String(row.auction_id))}">
        <td>
          <span class="auction-name">
            ${escapeHtml(row.auction_title || `Auction ${row.auction_id}`)}
            <span class="row-arrow">→</span>
          </span>
        </td>
        <td class="num">${Number(row.lot_count || 0).toLocaleString()}</td>
      </tr>
    `)
    .join("");

  tbody.querySelectorAll("tr[data-id]").forEach((row) => {
    row.addEventListener("click", () => {
      location.hash = `#auction/${row.dataset.id}`;
    });
  });
}

function renderAuctionLots(search) {
  const rows = search
    ? state.currentAuctionLots.filter((r) => {
        const hay = `${r.lot_title || ""} ${r.location || ""}`.toLowerCase();
        return hay.includes(search);
      })
    : state.currentAuctionLots;

  document.getElementById("auction-lots-count").textContent = `${rows.length} lots`;
  document.getElementById("auction-lots-grid").innerHTML = rows.length
    ? rows.map((r) => lotCardHTML(r, false)).join("")
    : `<p class="empty">No lots match.</p>`;
}

function renderLotDetail(lot, bids) {
  const images = parseLotImages(lot.lot_images, lot.image_url);
  const primary = images[0] || lot.image_url || "";
  const latestBid = lot.latest_bid != null ? lot.latest_bid
    : bids.length > 0 && bids[0].bid != null ? bids[0].bid : null;

  const content = document.getElementById("lot-detail-content");
  content.innerHTML = `
    <div class="lot-detail-layout">
      <div class="lot-detail-images">
        ${primary
          ? `<img id="lot-main-img" class="lot-main-image" src="${escapeHtml(primary)}" alt="${escapeHtml(lot.lot_title || "")}" />`
          : `<div class="lot-main-image"></div>`
        }
        ${images.length > 1 ? `
          <div class="lot-thumb-strip">
            ${images.map((url, i) => `
              <img class="lot-thumb ${i === 0 ? "active" : ""}"
                   src="${escapeHtml(url)}"
                   data-full="${escapeHtml(url)}"
                   loading="lazy" />
            `).join("")}
          </div>
        ` : ""}
      </div>

      <div class="lot-detail-info">
        <a class="lot-auction-crumb" href="#auction/${escapeHtml(String(lot.auction_id))}">
          ← ${escapeHtml(lot.auction_title || `Auction ${lot.auction_id}`)}
        </a>

        <h2 class="lot-detail-title">${escapeHtml(lot.lot_title || `Lot ${lot.lot_id}`)}</h2>
        ${lot.location ? `<p class="lot-detail-location">${escapeHtml(lot.location)}</p>` : ""}

        <div class="bid-box">
          <p class="bid-box-label">Current bid</p>
          <p class="bid-box-amount">${latestBid != null
            ? `$${Number(latestBid).toLocaleString(undefined, { maximumFractionDigits: 2 })}`
            : "No bids yet"
          }</p>
        </div>

        ${lot.lot_url ? `
          <a class="lot-cta" href="${escapeHtml(lot.lot_url)}" target="_blank" rel="noreferrer">
            Bid on Lloyds Auctions →
          </a>
        ` : ""}

        ${lot.description ? `
          <div class="lot-detail-description">
            <h3>Description</h3>
            <div class="lot-description-body">${sanitizeDescription(lot.description)}</div>
          </div>
        ` : ""}

        ${bids.length ? `
          <div class="bid-history-section">
            <h3>Bid history</h3>
            <ul class="bid-history">
              ${bids.map((b) => `
                <li>
                  <span class="bid-amount">${b.bid != null
                    ? `$${Number(b.bid).toLocaleString()}`
                    : "—"
                  }</span>
                  <span class="bid-time">${escapeHtml(formatTimestamp(b.scraped_at))}</span>
                </li>
              `).join("")}
            </ul>
          </div>
        ` : ""}
      </div>
    </div>
  `;

  content.querySelectorAll(".lot-thumb").forEach((thumb) => {
    thumb.addEventListener("click", () => {
      content.querySelectorAll(".lot-thumb").forEach((t) => t.classList.remove("active"));
      thumb.classList.add("active");
      const main = document.getElementById("lot-main-img");
      if (main) main.src = thumb.dataset.full;
    });
  });
}

function lotCardHTML(row, showAuction) {
  const title = escapeHtml(row.lot_title || `Lot ${row.lot_id}`);
  const auctionTitle = escapeHtml(row.auction_title || "");
  const location = row.location ? escapeHtml(row.location) : "";
  const image = row.image_url ? escapeHtml(row.image_url) : "";
  const href = `#lot/${row.auction_id}/${row.lot_id}`;
  const bid = row.latest_bid == null
    ? "No bids yet"
    : `$${Number(row.latest_bid).toLocaleString(undefined, { maximumFractionDigits: 2 })}`;
  const desc = row.description ? escapeHtml(stripHtml(row.description).slice(0, 120)).trimEnd() + "…" : "";

  return `
    <article class="lot-card" onclick="location.hash='${escapeHtml(href)}'">
      ${image
        ? `<img class="lot-image" src="${image}" alt="${title}" loading="lazy" />`
        : `<div class="lot-image"></div>`
      }
      <div class="lot-body">
        ${showAuction && auctionTitle ? `<p class="lot-auction">${auctionTitle}</p>` : ""}
        <span class="lot-title">${title}</span>
        ${desc ? `<p class="lot-desc">${desc}</p>` : ""}
        ${location ? `<p class="lot-location">${location}</p>` : ""}
        <div class="lot-footer">
          <p class="lot-bid">${bid}</p>
          <a class="lot-link" href="${escapeHtml(href)}">View lot →</a>
        </div>
      </div>
    </article>
  `;
}

// ── Helpers ───────────────────────────────────────────────

async function queryRows(sql) {
  const result = await state.client.sqlJson(sql);
  return Array.isArray(result?.data) ? result.data : [];
}

function parseLotImages(val, fallback) {
  let imgs = [];

  if (Array.isArray(val)) {
    imgs = val.filter(Boolean);
  } else if (typeof val === "string" && val.length > 2) {
    // Spice returns VARCHAR[] as a string like [url1, url2, url3]
    const trimmed = val.trim();
    if (trimmed.startsWith("[") && trimmed.endsWith("]")) {
      try {
        // Try JSON parse first (works if URLs are quoted)
        const parsed = JSON.parse(trimmed);
        if (Array.isArray(parsed)) {
          imgs = parsed.filter(Boolean);
        }
      } catch {
        // Not valid JSON — split on ", " boundary
        imgs = trimmed
          .slice(1, -1)
          .split(", ")
          .map((s) => s.trim())
          .filter(Boolean);
      }
    } else if (trimmed.startsWith("{") && trimmed.endsWith("}")) {
      imgs = trimmed
        .slice(1, -1)
        .split(",")
        .map((s) => s.trim().replace(/^"|"$/g, ""))
        .filter(Boolean);
    }
  }

  if (!imgs.length && fallback) imgs = [fallback];
  return imgs;
}


function formatTimestamp(value) {
  const d = new Date(value);
  return Number.isNaN(d.getTime()) ? String(value || "") : d.toLocaleString();
}

function sanitizeDescription(html) {
  // Allow the auction company's HTML (p, b, strong, br, ul, li, a) but strip scripts and iframes
  return String(html)
    .replace(/<script[\s\S]*?<\/script>/gi, "")
    .replace(/<iframe[\s\S]*?<\/iframe>/gi, "")
    .replace(/\son\w+="[^"]*"/gi, "")
    .replace(/\son\w+='[^']*'/gi, "");
}

function stripHtml(html) {
  return String(html)
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}
