# Website (GitHub Pages)

This folder contains a production-style, ecommerce-focused auction marketplace UI.

It queries Spice Cloud directly from the browser via `spice.js` (no `site/data/*.json` snapshot files).

## Local preview

From repository root:

```bash
python3 -m http.server 8080
```

Then open:

- <http://localhost:8080/site/>

## Runtime config

The site reads `window.AUCTIONS_SITE_CONFIG` from `site/config.js`:

- `httpUrl`
- `flightUrl`
- `apiKey`

> `apiKey` is public in browser-delivered JS. Use a read-only/restricted key only.

## Deploy

Deployment is automated by:

- `.github/workflows/deploy-site-pages.yml`

The workflow publishes `site/` and writes `site/config.js` from repository variables.

## Customize quickly

- Content/layout: `site/index.html`
- Visual styling: `site/assets/styles.css`
- Motion + data queries: `site/assets/main.js`
- Runtime config defaults: `site/config.js`

## Notes

- Includes reduced-motion fallback (`prefers-reduced-motion`).
- Uses no build step and no framework runtime.
