# WebScraper (Browser Automation)

## Overview
Remote-controlled headless browser node for navigation, clicking, typing, scrolling, screenshots, and DOM capture via a Hydra web-scrape backend.

## Inputs
- `url` — Target URL for open/nav.  
- `action` — Command (connect/open/close/nav/back/forward/click/type/enter/scroll/drag/screenshot/dom/events).  
- `selector` — CSS selector for element actions.  
- `text` — Typing payload or default input.  
- `amount` — Scroll pixels.  
- `xy` — Coordinates for click/drag helpers.  
- `sid` — Session override.

## Outputs
- `status` — Status text for logging.  
- `frame` — Screenshot (wrapped or raw per config).  
- `dom` — DOM snapshot payload.  
- `log` — On-card log stream.  
- `rawFrame` — Base64 screenshot string.

## Key Settings
- Transport: `base`, `relay`, `service`, `api`, `endpointMode`.  
- Browser: `headless`, `autoScreenshot`, `autoCapture`, `frameRate`, `frameOutputMode`.  
- Session: `sid` override.

## Data Contracts
- Actions: `{ action, selector?, text?, url?, amount?, xy? }`. `selector` and `text` can be strings or `{ text }`; `xy` is `{ x, y, viewportW?, viewportH?, naturalW?, naturalH? }`.  
- Outputs: `status` strings; `frame` `{ blobUrl?, b64?, size?, mode }`; `dom` `{ dom, length, sid }`; `log` text; `rawFrame` base64 string.

## How It Works
- Connects to the scraper service (HTTP or NKN) and issues JSON RPC-style actions.  
- Manages session IDs so multiple actions reuse the same browser.  
- Captures frames/DOM per action or on intervals when auto-capture is enabled.

## Basic Use
1) Set `base` to your scraper service; keep `service=web_scrape`.  
2) Send `{ action: 'connect' }` then `{ action: 'open', url: 'https://…' }`.  
3) Wire TextInput to emit `action`/`selector`/`text` fields for clicks/typing.  
4) Consume `status`/`frame`/`dom` downstream for parsing or display.

## Advanced Tips
- Use `frameOutputMode=raw` for downstream image processors.  
- `autoCapture=true` with `frameRate` streams frames continuously; mind bandwidth.  
- Provide `sid` to attach to an existing remote browser session.  
- For drags, pass `{ action:'drag', selector, xy:{x,y,viewportW,viewportH} }`.

## Routing & Compatibility
- Upstream: TextInput configured with action and selector fields; LogicGate for conditional actions.  
- Downstream: TextDisplay/LLM for status/logs; Image consumers for frames.  
- Avoid sending large `frame` payloads to text-only nodes; use `status`/`dom` instead.

## Signals & Router
- Outputs are JSON payloads; image data is base64 when wrapped.  
- Actions are idempotent per session; check `status` for progression.

## Troubleshooting
- No session: ensure `base` reachable and `service` matches backend.  
- Blank frames: set `headless=false` when site blocks headless browsers.  
- Slow DOM: reduce `autoCapture` and prefer targeted `dom` actions.
