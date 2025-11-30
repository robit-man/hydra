# Location

## Overview
Streams geolocation updates from the browser for location-aware flows.

## Inputs
None; uses browser geolocation.

## Outputs
- Location payloads (`coords`, `timestamp`) to downstream nodes.

## Key Settings
- `format` — Output style (`raw`).  
- `precision` — Decimal places for coordinates.  
- `running` — Runtime flag (toggled from the card).

## How It Works
- Requests geolocation permission, polls/streams position updates, rounds to `precision`, and emits via the router.

## Basic Use
1) Press ▶ to start/stop location streaming.  
2) Wire outputs to logging, map, or routing nodes.

## Advanced Tips
- Browsers often require HTTPS for geolocation.  
- Reduce `precision` for privacy when sharing coordinates broadly.

## Troubleshooting
- Permission denied: allow location access in the browser.  
- No updates: ensure the device has a GPS/network fix and isn’t in airplane mode.
