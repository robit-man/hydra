# FileTransfer

## Overview
Chunked file sender/receiver over Hydra transport (typically NKN). Handles chunk sizing, acceptance, and optional keying for peer deliveries.

## Inputs
- File input is user-triggered in the card UI; no graph inputs required.

## Outputs
- Emits transfer status and payloads to connected peers (no router outputs).

## Key Settings
- `chunkSize` — Bytes per chunk (default 1024).  
- `autoAccept` — Automatically accept incoming transfers.  
- `defaultKey` — Optional shared key/token.  
- `preferRoute` — Preferred route/peer.

## Data Contracts
- Offers/receipts use file metadata plus chunked binary payloads over transport; router outputs are limited to status text.  
- No text/audio outputs are emitted; downstream nodes must be transport-aware.

## How It Works
- Splits selected files into chunks, sends over the configured transport, and reassembles on the receiver.  
- Listens for incoming offers; accepts automatically when enabled.

## Basic Use
1) Set `autoAccept` to taste; adjust `chunkSize` for your link.  
2) Initiate a send from the card UI, pick a file, and confirm the peer/route.  
3) Monitor status text on the card for completion or errors.

## Advanced Tips
- Larger `chunkSize` speeds LAN transfers; smaller values help unreliable relays.  
- Use `defaultKey` to gate transfers to trusted peers only.

## Troubleshooting
- Stalls: lower `chunkSize` or verify relay connectivity.  
- Unauthorized: ensure both sides share the same `defaultKey` when required.
