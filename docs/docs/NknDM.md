# NknDM (Direct Message)

## Overview
Peer-to-peer messaging node over NKN. Establishes handshakes, maintains peer lists, and streams small payloads or chat-style messages between Hydra graphs.

## Inputs
- `text` — Message payload to send.

## Outputs
- `text` — Incoming messages (as payload objects).  
- `status` — Connection status updates.

## Key Settings
- `address` — Target NKN address.  
- `chunkBytes` — Max chunk size for messages.  
- `heartbeatInterval` — Keepalive seconds.  
- `componentId`, `handshake`, `peer`, `allowedPeers`, `autoAccept`, `autoChunk`.

## Data Contracts
- Inputs: strings or `{ text }`; large payloads are chunked when `autoChunk` is on.  
- Outputs: messages as `{ text, from, ts, meta? }`; status strings on the card.  
- Transport-only; router handles JSON payloads, not binary.

## How It Works
- Uses the configured `address` or scanned QR to initiate a handshake.  
- Auto-accept can permit inbound connections without prompts.  
- Messages are chunked/signed based on settings and delivered via Router.

## Basic Use
1) Set or scan the peer `address`.  
2) Wire TextInput → `text` to send; consume `text` output downstream.  
3) Watch status badges/logs for connection success.

## Advanced Tips
- Set `autoChunk` to automatically split larger payloads.  
- Use `allowedPeers` to whitelist senders when `autoAccept` is on.  
- `componentId` helps isolate multiple DM nodes in one graph.

## Troubleshooting
- No handshake: confirm peer online and address spelled correctly.  
- Dropped messages: reduce `chunkBytes` or increase `heartbeatInterval` for long-lived links.
