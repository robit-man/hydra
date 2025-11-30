# MediaStream

## Overview
Captures local camera/microphone streams and forwards them to peers over NKN. Provides on-card preview, torch control, and target management.

## Inputs
- `mute` (implicit via UI) — no graph input required.

## Outputs
- `status`/peer updates (on-card); media is forwarded directly to peers rather than router ports.

## Key Settings
- Capture: `includeVideo`, `includeAudio`, `frameRate`, `videoWidth`, `videoHeight`, `compression`.  
- Audio: `audioSampleRate`, `audioChannels`, `audioFormat`, `audioBitsPerSecond`.  
- Session: `running`, `lastFacingMode`, `torchEnabled`.  
- Transport: `targets`, `pendingAddress`, `relay` (uses node transport button).

## Data Contracts
- Media is sent over transport to peers; router outputs are limited (status).  
- On-card preview uses raw MediaStream; no `{ text }` payloads are emitted to Router.  
- Target list is an array of NKN addresses.

## How It Works
- Requests MediaDevices permission, starts capture, displays local preview, and transmits encoded frames/audio to configured NKN targets.  
- Maintains active target list and per-peer metadata (latency, status).

## Basic Use
1) Add NKN targets in settings (or on-card).  
2) Press ▶ to start/stop streaming.  
3) Wire status outputs downstream if needed; remote peers receive the media directly.

## Advanced Tips
- Lower `compression` and `frameRate` for constrained links.  
- Toggle torch on supported devices via the on-card button.  
- Use multiple nodes for multi-room streaming with distinct target sets.

## Troubleshooting
- Permission denied: re-request camera/mic in the browser.  
- Black preview: check device selection and `videoWidth`/`videoHeight` constraints.  
- No remote feed: confirm target address and transport badge (NKN connectivity).
