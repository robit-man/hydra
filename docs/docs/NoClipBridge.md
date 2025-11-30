# NoClipBridge

## Overview
Bridge node for relaying data through the NoClip network. It advertises Hydra identity, joins rooms, and forwards messages between peers.

## Inputs
- `text` — Payload to forward into the bridge.

## Outputs
- `text` — Messages received from the bridge.  
- `status` — Bridge/room status.

## Key Settings
- `targetPub`, `targetAddr` — Optional explicit peers.  
- `room` — Room name (`auto` uses discovery).  
- `autoConnect` — Connect on startup.

## How It Works
- Connects to the NoClip overlay, joins the selected room, and relays payloads with metadata so downstream nodes can route/respond.

## Basic Use
1) Leave `room=auto` for discovery or set a specific room.  
2) Wire TextInput/LLM → `text`; read `text` output for inbound messages.  
3) Enable `autoConnect` to rejoin automatically after reloads.

## Advanced Tips
- Provide `targetPub`/`targetAddr` for direct routing when discovery is unreliable.  
- Combine with LogicGate to filter inbound bridge traffic before handing to agents.

## Troubleshooting
- No peers: verify room name and upstream NoClip connectivity.  
- Flooding: add LogicGate filters or isolate bridges per conversation.
