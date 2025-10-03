# Hydra Agent Field Guide

This guide gives future agents a map of Hydra's runtime surfaces, code organization, and common workflows so fresh sessions can skip expensive rediscovery.

## System Topology

```
docs (browser UI) ⇆ service_router/router.py (Python orchestrator) ⇆ local services (ASR/TTS/LLM/MCP)
                       ⇡                                       ⇣
                NKN transport via Net module           bridge-node/nkn_bridge.js (Node.js)
```

- The browser workspace toggles HTTP vs NKN for every request (`docs/js/main.js:144`).
- Python router supervises service processes, drives HTTP multiplexing, and owns relay state (`service_router/router.py:35`).
- Node bridge provides the raw NKN MultiClient connection used by the router's DM queue (`service_router/bridge-node/nkn_bridge.js:18`).

## Directory Map

| Path | Purpose | Highlights |
| --- | --- | --- |
| `docs/` | Front-end workspace + local HTTPS dev server | UI entrypoint (`docs/index.html:5`), SPA runtime (`docs/js/main.js:1`), TLS dev server (`docs/server.py:53`)
| `docs/js/` | Modular node graph runtime | Transport core (`docs/js/net.js:8`), graph layout (`docs/js/graph.js:1`), node wiring (`docs/js/router.js:1`)
| `service_router/` | Unified NKN router and config | Bootstrap & orchestration (`service_router/router.py:35`), config defaults (`service_router/router_config.json:1`)
| `service_router/bridge-node/` | Node-based NKN sidecar | MultiClient runner (`service_router/bridge-node/nkn_bridge.js:18`)

## Runtime Surfaces

### Python Router (service_router/router.py)
- Self-bootstraps a private virtualenv and dependencies for resilience (`service_router/router.py:45`).
- Loads relay + endpoint targets from `router_config.json` with sensible localhost defaults (`service_router/router.py:92`).
- `ServiceWatchdog` clones and supervises external services—including Piper TTS, Whisper ASR, Ollama farm, and Hydra MCP—using a restart/backoff loop (`service_router/router.py:327`).
- Automatically fetches service scripts into `.services/<name>/` via git and tracks metadata (`service_router/router.py:414`).
- Manages per-service HTTP worker pools, stream formatting, and QR helpers for presenting relay info through the terminal dashboard (`service_router/router.py:166`).

### NKN Bridge (service_router/bridge-node/nkn_bridge.js)
- Accepts relay seed, identifier, and transport tuning via environment variables (`service_router/bridge-node/nkn_bridge.js:6`).
- Emits structured stdout events for ready status, probes, and DM payloads that the router consumes (`service_router/bridge-node/nkn_bridge.js:55`).
- Listens for JSON control messages on stdin to relay outbound DMs and enforces heartbeat-based self exits for hung connections (`service_router/bridge-node/nkn_bridge.js:73`).

### Graph Workspace (docs/index.html & js/main.js)
- Minimal HTML shell loads the node graph bundle, NKN SDK, and QR libraries (`docs/index.html:5`).
- `main.js` wires together transport, graph rendering, node factories, and workspace synchronization in one place (`docs/js/main.js:20`).
- Transport toggle button flips CFG state and reinitializes NKN clients as needed (`docs/js/main.js:144`).

### Local HTTPS Dev Server (docs/server.py)
- Bootstraps a venv on first launch and installs `cryptography` automatically (`docs/server.py:53`).
- Generates self-signed certificates covering LAN/public IPs and optional SANs (`docs/server.py:162`).
- Supports Let’s Encrypt, Smallstep, and GCP Private CA options via CLI flags (`docs/server.py:100`).
- Serves `docs/config.json`-defined roots and prints LAN/public URLs for quick device pairing (`docs/server.py:148`).

## Front-End Core Modules

- **Configuration & Storage**: `CFG` is persisted to `localStorage` and seeded with transport + wiring defaults (`docs/js/config.js:9`). Use `saveCFG` to persist edits.
- **Transport Layer**: `Net` abstracts HTTP vs NKN for JSON, blob, and streaming requests, caching NKN seeds in storage and maintaining pending requests (`docs/js/net.js:25`).
- **Wire Router**: User-created connections live in `CFG.wires`; `Router` handles port registration, wiring UI, and payload fan-out (`docs/js/router.js:23`).
- **Graph Engine**: `createGraph` owns workspace canvas transforms, node lifecycle, link drawing, and relay state badges (`docs/js/graph.js:18`).
- **Node Registry**: `NodeStore` persists per-node configuration and provides typed defaults for every node category (`docs/js/nodeStore.js:9`).
- **Workspace Sync**: Generates QR codes, attaches modal listeners, and drives NKN workspace sharing flow (`docs/js/workspaceSync.js:35`).
- **Utilities**: Common helpers for logging, QR conversion, boolean toggles, and ASR prompt sanitization (`docs/js/utils.js:1`).
- **Transport Toggle UI**: `makeTransportButtonUpdater` renders the status pill based on current transport state (`docs/js/transport.js:3`).
- **QR Scanner**: Camera capture + jsQR integration for filling inputs from codes (`docs/js/qrScanner.js:21`).
- **Sentence & Streaming Helpers**: Handles NDJSON/Server-Sent Events and sentence stabilization for LLM streams (`docs/js/sentence.js:11`).
- **Flows Library**: Modal-based flow manager for saving, importing, editing, and loading named workspaces (`docs/js/flows.js:1`).

## Node Implementations

| Node | Responsibilities | Source |
| --- | --- | --- |
| LLM | Model discovery, NDJSON streaming, image/tool payload prep, relay state updates | `docs/js/llm.js:11` |
| ASR | Model metadata, Web Audio capture, chunked uploads, sign-off filtering | `docs/js/asr.js:20` |
| TTS | Model caching, audio queue, oscilloscope visualization | `docs/js/tts.js:20` |
| NKN DM | Handshake state machine, inbox tracking, invite modal control | `docs/js/nknDm.js:3` |
| MCP | Handles MCP connection lifecycle, resource/tool syncing, status rendering | `docs/js/mcp.js:52` |
| Media Stream | Screenshots + audio capture, base64 packaging, remote playback | `docs/js/mediaNode.js:24` |
| Orientation | Device orientation permissions, throttling, quaternion conversion | `docs/js/orientation.js:20` |
| Location | Geolocation watch, geohash/latlon formatting, payload throttling | `docs/js/location.js:76` |

Defaults (base URLs, auth slots, feature toggles) are centralized in `NodeStore.defaultsByType` for quick inspection (`docs/js/nodeStore.js:9`). When adding a new node type, mirror this pattern so `NodeStore.ensure` remains authoritative (`docs/js/nodeStore.js:41`).

## Workspace & Share Flow

1. QR modal exposes current workspace state as a signed payload; `workspaceSync` ensures QR dependencies are loaded before rendering (`docs/js/workspaceSync.js:80`).
2. Incoming `?sync=` URLs are stripped and stored while forcing NKN transport to connect to the sender (`docs/js/workspaceSync.js:134`).
3. NKN messages fan through `Net.nkn` listeners, so ensure the transport toggle is in sync before expecting share invitations (`docs/js/net.js:119`).

## Router Configuration

- Relay seeds, names, and service assignments are in `router_config.json:1`. Rotate `seed_hex` values with `generate_seed_hex()` (`service_router/router.py:122`).
- Targets map human names to local service URLs (`router_config.json:3`). Update these to point at remote nodes if services move.
- Bridge tuning (subclients, heartbeats) uses the `bridge` section; defaults match `nkn_bridge.js` expectations (`router_config.json:19`).

## Operational Playbooks

### Start the Graph UI (self-signed TLS)
```bash
python3 docs/server.py --cert-mode self
```
- First run builds `docs/venv` and emits LAN/public URLs. Adjust SANs via `docs/config.json:1` before launch.

### Run the Unified Router
```bash
python3 service_router/router.py --config router_config.json
```
- Spawns bridge sidecars + service watchdog; watch terminal for QR output and service status.
- Ensure required repos are reachable—`ServiceWatchdog.ensure_sources()` needs git and network access (`service_router/router.py:340`).

### Refresh NKN bridge dependencies
```bash
(cd service_router/bridge-node && npm install)
```
- `package.json` pins `nkn-sdk`; reinstall if Node modules were pruned.

### Manage Workspace Flows
- Use the `Flows` button in the toolbar to open the modal library.
- Save the current canvas, import/export JSON snapshots, or load flows back into the editor within the same modal (`docs/js/flows.js`).

## Troubleshooting Checklist

- **NKN badge stays red**: Confirm CDN `nkn.min.js` loads (browser console) and reinitialize transport via toggle (`docs/js/main.js:147`).
- **Workspace QR fails**: Verify `QRCode.toCanvas` is available; `ensureQrReady()` waits for CDN script but logs errors if missing (`docs/js/workspaceSync.js:97`).
- **Service won’t start**: Inspect `.logs/<service>.log` created by `ServiceWatchdog` and check `state.last_error` snapshots (`service_router/router.py:384`).
- **Ollama health fallback**: Router auto-frees ports and retries before falling back to “system” mode (`service_router/router.py:438`).
- **Audio underruns**: TTS node increments `underruns` on empty buffers; adjust stream chunk sizes or model latency (`docs/js/tts.js:128`).

## External Dependencies

- CDN: NKN SDK 1.3.6, jsQR 1.4.0, QRCode 1.5.4 (`docs/index.html:13`).
- Python: requests, python-dotenv, qrcode (auto-installed) (`service_router/router.py:61`).
- Node: nkn-sdk (bridge) (`service_router/bridge-node/package.json`).

## Contributing Notes

- Maintain ASCII-friendly UI assets and avoid heavy dependencies to keep front-end lightweight.
- New nodes should expose ports via `Router.register` and persist configuration through `NodeStore` to participate in exports/imports (`docs/js/router.js:23`).
- For additional backend services, extend `ServiceWatchdog.DEFINITIONS` and map aliases in `SERVICE_TARGETS` to keep router lookups consistent (`service_router/router.py:100`).

Keep this guide updated when new node types, transports, or services land so future agents can onboard instantly.
