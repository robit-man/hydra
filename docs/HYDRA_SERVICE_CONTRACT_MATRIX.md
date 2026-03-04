# Hydra Service Contract Matrix

## Ownership
- Owner: `service_router` maintainers.
- Primary artifact: `docs/HYDRA_SERVICE_CONTRACT_MATRIX.json`.
- Last refresh date: `2026-03-03`.

## Update Rules
1. Update this matrix whenever routes, auth, bind behavior, or discovery payloads change in:
   - `service_router/router.py`
   - `service_router/router_config.json`
   - `service_router/.services/*`
   - `docs/js/{asr,tts,llm,mcp,webScraper,pointcloud}.js`
2. Preserve the service keys in JSON under `.services`:
   - `ollama`, `asr`, `tts`, `mcp`, `web_scrape`, `depth_any`.
3. Keep canonical schemas backward-compatible by adding coercion notes before tightening required fields.
4. Re-run verification commands from this file after each edit.

## Hydra Service Inventory (Router Targets)

| Router target | Watchdog service | Health path | Primary frontend routes | Auth mode | Bind behavior |
| --- | --- | --- | --- | --- | --- |
| `ollama` | `ollama_farm` | `/_health` | `/v1/models`, `/models`, `/api/tags`, `/api/show`, `/api/pull`, `/api/chat` | none | CLI args `--host`, `--front-port`, `--back-port`; proxy catch-all |
| `asr` | `whisper_asr` | `/health` | `/models`, `/recognize`, `/recognize/stream/*` | none | `ASR_HOST`/`ASR_PORT` |
| `tts` | `piper_tts` | `/health` | `/models`, `/models/pull`, `/speak` | conditional `X-API-Key` or bearer session | `TTS_BIND`/`TTS_PORT` with free-port fallback |
| `mcp` | `mcp_server` | `/healthz` | `/mcp/status`, `/mcp/query`, `/mcp/tool`, `/mcp/ws` | conditional API key | `MCP_BIND`/`MCP_PORT` |
| `web_scrape` | `web_scrape` | `/health` | `/session/start`, `/navigate`, `/click`, `/type`, `/scroll*`, `/dom`, `/screenshot`, `/events` | conditional `X-API-Key` or bearer API key | `SCRAPE_BIND`/`SCRAPE_PORT` |
| `depth_any` | `depth_any` | `/api/v1/health` | `/api/models/*`, `/api/load_model`, `/api/process`, `/api/job/<id>`, `/api/floor_align*`, `/api/export/glb` | none | dynamic first free port in `5000-5009` |

## Canonical Discovery Schema Targets

### `health` (`hydra.health.v1`)
Required:
- `status`
- `service`

Optional:
- `timestamp_ms`, `uptime_seconds`, `ready`, `requests_served`, `network`, `security`, `tunnel`, `fallback`, `legacy`

Compatibility coercion:
- `{ok:true}` without `status` maps to `status="ok"`.
- `/healthz` and `/api/v1/health` are valid health probes and normalize into the same schema.
- Missing `service` should be injected from router target identity.

### `router_info` (`hydra.router_info.v1`)
Required:
- `status`, `service`, `transport`, `base_url`, `local`, `tunnel`, `fallback`

Optional:
- `http_endpoint`, `ws_endpoint`, `security`

Compatibility coercion:
- `transport` can be derived from `fallback.selected_transport`.
- `base_url` fallback precedence: tunnel -> upnp -> lan -> local.

### `tunnel_info` (`hydra.tunnel_info.v1`)
Required:
- `status`, `running`, `fallback`

Optional:
- `tunnel_url`, `stale_tunnel_url`, `error`, `message`

Compatibility coercion:
- `status=error` + `stale_tunnel_url` implies stale tunnel state.
- `running=true` + `tunnel_url` implies usable active endpoint.

## Compatibility Gaps Identified
- Health path drift across services (`/_health`, `/healthz`, `/api/v1/health`, `/health`).
- Health payload envelope drift (`ok` flag vs `status` string).
- Missing `depth_any` backend route `/api/process_base64` while frontend currently calls it in relay mode.
- Hydra services do not currently expose per-service `/router_info` and `/tunnel_info`; router synthesis is required.

## Teleoperation Parity Anchors
- Router control-plane routes and resolve behavior:
  - `/home/robit/Documents/repositories/Dropbear-Neck-Assembly/teleoperation/router/router.py:3417`
  - `/home/robit/Documents/repositories/Dropbear-Neck-Assembly/teleoperation/router/router.py:3447`
  - `/home/robit/Documents/repositories/Dropbear-Neck-Assembly/teleoperation/router/router.py:3494`
  - `/home/robit/Documents/repositories/Dropbear-Neck-Assembly/teleoperation/router/router.py:3501`
  - `/home/robit/Documents/repositories/Dropbear-Neck-Assembly/teleoperation/router/router.py:3526`
- Service discovery contracts:
  - `/home/robit/Documents/repositories/Dropbear-Neck-Assembly/teleoperation/adapter/adapter.py:2643`
  - `/home/robit/Documents/repositories/Dropbear-Neck-Assembly/teleoperation/adapter/adapter.py:2727`
  - `/home/robit/Documents/repositories/Dropbear-Neck-Assembly/teleoperation/adapter/adapter.py:2752`
  - `/home/robit/Documents/repositories/Dropbear-Neck-Assembly/teleoperation/vision/camera_route.py:4988`
  - `/home/robit/Documents/repositories/Dropbear-Neck-Assembly/teleoperation/vision/camera_route.py:5039`
  - `/home/robit/Documents/repositories/Dropbear-Neck-Assembly/teleoperation/audio/audio_route.py:2224`
  - `/home/robit/Documents/repositories/Dropbear-Neck-Assembly/teleoperation/audio/audio_route.py:2275`

## Verification
```bash
jq . docs/HYDRA_SERVICE_CONTRACT_MATRIX.json
jq '.services | keys' docs/HYDRA_SERVICE_CONTRACT_MATRIX.json
rg "depth_any|web_scrape|mcp" docs/HYDRA_SERVICE_CONTRACT_MATRIX.*
```
