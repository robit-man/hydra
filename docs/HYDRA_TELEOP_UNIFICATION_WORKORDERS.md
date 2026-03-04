# Hydra + Teleoperation Unification Workorders

## 1) Goal
Unify Hydra's service router/watchdog/frontend with the teleoperation stack patterns so Hydra gains:
- Service-level and router-level `/health` consistency.
- Endpoint discovery parity (`/router_info`, `/tunnel_info`, snapshot aggregation, resolved endpoint synthesis).
- Cloudflared lifecycle robustness (install, restart, stale URL handling, fallback signaling).
- NKN discovery and forwarding parity (`resolve_tunnels`, `service_rpc_request`, request/reply tracking).
- Frontend endpoint auto-resolution and transport-aware fallback behavior.

## 2) Architecture Review Summary

### 2.1 Hydra current state
- `service_router/router.py` is a strong NKN relay/data-plane with embedded watchdog and port-isolation logic.
- Hydra currently lacks a router HTTP control-plane equivalent to teleoperation router endpoints (`/health`, `/nkn/info`, `/nkn/resolve`, `/services/snapshot`).
- Hydra service supervision is process-centric and restart-capable, but not yet at teleoperation watchdog activation/health-state-machine depth.
- Hydra frontend (`docs/js`) is modular and mature for graph workflows, but has no teleoperation-style router endpoint resolver and no cloudflared transport heuristics.

### 2.2 Teleoperation current state
- `teleoperation/app.py` provides a resilient watchdog orchestration model with:
- multi-mode health probes (`process`/`tcp`/`http`),
- activation windows and graceful degradation,
- dynamic port discovery and reclaim,
- single-instance locking and persisted desired state,
- controlled auto-update behavior.
- `teleoperation/router/router.py` provides router control-plane features Hydra is missing:
- service snapshot polling and endpoint normalization,
- endpoint resolution API and NKN DM request/reply tracking,
- telemetry and dashboard JSON surfaces,
- NKN sidecar lifecycle and readiness/error state surfaces.
- Teleoperation service routes (`adapter.py`, `camera_route.py`, `audio_route.py`) provide a common strategy:
- `/health` operational state,
- `/router_info` normalized discovery payload,
- `/tunnel_info` cloudflared state payload,
- fallback sections (`cloudflare`, `upnp`, `nats`, `nkn`, `local`) with selected transport.

### 2.3 Highest-impact gaps to close first
- Missing Hydra router HTTP control-plane.
- Missing canonical discovery payload contract for Hydra services.
- Missing cloudflared lifecycle and stale URL management strategy for Hydra services.
- Missing frontend resolver workflow that can consume snapshot/resolved payloads and auto-apply endpoint updates.

## 3) Target Unified Runtime Model
Browser `docs/js` (Hydra graph) uses:
- direct local HTTP when local is preferred,
- NKN relay mode for remote services,
- router-assisted endpoint discovery (`/nkn/resolve` + `resolve_tunnels_result`) and transport selection.

Hydra router owns:
- service watchdog orchestration,
- NKN bridge lifecycle,
- service endpoint snapshot polling,
- normalized resolved endpoint API,
- health + telemetry APIs,
- optional per-service cloudflared endpoint management.

Each Hydra service is surfaced through a normalized discovery contract:
- `/health`
- `/router_info`
- `/tunnel_info`

## 4) Phased Workorders

Canonical execution files live under:
- `workorders/phase-00` through `workorders/phase-07`
- Each workorder file includes scoped file targets, acceptance metrics, and failure-mode preventions used by implementation agents.

## Phase 0 - Discovery Baseline

### WO-00.1 Service Contract Inventory
Scope:
- `service_router/router.py`
- `service_router/.services/*`
- Teleoperation counterparts in `teleoperation/*`

Tasks:
- Build a machine-readable matrix of each Hydra service route surface:
- health endpoint,
- model/list endpoints,
- stream endpoints,
- auth requirements,
- current bind host/port behavior.
- Build matching matrix for teleoperation adapter/camera/audio/router.
- Define canonical schema for `health`, `router_info`, `tunnel_info` for Hydra.

Acceptance:
- Checked-in contract matrix artifacts:
  - `docs/HYDRA_SERVICE_CONTRACT_MATRIX.json`
  - `docs/HYDRA_SERVICE_CONTRACT_MATRIX.md`
- Phase-01 through Phase-07 workorders use these artifacts as the canonical schema and compatibility reference.

### WO-00.2 Endpoint Probe Harness
Scope:
- new script under `service_router/tools/`

Tasks:
- Add a probe CLI that tests all known services for:
- availability,
- status code,
- response shape,
- latency,
- route compatibility with target schema.

Acceptance:
- One command produces JSON output suitable for CI gating.

## Phase 1 - Router Control-Plane Parity

### WO-01.1 Add Hydra Router HTTP API Surface
Scope:
- `service_router/router.py`

Tasks:
- Embed Flask (or FastAPI) control-plane endpoints:
- `GET /`
- `GET /api`
- `GET /health`
- `GET /services/snapshot`
- `GET /nkn/info`
- `POST /nkn/resolve`
- Add request counters, uptime, and router network URL payloads.

Acceptance:
- Local curl against all endpoints returns valid JSON with stable schema.

### WO-01.2 Service Snapshot + Resolved Endpoint Engine
Scope:
- `service_router/router.py`

Tasks:
- Implement service polling logic with retries.
- Implement payload normalization/coercion similar to teleoperation router:
- tolerate `health`, `router_info`, or `tunnel_info` entrypoint differences,
- preserve previous good snapshot on temporary fetch failures,
- compute `resolved` per service with selected transport and endpoint fields.

Acceptance:
- `/services/snapshot` contains `services` and `resolved` blocks with deterministic fields.

### WO-01.3 NKN Resolve Request/Reply Tracking
Scope:
- `service_router/router.py`

Tasks:
- Add pending resolve map with TTL.
- Support inbound `resolve_tunnels` and outbound `resolve_tunnels_result` handling in router-level NKN handler.
- Expose resolve outcomes in telemetry fields.

Acceptance:
- `POST /nkn/resolve` resolves local and remote targets with timeout semantics.

## Phase 2 - Watchdog Hardening

### WO-02.1 Upgrade Watchdog Runtime State Machine
Scope:
- `service_router/router.py` (ServiceWatchdog section)

Tasks:
- Introduce per-service health modes (`process`, `tcp`, `http`) and probes.
- Add activation timeout, activation stability fallback, degraded state, and consecutive health failure thresholds.
- Add per-service resolved runtime port tracking and on-the-fly health-port rediscovery.

Acceptance:
- Router UI and logs distinguish `launching`, `activating`, `running`, `degraded`, `error`, `stopping` states.

### WO-02.2 Port Reclaim and Ownership Safety
Scope:
- `service_router/router.py`

Tasks:
- Add owned-vs-foreign pid detection prior to port reclaim.
- Add configurable reclaim policy (`owned-only` and optional force mode).
- Add deterministic stop escalation (`SIGINT`, `SIGTERM`, `SIGKILL`) with timing controls.

Acceptance:
- Reclaim actions are explicit in logs and avoid collateral process kills by default.

### WO-02.3 Single-Instance Lock + Desired State Persistence
Scope:
- `service_router/router.py`
- `service_router/.watchdog_runtime/` (new)

Tasks:
- Add file-lock mechanism to prevent duplicate watchdog instances.
- Persist per-service desired enabled states across restarts.

Acceptance:
- Second watchdog instance fails fast with useful diagnostics.

## Phase 3 - Cloudflared + Fallback Strategy for Hydra Services

### WO-03.1 Common Cloudflared Manager Utility
Scope:
- new module: `service_router/cloudflared_manager.py`
- router integration in `service_router/router.py`

Tasks:
- Implement reusable cloudflared lifecycle management:
- binary discovery and optional install,
- process launch and URL parse,
- exponential restart backoff,
- stale URL and rate-limit handling.

Acceptance:
- Manager can run against one test service and emit structured state transitions.

### WO-03.2 Hydra Service Discovery Payload Schema
Scope:
- `service_router/router.py`
- optional service wrappers under `service_router/wrappers/`

Tasks:
- For each Hydra service, expose normalized discovery payload containing:
- `service`, `transport`, `base_url`,
- `local`, `tunnel`, `fallback`,
- `security` and key feature routes.
- Prefer wrappers/proxies where upstream repos should remain untouched.

Acceptance:
- Every enabled Hydra service has normalized discovery data in snapshot output.

### WO-03.3 Fallback Metadata Surfaces
Scope:
- `service_router/router.py`

Tasks:
- Include fallback metadata blocks for `upnp`, `nats`, `nkn`, `local`, and selected transport.
- Include stale tunnel URL field and tunnel error details.

Acceptance:
- Frontend can choose transport without brittle endpoint assumptions.

## Phase 4 - NKN Router Feature Uplift

### WO-04.1 Router-level NKN Telemetry + Dashboard Data
Scope:
- `service_router/router.py`

Tasks:
- Add inbound/outbound byte and message counters.
- Add per-peer usage and endpoint hit aggregation.
- Add JSON dashboard payload endpoint (`/dashboard/data`).

Acceptance:
- Telemetry fields are queryable and stable over runtime.

### WO-04.2 Generic Service RPC Over NKN
Scope:
- `service_router/router.py`
- `docs/js/net.js`

Tasks:
- Add `service_rpc_request` / `service_rpc_result` event handling in router.
- Add frontend helper for service RPC with timeout and body-kind support.

Acceptance:
- Browser can call selected service path via NKN without direct relay URL knowledge.

## Phase 5 - Frontend Unification Workorders (`docs/js`)

### WO-05.1 Add Router Discovery Client Module
Scope:
- new module: `docs/js/routerDiscovery.js`
- `docs/js/main.js`
- `docs/js/config.js`

Tasks:
- Add persistent router target NKN address.
- Add resolver state machine for manual and periodic auto-resolve.
- Add request/reply correlation and timeout handling for NKN resolve.

Acceptance:
- Frontend can resolve remote endpoints from router NKN address and apply them without manual URL edits.

### WO-05.2 Extend `Net` for Router Resolve + RPC
Scope:
- `docs/js/net.js`

Tasks:
- Add `nknResolveTunnels(targetAddr, timeoutMs)` helper.
- Add `nknServiceRpc(service, path, options)` helper.
- Keep current `relay.health` and `http.request` flows backward-compatible.

Acceptance:
- Existing nodes continue working; new resolver + RPC features are available.

### WO-05.3 Endpoint Normalization and Transport Selection
Scope:
- new utility module: `docs/js/endpointResolver.js`
- call sites in `main.js` and node modules

Tasks:
- Port teleoperation resolver heuristics:
- normalize `resolved` + `snapshot` payload variants,
- prefer cloudflare when healthy and routable,
- fall back through upnp/nats/nkn/local order,
- reject loopback-only endpoints for remote mode.

Acceptance:
- Endpoint application logic is centralized and deterministic.

### WO-05.4 Frontend UX Controls for Router Resolve
Scope:
- `docs/index.html`
- `docs/style.css`
- `docs/js/main.js`
- `docs/js/qrScanner.js`

Tasks:
- Add router panel controls:
- router NKN input,
- resolve now,
- refresh NKN client,
- scan QR for router address,
- resolve status indicator.
- Add health/error messages for stale trycloudflare URLs.

Acceptance:
- User can fully configure remote endpoint discovery from UI without opening dev tools.

### WO-05.5 Node Configuration Injection
Scope:
- `docs/js/nodeStore.js`
- `docs/js/asr.js`
- `docs/js/tts.js`
- `docs/js/llm.js`
- `docs/js/mcp.js`
- `docs/js/webScraper.js`
- `docs/js/pointcloud.js`

Tasks:
- Add optional resolver-aware fields per node:
- `service` identity,
- `endpointSource` (`manual` vs `router-resolved`),
- `endpointMode` default policy,
- `lastResolvedAt` and diagnostics.
- Auto-apply resolved base/relay transport settings where service names map cleanly.

Acceptance:
- Nodes can run in manual or router-resolved mode without breaking saved flows.

### WO-05.6 Teleoperation Media Pattern Port
Scope:
- `docs/js/mediaNode.js`
- `docs/js/vision.js`
- optional new node modules for camera/audio router integration

Tasks:
- Add NKN-assisted frame packet retrieval mode for camera streams.
- Add audio WebRTC offer path consumption using resolved endpoints.
- Add tunnel-offline detection and fallback messaging (Cloudflare 530/stale URL semantics).

Acceptance:
- Remote media controls degrade gracefully across cloudflare and NKN modes.

## Phase 6 - Security and Config Governance

### WO-06.1 Config Schema + Runtime Apply Pattern
Scope:
- `service_router/router.py`
- `service_router/router_config.json`

Tasks:
- Introduce typed config schema and runtime apply semantics similar to teleoperation config handlers.
- Support legacy config key promotion where needed.

Acceptance:
- Router can self-heal legacy config and persist normalized config.

### WO-06.2 Auth + Sensitive Field Handling
Scope:
- router API and frontend settings surfaces

Tasks:
- Redact seed secrets in read endpoints.
- Ensure frontend does not leak sensitive fields in logs/exported flows by default.

Acceptance:
- No seed/password fields are exposed in plaintext on public API routes.

## Phase 7 - Validation, CI, and Rollout

### WO-07.1 API Contract Tests
Scope:
- new tests under `service_router/tests/`

Tasks:
- Add tests for `/health`, `/services/snapshot`, `/nkn/info`, `/nkn/resolve` payload schemas.
- Add regression tests for resolver normalization edge cases.

Acceptance:
- CI fails on schema regressions.

### WO-07.2 End-to-End Simulation
Scope:
- test harness scripts under `service_router/tools/`

Tasks:
- Simulate two routers and verify remote resolve round-trip.
- Simulate stale cloudflared URL and fallback path selection.

Acceptance:
- Scripted run demonstrates controlled recovery from tunnel failures.

### WO-07.3 Controlled Migration Plan
Scope:
- rollout docs under `docs/`

Tasks:
- Add feature flags for:
- router control-plane API,
- endpoint auto-apply,
- cloudflared manager.
- Define rollback steps per feature flag.

Acceptance:
- Production migration can be done incrementally with fast rollback.

## 5) Detailed Frontend Change Map (`docs/js`)

### `docs/js/config.js`
- Add router discovery state:
- `routerTargetNknAddress`
- `routerAutoResolve`
- `routerAutoResolveIntervalMs`
- `routerLastResolveStatus`

### `docs/js/net.js`
- Add pending maps for `resolve_tunnels_result` and `service_rpc_result`.
- Add helper methods:
- `nknResolveTunnels()`
- `nknServiceRpc()`
- `normalizeRouterResolvePayload()`
- Preserve existing `nknSend` and `nknStream` behavior.

### `docs/js/main.js`
- Replace prompt-based relay health check flow with router panel actions.
- Subscribe to resolver updates and apply endpoint updates to nodes.
- Surface clear status messages for transport decisions.

### `docs/js/nodeStore.js`
- Extend defaults for nodes that consume service endpoints:
- service key mapping,
- resolver mode,
- diagnostics metadata.

### `docs/js/asr.js`, `tts.js`, `llm.js`, `mcp.js`, `webScraper.js`, `pointcloud.js`
- Support `endpointSource=router` and keep manual mode fallback.
- Consume resolver-provided base URL and relay address.
- Preserve existing per-node overrides for advanced users.

### `docs/js/qrScanner.js`
- Add router QR scan parser path for NKN address extraction.

### New module `docs/js/routerDiscovery.js`
- Own resolve lifecycle and status.
- Decode `snapshot` + `resolved` payloads.
- Emit normalized endpoint updates.

### Optional new module `docs/js/endpointResolver.js`
- Centralize extraction and transport preference logic to avoid duplication across node modules.

## 6) Execution Order Recommendation
1. Phase 0 and Phase 1 first, because frontend and cloudflare logic depend on normalized router API output.
2. Phase 2 next, to stabilize service supervision and eliminate startup/health ambiguity.
3. Phase 3 and Phase 4 in parallel once control-plane schema is locked.
4. Phase 5 after `/services/snapshot` + `/nkn/resolve` payloads are stable.
5. Phase 6 and Phase 7 before full rollout.

## 7) Definition of Done (Program-Level)
- Hydra router exposes stable control-plane APIs and NKN resolve semantics matching teleoperation behavior.
- Hydra service snapshot and resolved endpoints include transport, tunnel, and fallback metadata.
- Hydra frontend can discover and apply service endpoints from router NKN address, including QR onboarding.
- Cloudflared failure modes are explicitly surfaced with stale-url and fallback handling.
- End-to-end tests verify local and remote resolution paths and key failure recoveries.
