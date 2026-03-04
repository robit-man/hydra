# Hydra Teleoperation Unification Rollout Plan

## Owner
- Owner: Hydra service_router maintainers
- Scope: staged production rollout of router control-plane, resolver auto-apply, and cloudflared manager
- Primary config: `service_router/router_config.json` (`feature_flags`, `api`, `cloudflared`)

## Feature Flag Matrix
| Feature | Config key | Default | Dependency | Rollback switch |
| --- | --- | --- | --- | --- |
| Router control-plane API | `feature_flags.router_control_plane_api` | `true` | `api.enable=true` | set flag `false` |
| Resolver auto-apply (frontend) | `CFG.featureFlags.resolverAutoApply` | `true` | router `/nkn/resolve` reachable | set flag `false` |
| Cloudflared manager | `feature_flags.cloudflared_manager` | `false` | `cloudflared.enable=true` + cloudflared binary | set flag `false` |

## Staged Rollout

### Stage 0 - Baseline Freeze
- Entry: current production healthy for 24h.
- Actions:
  - Capture baseline from `/health`, `/nkn/info`, `/services/snapshot`, `/dashboard/data`.
  - Record resolved transport distribution (`cloudflare`, `upnp`, `nats`, `nkn`, `local`).
- Exit criteria:
  - Baseline metrics archived.
  - Rollback operator on-call assigned.

### Stage 1 - Control-plane API Only
- Config:
  - `feature_flags.router_control_plane_api=true`
  - `api.enable=true`
  - `feature_flags.cloudflared_manager=false`
- Actions:
  - Deploy router with API enabled only.
  - Verify `/api` exposes `feature_flags` and route map.
- Exit criteria:
  - 0 API bind/start failures over 30 minutes.
  - `/health` and `/nkn/info` schema checks passing.

### Stage 2 - Resolver Auto-Apply (Frontend)
- Config/UI:
  - `CFG.featureFlags.resolverAutoApply=true`
  - `CFG.routerAutoResolve=true` only for pilot operators.
- Actions:
  - Enable on 10% pilot clients.
  - Track stale resolve discards and wrong-endpoint incidents.
- Exit criteria:
  - No increase in endpoint resolution errors vs Stage 1 baseline.
  - Manual fallback still usable when auto-resolve fails.

### Stage 3 - Cloudflared Manager Pilot
- Config:
  - `feature_flags.cloudflared_manager=true`
  - `cloudflared.enable=true`
- Actions:
  - Enable for one non-critical service first.
  - Monitor stale tunnel transitions and fallback transport selection.
- Exit criteria:
  - Stale tunnel transitions recover to non-cloudflare fallback within SLA.
  - No restart storm (`restarts` and `rate_limited` stable).

### Stage 4 - Full Enablement
- Config:
  - Keep Stage 1-3 flags enabled where validated.
- Actions:
  - Expand to all targeted services.
  - Keep rollback operator active for first 24h.
- Exit criteria:
  - 24h stable operation with no unresolved Sev-1/Sev-2 incidents.

## Rollback Runbook

### Immediate Safe Rollback (all major features)
```bash
cp service_router/router_config.json service_router/router_config.json.bak
jq '.feature_flags.router_control_plane_api=false
  | .feature_flags.cloudflared_manager=false
  | .cloudflared.enable=false
  | .api.enable=false' \
  service_router/router_config.json > /tmp/router_config.rollback.json
mv /tmp/router_config.rollback.json service_router/router_config.json
python3 service_router/router.py --config service_router/router_config.json
```

### Partial Rollback - Cloudflared only
```bash
jq '.feature_flags.cloudflared_manager=false | .cloudflared.enable=false' \
  service_router/router_config.json > /tmp/router_config.no_cloudflared.json
mv /tmp/router_config.no_cloudflared.json service_router/router_config.json
python3 service_router/router.py --config service_router/router_config.json
```

### Frontend Resolver Auto-Apply rollback
- Set `CFG.featureFlags.resolverAutoApply=false` in client config/local storage.
- Keep resolve requests available for manual/operator-triggered use.

## Go/No-Go Operational Checks
- `/api`:
  - `feature_flags` match intended stage.
  - required routes present.
- `/health`:
  - `status=ok`, `pending_resolves` bounded, telemetry counters moving.
- `/services/snapshot`:
  - `services` and `resolved` present; no schema regressions.
- `/dashboard/data`:
  - no abnormal surge in `resolve_fail_out` or `rpc_fail_out`.
- Tunnel/fallback:
  - when cloudflared stale/error occurs, selected transport is non-cloudflare and routable.

## Acceptance Gates
- Every stage must pass required checks from `docs/HYDRA_SERVICE_CONTRACT_MATRIX.json`.
- Rollback commands executed successfully in staging before production promotion.
- Post-rollout incident log reviewed and signed off by owner.
