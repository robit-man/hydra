# Hydra <-> NoClip Interop Rollout And Rollback Runbook

## Scope
This runbook gates rollout of the Hydra/NoClip interop contract (`hydra_noclip_interop`), endpoint federation, overlay ingress, and multimodal relay behavior.

## Preconditions
- Hydra router is running and control-plane API is reachable.
- NoClip API and Earth client are reachable in staging.
- Contract version parity is confirmed on both sides (`1.0.0` at time of writing).
- Feature flags are explicitly set in router config:
  - `feature_flags.router_control_plane_api = true`
  - `feature_flags.resolver_auto_apply = true`
  - `feature_flags.cloudflared_manager` as environment requires

## Required Validation Commands
1. Smoke test (use live URLs when available):
```bash
python3 service_router/tools/hydra_noclip_interop_smoke.py \
  --router-base http://127.0.0.1:9071 \
  --overlay-ingress-url http://127.0.0.1:8080/api/interop/overlay/ingest \
  --media-echo-url http://127.0.0.1:8080/api/interop/media/echo \
  --json
```
2. Failure drills (live backend auth denial URL if available):
```bash
python3 service_router/tools/hydra_noclip_failure_drill.py \
  --endpoint-update-url http://127.0.0.1:3001/api/p2p/endpoint/update \
  --json
```
3. Health-gate sanity:
```bash
curl -s http://127.0.0.1:9071/health | jq '.rollout_gates'
curl -s http://127.0.0.1:9071/nkn/resolve -X POST -H 'content-type: application/json' -d '{"refresh_local": true}' | jq '.rollout_gates'
```

## Rollout Gates
Rollout proceeds only when all conditions hold:
- Smoke result `ok=true`.
- Failure drills result `ok=true`.
- Router `/health.rollout_gates.ready=true`.
- Router contract version gate is green:
  - `contract_version_expected == contract_version_reported`
  - `contract_version_ok=true`
- Resolver quality gate is green:
  - `resolved_services > 0`
  - `resolve_error_rate_ok=true`
  - `pending_resolves_clear=true`
- NoClip backend `/health` and `/api/da3/health` return `status=ok` with interop gate payload.

## Rollout Procedure
1. Apply config with feature flags in staging.
2. Run smoke + failure drills and archive JSON artifacts.
3. Promote to canary (single router instance + subset of users).
4. Observe:
  - router `/dashboard/data` telemetry
  - backend transport metrics (`fanout`, overlay ingress)
  - frontend harness hooks (`scene.api.interop.getHarnessState()`)
5. Promote globally only after stable canary window and no gate regressions.

## Rollback Procedure
1. Disable interop entry points first (safe stop):
  - set `feature_flags.resolver_auto_apply=false`
  - disable frontend interop bridge ingress toggle (if exposed in client config)
2. Keep control-plane API enabled for recovery observability.
3. Flush pending endpoint updates on backend (prevent stale cross-writes).
4. Restart router and backend processes.
5. Validate rollback:
  - `/health.rollout_gates.ready=false` is acceptable during rollback
  - local service paths still healthy
  - no crash loops in watchdog, bridge, or WS transport
6. Remove stale endpoint/session records if needed, then re-run smoke in simulated mode:
```bash
python3 service_router/tools/hydra_noclip_interop_smoke.py --simulate --json
python3 service_router/tools/hydra_noclip_failure_drill.py --simulate --json
```

## Failure Modes And Recovery Mapping
- NATS unavailable:
  - Expected: resolver demotes to non-NATS transport and continues serving.
- Stale Cloudflare URL:
  - Expected: stale candidate rejected and alternate transport selected.
- Invalid shared-key metadata:
  - Expected: metadata rejected, previous trust state retained.
- Backend auth denial:
  - Expected: endpoint-update attempt returns 401/403, no mutation committed.

## Artifact Retention
- Store smoke and failure artifact JSON per rollout window.
- Keep at least:
  - last successful staging run
  - last successful canary run
  - most recent rollback run (if any)
