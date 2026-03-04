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
1. Unified rollout gate (recommended):
```bash
python3 service_router/tools/hydra_noclip_rollout_gate.py \
  --router-base http://127.0.0.1:9071 \
  --overlay-ingress-url http://127.0.0.1:8080/api/interop/overlay/ingest \
  --media-echo-url http://127.0.0.1:8080/api/interop/media/echo \
  --marketplace-gate-url http://127.0.0.1:3001/marketplace/gates \
  --endpoint-update-url http://127.0.0.1:3001/api/p2p/endpoint/update \
  --backend-health-url http://127.0.0.1:3001/health \
  --da3-health-url http://127.0.0.1:3001/api/da3/health \
  --require-backend-health \
  --require-marketplace-gate \
  --json
```
2. Smoke test only (diagnostics):
```bash
python3 service_router/tools/hydra_noclip_interop_smoke.py \
  --router-base http://127.0.0.1:9071 \
  --overlay-ingress-url http://127.0.0.1:8080/api/interop/overlay/ingest \
  --media-echo-url http://127.0.0.1:8080/api/interop/media/echo \
  --json
```
3. Failure drills only (diagnostics):
```bash
python3 service_router/tools/hydra_noclip_failure_drill.py \
  --endpoint-update-url http://127.0.0.1:3001/api/p2p/endpoint/update \
  --marketplace-gate-url http://127.0.0.1:3001/marketplace/gates \
  --json
```
4. Health-gate sanity:
```bash
curl -s http://127.0.0.1:9071/health | jq '.rollout_gates'
curl -s http://127.0.0.1:9071/nkn/resolve -X POST -H 'content-type: application/json' -d '{"refresh_local": true}' | jq '.rollout_gates'
curl -s http://127.0.0.1:9071/marketplace/sync | jq '.state'
curl -s http://127.0.0.1:3001/health | jq '.marketplace.external_catalogs,.interop.gates.marketplace_external_catalog_fresh'
curl -s "http://127.0.0.1:3001/api/interop/marketplace/catalog/providers?sourceNetwork=hydra&activeOnly=true" | jq '.total,.diagnostics'
```
5. Frontend peer-ingress sanity:
- In Hydra dashboard, click `Dir Refresh` and confirm marketplace directory text includes `peers <m>`.
- Open peer list (`Hydra` network) and confirm imported marketplace routers appear without manual entry.
- Open peer list (`NoClip` network), click `Bridge` on a peer, and confirm a `NoClipBridge` node is configured (or created) with that target.
- Open Hydra using invite URL params (`noclip`, optional `object/session`) and confirm bridge target/context are auto-applied with URL params cleared after ingest.

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
- Marketplace rollout gates are green:
  - `contract_version_ok=true`
  - `provider_catalog_integrity_ok=true`
  - `quote_settlement_integrity_ok=true`
  - `credit_ledger_invariants_ok=true`
  - `ticket_replay_defense_ok=true`
  - `fraud_controls_ready=true`
  - `publication_enabled=true`
  - `billing_enabled=true`

## Rollout Procedure
1. Apply config with feature flags in staging.
2. Run unified rollout gate command and archive consolidated + sub-artifacts.
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
python3 service_router/tools/hydra_noclip_rollout_gate.py --simulate --json
```

## Phase-10 Security Gates
- Keep marketplace kill switches independently addressable:
  - publication switch blocks new offer publication but keeps read-only catalog/health online.
  - billing switch blocks quote settlement and debit execution while preserving diagnostics.
- Require fraud counters and replay defense to be active before rollout:
  - backend `/health.interop.gates.marketplace_fraud_controls_ready=true`
  - backend `/health.interop.gates.marketplace_ticket_replay_defense_ok=true`
- Always archive audit bundle data from rollout artifacts:
  - `trace_id`
  - `failed_required_checks`
  - `raw_errors`
  - `rollback_instructions`

## Operator Runbooks
- Core rollout + rollback flow remains in this document.
- Detailed marketplace-specific incident actions are in:
  - `docs/HYDRA_NOCLIP_MARKETPLACE_RUNBOOK.md`

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
- Store consolidated rollout-gate artifacts and linked smoke/drill artifacts per rollout window.
- Keep at least:
  - last successful staging run
  - last successful canary run
  - most recent rollback run (if any)
