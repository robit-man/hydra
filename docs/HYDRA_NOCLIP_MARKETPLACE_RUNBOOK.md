# Hydra + NoClip Marketplace Security Runbook

## Purpose
Operational guide for rollout, freeze, fraud response, and rollback of marketplace publication and billing flows shared between Hydra and NoClip.

## Critical Controls
- `publication_enabled`: allows/disallows new marketplace offer publication.
- `billing_enabled`: allows/disallows quote settlement and ledger debit execution.
- `fraud_controls_ready`: credit guard/rate-limit/double-spend protection readiness.
- `ticket_replay_defense_ok`: replay rejection readiness for signed access tickets.

## Readiness Checklist
1. `GET /marketplace/gates` returns `status=ok` and `rollout_gates.ready=true`.
2. `GET /health` returns:
   - `interop.gates.marketplace_rollout_ready=true`
   - `interop.gates.marketplace_fraud_controls_ready=true`
   - `interop.gates.marketplace_ticket_replay_defense_ok=true`
3. `python3 service_router/tools/hydra_noclip_rollout_gate.py --require-marketplace-gate ...` passes.

## Hydra Catalog Federation Controls
- `GET /marketplace/sync` (Hydra router) exposes sync config/state (`in_flight`, `next_due_ts_ms`, last errors/results).
- `POST /marketplace/catalog/publish` (Hydra router) triggers manual publish to configured or request-specified targets.
- `python3 service_router/tools/hydra_catalog_publish.py --target-url <url>` triggers a manual publish from CLI.

Example dry-run:
```bash
python3 service_router/tools/hydra_catalog_publish.py \
  --router-base http://127.0.0.1:9071 \
  --target-url http://127.0.0.1:3001/api/interop/marketplace/catalog/ingest \
  --dry-run \
  --json
```

## NoClip Catalog Ingest API
- Ingest endpoint: `POST /api/interop/marketplace/catalog/ingest`
  - Auth accepted:
    - `x-admin-key: $ADMIN_UNLOCK_KEY`, or
    - `x-hydra-ingest-token: $HYDRA_MARKETPLACE_INGEST_TOKEN`, or
    - `authorization: Bearer $HYDRA_MARKETPLACE_INGEST_TOKEN`
- Provider directory endpoint: `GET /api/interop/marketplace/catalog/providers`

Ingest check:
```bash
curl -sS -X POST http://127.0.0.1:3001/api/interop/marketplace/catalog/ingest \
  -H "content-type: application/json" \
  -H "x-hydra-ingest-token: $HYDRA_MARKETPLACE_INGEST_TOKEN" \
  -d @/tmp/hydra_catalog_event.json | jq '.status,.catalog.providerFingerprint,.catalog.lastIngestedAt'
```

Provider list check:
```bash
curl -sS "http://127.0.0.1:3001/api/interop/marketplace/catalog/providers?sourceNetwork=hydra&activeOnly=true" \
  | jq '.total,.diagnostics,.catalogs[0].providerLabel'
```

Hydra frontend check:
1. Open Hydra dashboard and confirm marketplace card shows directory status text:
   - `Directory: <count> providers • fresh <n> • stale <n> • peers <m>`
2. Click `Dir Refresh` and confirm the status text and badge update immediately.
3. Open peer list modal on `Hydra` tab and confirm directory-imported router peers appear with source metadata.
4. Open peer list modal on `NoClip` tab and click `Bridge` on a peer:
   - if no `NoClipBridge` node exists, confirm one is created automatically,
   - confirm target peer is assigned and bridge node log shows target set message.
5. Open Hydra with invite query params (example `?noclip=noclip.<pub>&object=<uuid>&session=<sid>`):
   - confirm bridge target is auto-applied without manual settings,
   - confirm object/session context is reflected in bridge node target config,
   - confirm invite query params are removed from URL after processing.
6. Open any `NoClipBridge` node and click the `📱` invite control:
   - confirm Smart Invite modal opens for that bridge node,
   - generate QR and verify URL includes `noclip`, `bridgeNodeId`, and `autoSync`,
   - if bridge node has target context, verify URL includes context keys (`objectUuid/sessionId/...`).
7. Validate URL invite auto-sync retry lifecycle:
   - open Hydra with `?noclip=<pub>&autoSync=true` for an unreachable peer and confirm retries back off then stop at cap,
   - open with a reachable peer and confirm pending invite retry state clears after sync success.

## Kill-Switch Operations
### Freeze publication only
```bash
curl -sS -X PATCH http://127.0.0.1:3001/marketplace/admin/gates \
  -H "content-type: application/json" \
  -H "x-admin-key: $ADMIN_UNLOCK_KEY" \
  -d '{"publicationEnabled":false,"reason":"maintenance window"}'
```

### Freeze billing only
```bash
curl -sS -X PATCH http://127.0.0.1:3001/marketplace/admin/gates \
  -H "content-type: application/json" \
  -H "x-admin-key: $ADMIN_UNLOCK_KEY" \
  -d '{"billingEnabled":false,"reason":"fraud investigation"}'
```

### Re-enable both
```bash
curl -sS -X PATCH http://127.0.0.1:3001/marketplace/admin/gates \
  -H "content-type: application/json" \
  -H "x-admin-key: $ADMIN_UNLOCK_KEY" \
  -d '{"publicationEnabled":true,"billingEnabled":true,"reason":"incident closed"}'
```

## Incident Response Patterns
### Rapid-fire abuse
1. Freeze billing.
2. Inspect `/health.marketplace.counters.rate_limited`, `.fraud_signals`, `.abuse_bursts`.
3. Run `hydra_noclip_failure_drill.py` and confirm `fraudulent_rapid_fire_invocation_throttled`.

### Ticket replay signals
1. Keep billing frozen until replay trend stabilizes.
2. Validate `/health.marketplace.counters.replay_rejected` increments while no ledger mismatch appears.
3. Run `hydra_noclip_failure_drill.py` and confirm `ticket_replay_rejected`.

### Suspected double-spend
1. Freeze billing.
2. Validate `GET /marketplace/gates` -> `credit_ledger_invariants_ok=true`.
3. Run `hydra_noclip_failure_drill.py` and confirm `credit_double_spend_guard`.

## Rollback Procedure
1. Freeze publication and billing.
2. Keep diagnostics online (`/health`, `/marketplace/gates`) while recovery runs.
3. Replay settlement journal if needed; resolve any invariant mismatch before reopen.
4. Re-run:
   - `hydra_noclip_rollout_gate.py --simulate --json`
   - `hydra_noclip_rollout_gate.py --require-marketplace-gate ... --json`
5. Re-enable publication first, then billing after stable verification.

## Required Artifact Capture
- Consolidated rollout artifact JSON.
- Smoke artifact JSON.
- Failure-drill artifact JSON.
- `trace_id`, `failed_required_checks`, and rollback actions taken.
