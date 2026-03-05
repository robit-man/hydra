# Hydra + Teleoperation Unification Workorders

This folder breaks the phased plan into executable, individually completable workorders.

## Session Signal

- `IMPLEMENTATION_SIGNAL_LOG.md`
  - Compact running change log for cross-session context continuity.

## Phase Index

- `phase-00`
  - `WO-00.1-service-contract-inventory.md`
  - `WO-00.2-endpoint-probe-harness.md`
- `phase-01`
  - `WO-01.1-router-http-api-surface.md`
  - `WO-01.2-snapshot-resolved-engine.md`
  - `WO-01.3-nkn-resolve-tracking.md`
- `phase-02`
  - `WO-02.1-watchdog-state-machine.md`
  - `WO-02.2-port-reclaim-ownership-safety.md`
  - `WO-02.3-single-instance-lock-desired-state.md`
- `phase-03`
  - `WO-03.1-cloudflared-manager-utility.md`
  - `WO-03.2-service-discovery-payload-schema.md`
  - `WO-03.3-fallback-metadata-surfaces.md`
- `phase-04`
  - `WO-04.1-nkn-telemetry-dashboard.md`
  - `WO-04.2-service-rpc-over-nkn.md`
- `phase-05`
  - `WO-05.1-router-discovery-client-module.md`
  - `WO-05.2-net-resolve-rpc-helpers.md`
  - `WO-05.3-endpoint-normalization-selection.md`
  - `WO-05.4-frontend-router-resolve-controls.md`
  - `WO-05.5-node-configuration-injection.md`
  - `WO-05.6-media-pattern-port.md`
- `phase-06`
  - `WO-06.1-config-schema-runtime-apply.md`
  - `WO-06.2-auth-sensitive-field-handling.md`
- `phase-07`
  - `WO-07.1-api-contract-tests.md`
  - `WO-07.2-end-to-end-simulation.md`
  - `WO-07.3-controlled-migration-plan.md`
- `phase-08`
  - `WO-08.1-peer-identity-contract-and-prefix-canonicalization.md`
  - `WO-08.2-nats-discovery-unification-and-bridge.md`
  - `WO-08.3-manual-peer-addition-and-qr-ingress.md`
  - `WO-08.4-noclip-backend-endpoint-registry-and-shared-key-scheme.md`
  - `WO-08.5-common-interop-api-contract-and-envelope-versioning.md`
  - `WO-08.6-hydra-overlay-data-plane-and-multimodal-payloads.md`
  - `WO-08.7-noclip-overlay-ingress-and-asset-hydration-runtime.md`
  - `WO-08.8-endpoint-state-federation-and-discovery-interop.md`
  - `WO-08.9-cross-frontend-ui-and-style-unification.md`
  - `WO-08.10-end-to-end-validation-failure-drills-and-rollout-gates.md`
- `phase-09`
  - `WO-09.1-rollout-gate-orchestrator-and-artifact-unification.md`
- `phase-10`
  - `WO-10.1-marketplace-contract-and-interop-namespace.md`
  - `WO-10.2-hydra-router-service-publication-and-catalog.md`
  - `WO-10.3-noclip-provider-registry-and-offer-lifecycle.md`
  - `WO-10.4-multi-transport-discovery-and-peer-ingress-unification.md`
  - `WO-10.5-access-tickets-shared-keys-and-audience-gating.md`
  - `WO-10.6-credit-ledger-and-daily-entitlement-engine.md`
  - `WO-10.7-usage-metering-pricing-and-settlement-events.md`
  - `WO-10.8-runtime-enforcement-for-router-rpc-and-overlay-ingress.md`
  - `WO-10.9-cross-frontend-marketplace-ui-and-badge-unification.md`
  - `WO-10.10-rollout-security-audit-and-failure-recovery.md`
- `phase-11`
  - `WO-11.1-marketplace-catalog-federation-sync.md`
  - `WO-11.2-noclip-catalog-ingest-provider-directory.md`
  - `WO-11.3-hydra-frontend-federated-provider-directory.md`
  - `WO-11.4-marketplace-directory-peer-ingress.md`
  - `WO-11.5-peer-modal-bridge-target-orchestration.md`
  - `WO-11.6-url-invite-bridge-autotarget-and-context-hydration.md`
  - `WO-11.7-smart-invite-egress-and-ui-hook-alignment.md`
  - `WO-11.8-url-invite-autosync-retry-and-lifecycle-hardening.md`
- `phase-12`
  - `WO-12.1-noclip-transport-marketplace-handler-activation.md`
  - `WO-12.2-noclip-backend-nats-marketplace-gossip.md`
  - `WO-12.3-hydra-router-nats-marketplace-sync.md`
  - `WO-12.4-hydra-marketplace-discovery-event-pipeline.md`
  - `WO-12.5-hydra-noclip-bridge-market-broadcast.md`
  - `WO-12.6-hydra-provider-provisioning-and-service-config-ui.md`
  - `WO-12.7-noclip-provider-management-frontend.md`
  - `WO-12.8-cross-platform-catalog-reconciliation-and-health.md`
  - `WO-12.9-marketplace-remediation-e2e-gates-and-rollout.md`

## Completion Rule

A workorder is complete only when all of the following are true:

1. The implementation checklist is finished.
2. Every success metric is demonstrably met (commands/tests/log evidence).
3. Failure-mode preventions are implemented, not just documented.
4. Verification commands run cleanly in this repo.
