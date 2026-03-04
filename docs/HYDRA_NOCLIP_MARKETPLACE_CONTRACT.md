# Hydra + NoClip Marketplace Interop Contract (WO-10.1)

## Contract Identity

- `name`: `hydra_noclip_interop`
- `version`: `1.0.0`
- `compat_min_version`: `1.0.0`
- `namespace`: `hydra.noclip.marketplace.v1`
- `schema`: `hydra_noclip_marketplace_contract_v1`

## Canonical Envelope

All marketplace/interop payloads exchanged between Hydra Router, Hydra Frontend, NoClip Backend, and NoClip Earth must include:

```json
{
  "type": "market-service-catalog",
  "event": "market.service.catalog",
  "messageId": "msg-123",
  "ts": 1730745600000,
  "source_address": "hydra.abcd1234",
  "target_address": "noclip.efgh5678",
  "interop_contract": {
    "name": "hydra_noclip_interop",
    "version": "1.0.0",
    "compat_min_version": "1.0.0",
    "namespace": "hydra.noclip.marketplace.v1",
    "schema": "hydra_noclip_marketplace_contract_v1"
  },
  "interop_contract_version": "1.0.0",
  "interop_contract_compat_min_version": "1.0.0",
  "interop_contract_namespace": "hydra.noclip.marketplace.v1",
  "payload": {}
}
```

Required validation rules:

1. Envelope must be an object.
2. At least one of `type` or `event` must be present.
3. Interop contract must be name-compatible and semver-compatible:
   - incoming `version >= expected.compat_min_version`
   - expected `version >= incoming.compat_min_version`
4. Unknown fields are allowed and must be preserved for pass-through compatibility.

Deterministic error codes:

- `INVALID_ENVELOPE`
- `MISSING_REQUIRED_FIELDS`
- `UNSUPPORTED_CONTRACT_VERSION`
- `INVALID_TARGET`
- `MARKET_NOT_READY`

## Marketplace Event Types

- `market.service.catalog`
- `market.service.status`
- `market.quote.request`
- `market.quote.result`
- `market.credit.balance`
- `market.access.ticket.issue`
- `market.access.ticket.verify`
- `market.usage.record`
- `interop.contract.get`

## Normative Schema Bundle (JSON)

```json
{
  "$id": "hydra_noclip_marketplace_contract_v1",
  "contract": {
    "name": "hydra_noclip_interop",
    "version": "1.0.0",
    "compat_min_version": "1.0.0",
    "namespace": "hydra.noclip.marketplace.v1",
    "schema": "hydra_noclip_marketplace_contract_v1"
  },
  "envelope": {
    "required_any": ["type|event"],
    "required": ["interop_contract", "interop_contract_version", "interop_contract_compat_min_version"],
    "optional": ["messageId", "ts", "source_address", "target_address", "payload", "error_code", "detail"]
  },
  "event_types": {
    "interop.contract.get": { "request": {}, "response": { "status": "ok|string" } },
    "market.service.catalog": { "request": {}, "response": { "services": "array" } },
    "market.service.status": { "request": { "service_ids": "array?" }, "response": { "services": "array" } },
    "market.quote.request": { "request": { "service_id": "string" }, "response": { "quote_id": "string?" } },
    "market.quote.result": { "request": { "quote_id": "string" }, "response": { "price": "number?" } },
    "market.credit.balance": { "request": { "account_id": "string?" }, "response": { "balance": "number?" } },
    "market.access.ticket.issue": { "request": { "service_id": "string" }, "response": { "ticket_id": "string?" } },
    "market.access.ticket.verify": { "request": { "ticket_id": "string" }, "response": { "valid": "boolean?" } },
    "market.usage.record": { "request": { "service_id": "string" }, "response": { "accepted": "boolean?" } }
  }
}
```

## Example: Hydra Router `/health`

```json
{
  "status": "ok",
  "service": "hydra_router",
  "interop_contract": {
    "name": "hydra_noclip_interop",
    "version": "1.0.0",
    "compat_min_version": "1.0.0",
    "namespace": "hydra.noclip.marketplace.v1",
    "schema": "hydra_noclip_marketplace_contract_v1"
  },
  "rollout_gates": {
    "contract_ok": true
  }
}
```

## Example: NoClip Backend `interop.contract.get`

```json
{
  "status": "ok",
  "interop_contract": {
    "name": "hydra_noclip_interop",
    "version": "1.0.0",
    "compat_min_version": "1.0.0",
    "namespace": "hydra.noclip.marketplace.v1",
    "schema": "hydra_noclip_marketplace_contract_v1"
  },
  "interop_contract_version": "1.0.0",
  "interop_contract_compat_min_version": "1.0.0",
  "interop_contract_namespace": "hydra.noclip.marketplace.v1"
}
```

## Example: Rejected Envelope

```json
{
  "event": "interop.bridge.ack",
  "status": "error",
  "error_code": "UNSUPPORTED_CONTRACT_VERSION",
  "detail": "interop contract mismatch"
}
```
