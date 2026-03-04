# Hydra + NoClip Interop API Contract

## Purpose
This document defines the versioned interop envelope and endpoint-state schema used between:
- Hydra router control-plane payloads
- Hydra frontend resolver/bridge payloads
- NoClip endpoint/discovery consumers

Current contract:
- `name`: `hydra_noclip_interop`
- `version`: `1.0.0`

## Envelope (Canonical)
All interop messages should use:

```json
{
  "event": "string",
  "interop_contract": {
    "name": "hydra_noclip_interop",
    "version": "1.0.0"
  },
  "interop_contract_version": "1.0.0",
  "timestamp_ms": 0,
  "source_address": "string",
  "target_address": "string",
  "payload": {}
}
```

Notes:
- `interop_contract` is preferred.
- `interop_contract_version` is retained for compact/legacy compatibility.

## Endpoint State (Canonical Resolved Entry)

```json
{
  "service": "whisper_asr",
  "interop_contract_version": "1.0.0",
  "selected_transport": "cloudflare",
  "transport": "cloudflare",
  "selection_reason": "auto-selected cloudflare from tunnel_url",
  "base_url": "https://example.trycloudflare.com",
  "http_endpoint": "https://example.trycloudflare.com",
  "ws_endpoint": "wss://example.trycloudflare.com",
  "remote_routable": true,
  "loopback_only": false,
  "is_public": true,
  "candidates": {
    "cloudflare": "https://example.trycloudflare.com",
    "upnp": "",
    "nats": "",
    "nkn": "",
    "local": "http://127.0.0.1:8126"
  }
}
```

## Transport Enum
Allowed transport values:
- `cloudflare`
- `upnp`
- `nats`
- `nkn`
- `local`

Unknown transport values must be treated as invalid and coerced to `local` only by explicit compatibility logic.

## Compatibility Rules
1. Accept messages that only include `interop_contract_version`.
2. Prefer `interop_contract.version` when both are present.
3. Preserve unrecognized fields in pass-through payloads.
4. Reject malformed required fields with deterministic error codes.

## Error Codes
- `INVALID_ENVELOPE`
- `INVALID_TRANSPORT`
- `INVALID_TARGET`
- `UNSUPPORTED_CONTRACT_VERSION`
- `AUTH_REQUIRED`
- `FORBIDDEN`

## Health/Discovery Surfaces Required
- Hydra router:
  - `/health`
  - `/services/snapshot`
  - `/nkn/info`
  - `/nkn/resolve`
- Any interop consumer should validate `interop_contract.version` from these payloads before applying state.
