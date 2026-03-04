#!/usr/bin/env python3
"""Deterministic end-to-end simulation for Hydra two-router resolve flow."""

from __future__ import annotations

import argparse
import copy
import json
import time
from dataclasses import dataclass
from typing import Any, Dict, List


@dataclass
class SimRouter:
    name: str
    address: str
    snapshot: Dict[str, Any]

    def resolve_tunnels(self, request_id: str, from_address: str) -> Dict[str, Any]:
        return {
            "event": "resolve_tunnels_result",
            "request_id": request_id,
            "source_address": self.address,
            "target_address": from_address,
            "timestamp_ms": int(time.time() * 1000),
            "snapshot": copy.deepcopy(self.snapshot),
        }


class SimTransport:
    def __init__(self) -> None:
        self._routers: Dict[str, SimRouter] = {}

    def register(self, router: SimRouter) -> None:
        self._routers[router.address] = router

    def request_resolve(self, source_address: str, target_address: str, request_id: str) -> Dict[str, Any]:
        target = self._routers.get(target_address)
        if target is None:
            raise RuntimeError(f"target router not found: {target_address}")
        return target.resolve_tunnels(request_id=request_id, from_address=source_address)


def _sample_snapshot() -> Dict[str, Any]:
    return {
        "ts_ms": int(time.time() * 1000),
        "services": {
            "asr": {
                "status": "ok",
                "transport": "local",
                "base_url": "http://127.0.0.1:8126",
                "fallback": {
                    "selected_transport": "local",
                    "order": ["cloudflare", "upnp", "nats", "nkn", "local"],
                    "cloudflare": {"public_base_url": "", "http_endpoint": "", "ws_endpoint": ""},
                    "upnp": {"public_base_url": "", "http_endpoint": "", "ws_endpoint": ""},
                    "nats": {"public_base_url": "", "http_endpoint": "", "ws_endpoint": ""},
                    "nkn": {"public_base_url": "", "http_endpoint": "", "ws_endpoint": ""},
                    "local": {
                        "base_url": "http://127.0.0.1:8126",
                        "http_endpoint": "http://127.0.0.1:8126",
                        "ws_endpoint": "",
                    },
                },
            }
        },
        "resolved": {
            "asr": {
                "service": "asr",
                "transport": "local",
                "selected_transport": "local",
                "base_url": "http://127.0.0.1:8126",
                "http_endpoint": "http://127.0.0.1:8126",
                "ws_endpoint": "",
                "selection_reason": "fallback.selected_transport=local",
                "is_public": False,
                "is_loopback": True,
                "loopback_only": True,
            }
        },
        "stale": False,
    }


def run_simulation() -> Dict[str, Any]:
    checks: List[Dict[str, Any]] = []

    transport = SimTransport()
    router_a = SimRouter(name="router-a", address="router.a.sim", snapshot=_sample_snapshot())
    router_b = SimRouter(name="router-b", address="router.b.sim", snapshot=_sample_snapshot())
    transport.register(router_a)
    transport.register(router_b)

    request_id = "sim-resolve-0001"
    reply = transport.request_resolve(source_address=router_a.address, target_address=router_b.address, request_id=request_id)

    round_trip_ok = (
        reply.get("event") == "resolve_tunnels_result"
        and reply.get("request_id") == request_id
        and reply.get("source_address") == router_b.address
        and isinstance(reply.get("snapshot"), dict)
        and isinstance((reply.get("snapshot") or {}).get("resolved"), dict)
        and "asr" in (reply.get("snapshot") or {}).get("resolved", {})
    )
    checks.append(
        {
            "name": "remote_resolve_round_trip",
            "ok": round_trip_ok,
            "detail": {
                "request_id": request_id,
                "source": router_a.address,
                "target": router_b.address,
                "reply_source": reply.get("source_address"),
                "resolved_keys": sorted(((reply.get("snapshot") or {}).get("resolved") or {}).keys()),
            },
        }
    )

    all_ok = all(check["ok"] for check in checks)
    return {
        "ok": all_ok,
        "checks": checks,
        "routers": {
            "router_a": {"name": router_a.name, "address": router_a.address},
            "router_b": {"name": router_b.name, "address": router_b.address},
        },
        "timestamp_ms": int(time.time() * 1000),
    }


def _main() -> int:
    parser = argparse.ArgumentParser(description="Hydra two-router resolve simulation")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON")
    args = parser.parse_args()

    result = run_simulation()
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        for check in result["checks"]:
            state = "PASS" if check["ok"] else "FAIL"
            print(f"[{state}] {check['name']}")
        print(f"overall={result['ok']}")

    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(_main())
