#!/usr/bin/env python3
"""Deterministic stale-tunnel fallback simulation for Hydra resolver."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from typing import Any, Dict, List

os.environ.setdefault("HYDRA_ROUTER_SKIP_BOOTSTRAP", "1")

SERVICE_ROUTER_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if SERVICE_ROUTER_DIR not in sys.path:
    sys.path.insert(0, SERVICE_ROUTER_DIR)

import router  # type: ignore


def _make_router() -> "router.Router":
    r = router.Router.__new__(router.Router)
    r.targets = {"asr": "http://127.0.0.1:8126"}
    r.cfg = {"targets": {"asr": "http://127.0.0.1:8126"}}
    r.latest_service_status = {}
    r.cloudflared_manager = None
    return r


def run_simulation() -> Dict[str, Any]:
    checks: List[Dict[str, Any]] = []

    r = _make_router()
    services = {
        "asr": {
            "transport": "local",
            "base_url": "http://127.0.0.1:8126",
            "http_endpoint": "http://127.0.0.1:8126",
            "ws_endpoint": "",
            "local": {"base_url": "http://127.0.0.1:8126"},
            "tunnel": {
                "tunnel_url": "",
                "stale_tunnel_url": "https://stale.trycloudflare.com",
                "error": "cloudflared exited",
            },
            "fallback": {
                "selected_transport": "cloudflare",
                "order": ["cloudflare", "upnp", "nats", "nkn", "local"],
                "cloudflare": {
                    "public_base_url": "",
                    "http_endpoint": "",
                    "ws_endpoint": "",
                    "error": "cloudflared exited",
                },
                "upnp": {
                    "public_base_url": "https://upnp.public.example",
                    "http_endpoint": "https://upnp.public.example",
                    "ws_endpoint": "wss://upnp.public.example",
                },
                "nats": {"public_base_url": "", "http_endpoint": "", "ws_endpoint": ""},
                "nkn": {"public_base_url": "", "http_endpoint": "", "ws_endpoint": ""},
                "local": {
                    "base_url": "http://127.0.0.1:8126",
                    "http_endpoint": "http://127.0.0.1:8126",
                    "ws_endpoint": "",
                },
            },
        }
    }

    resolved = r._build_resolved_endpoints(services)
    asr = resolved.get("asr", {})

    non_cloudflare_ok = asr.get("transport") != "cloudflare"
    checks.append(
        {
            "name": "stale_tunnel_switches_off_cloudflare",
            "ok": non_cloudflare_ok,
            "detail": {
                "transport": asr.get("transport"),
                "selection_reason": asr.get("selection_reason"),
            },
        }
    )

    upnp_fallback_ok = asr.get("transport") == "upnp" and asr.get("base_url") == "https://upnp.public.example"
    checks.append(
        {
            "name": "upnp_fallback_selected",
            "ok": upnp_fallback_ok,
            "detail": {
                "transport": asr.get("transport"),
                "base_url": asr.get("base_url"),
                "stale_tunnel_url": asr.get("stale_tunnel_url"),
            },
        }
    )

    all_ok = all(check["ok"] for check in checks)
    return {
        "ok": all_ok,
        "checks": checks,
        "resolved": resolved,
        "timestamp_ms": int(time.time() * 1000),
    }


def _main() -> int:
    parser = argparse.ArgumentParser(description="Hydra stale tunnel fallback simulation")
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
