#!/usr/bin/env python3
"""Normalization and fallback regression tests for router discovery payloads."""

from __future__ import annotations

from copy import deepcopy
from pathlib import Path
import sys

import pytest

SERVICE_ROUTER_DIR = Path(__file__).resolve().parents[1]
if str(SERVICE_ROUTER_DIR) not in sys.path:
    sys.path.insert(0, str(SERVICE_ROUTER_DIR))

import router  # type: ignore


class _CloudflaredStub:
    def __init__(self, states: dict[str, dict] | None = None):
        self._states = states or {}

    def get_state(self, service_name: str) -> dict:
        return deepcopy(self._states.get(service_name, {}))


def _probe(payload: dict | None = None, ok: bool = False, status: int = 200) -> dict:
    return {
        "json": deepcopy(payload),
        "ok": bool(ok),
        "status": int(status),
        "error": "",
    }


def _fallback(
    selected_transport: str = "local",
    cloudflare_url: str = "",
    upnp_url: str = "",
    nats_url: str = "",
    nkn_url: str = "",
    local_base: str = "http://127.0.0.1:8126",
) -> dict:
    return {
        "selected_transport": selected_transport,
        "order": ["cloudflare", "upnp", "nats", "nkn", "local"],
        "cloudflare": {
            "public_base_url": cloudflare_url,
            "http_endpoint": cloudflare_url,
            "ws_endpoint": cloudflare_url.replace("https://", "wss://") if cloudflare_url else "",
        },
        "upnp": {
            "public_base_url": upnp_url,
            "http_endpoint": upnp_url,
            "ws_endpoint": upnp_url.replace("https://", "wss://") if upnp_url else "",
        },
        "nats": {
            "public_base_url": nats_url,
            "http_endpoint": nats_url,
            "ws_endpoint": nats_url.replace("https://", "wss://") if nats_url else "",
        },
        "nkn": {
            "public_base_url": nkn_url,
            "http_endpoint": nkn_url,
            "ws_endpoint": "",
            "nkn_address": nkn_url,
        },
        "local": {
            "base_url": local_base,
            "http_endpoint": local_base,
            "ws_endpoint": "",
        },
    }


def _make_router(cloudflared_states: dict[str, dict] | None = None) -> router.Router:
    r = router.Router.__new__(router.Router)
    r.targets = {"asr": "http://127.0.0.1:8126"}
    r.cfg = {"targets": {"asr": "http://127.0.0.1:8126"}}
    r.latest_service_status = {}
    r.cloudflared_manager = _CloudflaredStub(cloudflared_states) if cloudflared_states else None
    return r


@pytest.mark.parametrize(
    "case_id,probes,cloud_states,expected_status,expected_transport,expected_tunnel_state",
    [
        (
            "health_ok_promotes_status",
            {"/health": _probe({"ok": True, "device": "cpu"})},
            None,
            "ok",
            "local",
            "inactive",
        ),
        (
            "health_status_passthrough",
            {"/healthz": _probe({"status": "degraded", "ok": False})},
            None,
            "degraded",
            "local",
            "inactive",
        ),
        (
            "router_info_overrides_transport",
            {
                "/router_info": _probe(
                    {
                        "status": "success",
                        "service": "asr",
                        "transport": "nkn",
                        "base_url": "nkn://asr.peer.addr",
                        "http_endpoint": "http://relay.local/asr",
                        "fallback": _fallback(selected_transport="nkn", nkn_url="nkn://asr.peer.addr"),
                    }
                )
            },
            None,
            "success",
            "nkn",
            "inactive",
        ),
        (
            "tunnel_info_active_promotes_cloudflare",
            {
                "/tunnel_info": _probe(
                    {
                        "running": True,
                        "tunnel_url": "https://active.trycloudflare.com",
                        "fallback": _fallback(selected_transport="cloudflare", cloudflare_url="https://active.trycloudflare.com"),
                    }
                )
            },
            None,
            "error",
            "cloudflare",
            "active",
        ),
        (
            "tunnel_info_stale_state",
            {
                "/tunnel_info": _probe(
                    {
                        "running": False,
                        "stale_tunnel_url": "https://stale.trycloudflare.com",
                        "fallback": _fallback(selected_transport="local"),
                    }
                )
            },
            None,
            "error",
            "local",
            "stale",
        ),
        (
            "tunnel_info_error_state",
            {
                "/tunnel_info": _probe(
                    {
                        "running": False,
                        "error": "tunnel down",
                        "fallback": _fallback(selected_transport="local"),
                    }
                )
            },
            None,
            "error",
            "local",
            "error",
        ),
        (
            "cloudflared_runtime_injected",
            {},
            {
                "whisper_asr": {
                    "active_url": "https://runtime.trycloudflare.com",
                    "stale_url": "",
                    "last_error": "",
                    "state": "active",
                    "running": True,
                    "restarts": 1,
                    "rate_limited": False,
                }
            },
            "error",
            "cloudflare",
            "active",
        ),
        (
            "fallback_invalid_transport_autoselects_upnp",
            {
                "/router_info": _probe(
                    {
                        "status": "success",
                        "service": "asr",
                        "transport": "local",
                        "base_url": "http://127.0.0.1:8126",
                        "fallback": _fallback(selected_transport="invalid", upnp_url="https://upnp.public.example"),
                    }
                )
            },
            None,
            "success",
            "upnp",
            "inactive",
        ),
    ],
)
def test_normalize_service_payload_edge_cases(
    case_id: str,
    probes: dict,
    cloud_states: dict | None,
    expected_status: str,
    expected_transport: str,
    expected_tunnel_state: str,
):
    r = _make_router(cloud_states)
    out = r._normalize_service_payload("whisper_asr", "asr", "http://127.0.0.1:8126", probes)

    assert out["status"] == expected_status, case_id
    assert out["transport"] == expected_transport, case_id
    assert out["tunnel"]["state"] == expected_tunnel_state, case_id

    fallback = out.get("fallback")
    assert isinstance(fallback, dict), case_id
    assert fallback.get("selected_transport") in {"cloudflare", "upnp", "nats", "nkn", "local"}, case_id
    assert isinstance(fallback.get("order"), list), case_id


def test_build_resolved_endpoints_prefers_cloudflare_tunnel_when_selected():
    r = _make_router()
    services = {
        "asr": {
            "transport": "local",
            "base_url": "http://127.0.0.1:8126",
            "http_endpoint": "http://127.0.0.1:8126",
            "ws_endpoint": "",
            "local": {"base_url": "http://127.0.0.1:8126"},
            "tunnel": {
                "tunnel_url": "https://a.trycloudflare.com",
                "stale_tunnel_url": "",
                "error": "",
            },
            "fallback": _fallback(selected_transport="cloudflare", cloudflare_url="https://a.trycloudflare.com"),
        }
    }

    resolved = r._build_resolved_endpoints(services)
    entry = resolved["asr"]
    assert entry["transport"] == "cloudflare"
    assert entry["base_url"] == "https://a.trycloudflare.com"
    assert "fallback.selected_transport=cloudflare" in entry["selection_reason"]


def test_build_resolved_endpoints_falls_back_to_upnp_when_cloudflare_missing():
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
                "error": "expired tunnel",
            },
            "fallback": _fallback(selected_transport="cloudflare", upnp_url="https://upnp.public.example"),
        }
    }

    resolved = r._build_resolved_endpoints(services)
    entry = resolved["asr"]
    assert entry["transport"] == "upnp"
    assert entry["base_url"] == "https://upnp.public.example"
    assert entry["selection_reason"] == "auto-selected upnp from fallback"
    assert entry["stale_tunnel_url"] == "https://stale.trycloudflare.com"


def test_build_resolved_endpoints_marks_loopback_local_as_not_public():
    r = _make_router()
    services = {
        "asr": {
            "transport": "local",
            "base_url": "http://127.0.0.1:8126",
            "http_endpoint": "http://127.0.0.1:8126",
            "local": {"base_url": "http://127.0.0.1:8126"},
            "tunnel": {"tunnel_url": "", "stale_tunnel_url": "", "error": ""},
            "fallback": _fallback(selected_transport="local", local_base="http://127.0.0.1:8126"),
        }
    }

    resolved = r._build_resolved_endpoints(services)
    entry = resolved["asr"]
    assert entry["transport"] == "local"
    assert entry["is_loopback"] is True
    assert entry["is_public"] is False
    assert entry["loopback_only"] is True
