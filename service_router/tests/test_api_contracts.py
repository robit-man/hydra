#!/usr/bin/env python3
"""Contract tests for Hydra router control-plane API surfaces."""

from __future__ import annotations

import json
import threading
import time
from collections import deque
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
import sys

import pytest

SERVICE_ROUTER_DIR = Path(__file__).resolve().parents[1]
REPO_ROOT = SERVICE_ROUTER_DIR.parent
if str(SERVICE_ROUTER_DIR) not in sys.path:
    sys.path.insert(0, str(SERVICE_ROUTER_DIR))

import router  # type: ignore

CONTRACT_MATRIX_PATH = REPO_ROOT / "docs" / "HYDRA_SERVICE_CONTRACT_MATRIX.json"
CONTRACT_MATRIX = json.loads(CONTRACT_MATRIX_PATH.read_text())
HEALTH_REQUIRED = set(CONTRACT_MATRIX["canonical_schemas"]["health"]["required"])


def _sample_snapshot() -> dict:
    return {
        "ts_ms": 1730000000000,
        "services": {
            "asr": {
                "status": "ok",
                "service": "asr",
                "security": {
                    "password": "super-secret-password",
                    "require_auth": True,
                },
                "fallback": {
                    "selected_transport": "local",
                    "order": ["cloudflare", "upnp", "nats", "nkn", "local"],
                    "cloudflare": {},
                    "upnp": {},
                    "nats": {},
                    "nkn": {},
                    "local": {"base_url": "http://127.0.0.1:8126"},
                },
            }
        },
        "resolved": {
            "asr": {
                "service": "asr",
                "transport": "local",
                "base_url": "http://127.0.0.1:8126",
                "http_endpoint": "http://127.0.0.1:8126",
                "seed_hex": "a" * 64,
                "security": {
                    "api_key": "example-api-key",
                },
            }
        },
        "stale": False,
    }


def _make_router_for_api(snapshot: dict | None = None) -> router.Router:
    r = router.Router.__new__(router.Router)
    r.startup_time = time.time() - 42
    r.request_counter = {"value": 0}
    r.pending_resolves = {}
    r.pending_resolves_lock = threading.Lock()
    r.telemetry_lock = threading.Lock()
    r.telemetry_state = {
        "inbound_messages": 3,
        "outbound_messages": 5,
        "inbound_bytes": 123,
        "outbound_bytes": 456,
        "resolve_requests_in": 1,
        "resolve_requests_out": 2,
        "resolve_success_out": 2,
        "resolve_fail_out": 0,
        "rpc_requests_in": 0,
        "rpc_requests_out": 0,
        "rpc_success_out": 0,
        "rpc_fail_out": 0,
        "peer_usage": {"peer-a": {"last_ts_ms": 1}},
        "endpoint_hits": {},
        "history": deque(maxlen=240),
    }
    r.nodes = [SimpleNamespace(current_address="router.test.addr")]
    r.nkn_settings = {
        "enable": True,
        "resolve_timeout_seconds": 20,
        "dm_retries": 1,
    }
    r.service_relays = {
        "whisper_asr": {
            "name": "whisper-asr-relay-test",
            "seed_hex": "b" * 64,
        }
    }
    r.api_host = "127.0.0.1"
    r.api_port = 9071
    r.activity_log = deque(maxlen=240)
    r.targets = {}
    r.cfg = {"targets": {}}
    r.cloudflared_manager = None
    r.stop = threading.Event()

    snapshot_value = deepcopy(snapshot if snapshot is not None else _sample_snapshot())

    def _snapshot(force_refresh: bool = False):
        return deepcopy(snapshot_value)

    r.get_service_snapshot = _snapshot
    r.send_nkn_dm = lambda *_args, **_kwargs: (False, "offline")
    r._create_pending_resolve = lambda target: {
        "request_id": "resolve-test-1",
        "target_address": target,
        "created_at": time.time(),
        "event": threading.Event(),
        "response": None,
    }
    r._pop_pending_resolve = lambda _rid: None
    r._record_resolve_outcome = lambda _ok: None
    r._record_endpoint_usage = lambda _peer, _labels: None
    return r


def _assert_no_raw_secrets(payload: dict) -> None:
    text = json.dumps(payload, sort_keys=True)
    assert "super-secret-password" not in text
    assert "example-api-key" not in text
    assert ("a" * 64) not in text


def _required_contract_fields(route_payload: dict, required: set[str]) -> None:
    missing = sorted(required - set(route_payload.keys()))
    assert not missing, f"missing contract fields: {missing}"


@pytest.fixture()
def api_router():
    return _make_router_for_api()


def test_health_payload_contract(api_router):
    with api_router.pending_resolves_lock:
        pending_count = len(api_router.pending_resolves)
    snapshot = api_router.get_service_snapshot(force_refresh=False)
    payload = api_router._redact_public_payload(api_router._health_payload(snapshot, pending_count))

    _required_contract_fields(payload, HEALTH_REQUIRED)
    _required_contract_fields(
        payload,
        {
            "uptime_seconds",
            "requests_served",
            "pending_resolves",
            "network",
            "nkn",
            "telemetry",
            "snapshot",
        },
    )
    _required_contract_fields(payload["nkn"], {"enabled", "ready", "addresses"})
    _required_contract_fields(
        payload["telemetry"],
        {
            "inbound_messages",
            "outbound_messages",
            "inbound_bytes",
            "outbound_bytes",
            "resolve_requests_in",
            "resolve_requests_out",
            "resolve_success_out",
            "resolve_fail_out",
            "active_peers",
        },
    )
    _assert_no_raw_secrets(payload)


def test_services_snapshot_payload_contract(api_router):
    snapshot = api_router.get_service_snapshot(force_refresh=False)
    payload = api_router._redact_public_payload(api_router._services_snapshot_payload(snapshot))

    _required_contract_fields(payload, {"status", "snapshot"})
    assert payload["status"] == "success"
    assert isinstance(payload["snapshot"], dict)
    _required_contract_fields(payload["snapshot"], {"services", "resolved", "ts_ms", "stale"})
    _assert_no_raw_secrets(payload)


def test_nkn_info_payload_contract(api_router):
    snapshot = api_router.get_service_snapshot(force_refresh=False)
    payload = api_router._redact_public_payload(api_router._nkn_info_payload(snapshot))

    _required_contract_fields(payload, {"status", "network", "nkn", "snapshot"})
    assert payload["status"] == "success"
    _required_contract_fields(payload["nkn"], {"enabled", "ready", "addresses", "service_relays", "seed_persisted"})
    assert payload["nkn"]["seed_persisted"] is True
    _assert_no_raw_secrets(payload)


def test_nkn_resolve_local_payload_contract(api_router):
    snapshot = api_router.get_service_snapshot(force_refresh=True)
    payload = api_router._redact_public_payload(api_router._nkn_resolve_local_payload(snapshot))

    _required_contract_fields(payload, {"status", "mode", "target_address", "snapshot", "resolved"})
    assert payload["status"] == "success"
    assert payload["mode"] == "local"
    assert isinstance(payload["resolved"], dict)
    _assert_no_raw_secrets(payload)


def test_nkn_resolve_remote_send_failure_contract(api_router):
    target_address = "remote.peer.addr"
    pending = api_router._create_pending_resolve(target_address)
    probe_payload = {
        "event": "resolve_tunnels",
        "request_id": pending["request_id"],
        "from": api_router.nodes[0].current_address,
        "timestamp_ms": int(time.time() * 1000),
    }
    ok, err = api_router.send_nkn_dm(target_address, probe_payload, tries=api_router.nkn_settings["dm_retries"])
    assert ok is False
    payload = api_router._redact_public_payload({"status": "error", "message": f"Failed to send DM: {err}"})

    _required_contract_fields(payload, {"status", "message"})
    assert payload["status"] == "error"
