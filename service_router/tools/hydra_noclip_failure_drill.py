#!/usr/bin/env python3
"""Hydra <-> NoClip failure drill harness with artifact reporting."""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple

os.environ.setdefault("HYDRA_ROUTER_SKIP_BOOTSTRAP", "1")

SERVICE_ROUTER_DIR = Path(__file__).resolve().parents[1]
if str(SERVICE_ROUTER_DIR) not in sys.path:
    sys.path.insert(0, str(SERVICE_ROUTER_DIR))

import router  # type: ignore
from tools import simulate_stale_tunnel  # type: ignore

DEFAULT_TIMEOUT_SECONDS = 8.0
DEFAULT_ARTIFACT_DIR = Path(__file__).resolve().parents[1] / ".logs"


def _now_ms() -> int:
    return int(time.time() * 1000)


def _http_json(method: str, url: str, payload: Dict[str, Any], timeout_seconds: float) -> Dict[str, Any]:
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        method=method.upper(),
        data=body,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    started = _now_ms()
    try:
        with urllib.request.urlopen(req, timeout=max(1.0, timeout_seconds)) as resp:
            raw = resp.read()
            status = int(resp.getcode() or 0)
        parsed = json.loads(raw.decode("utf-8")) if raw else {}
        return {
            "ok": True,
            "status": status,
            "json": parsed if isinstance(parsed, dict) else {},
            "latency_ms": _now_ms() - started,
            "error": "",
        }
    except urllib.error.HTTPError as exc:
        payload_out: Dict[str, Any] = {}
        try:
            raw = exc.read().decode("utf-8", errors="replace")
            parsed = json.loads(raw) if raw else {}
            if isinstance(parsed, dict):
                payload_out = parsed
        except Exception:
            payload_out = {}
        return {
            "ok": False,
            "status": int(exc.code or 0),
            "json": payload_out,
            "latency_ms": _now_ms() - started,
            "error": f"HTTPError {exc.code}",
        }
    except Exception as exc:
        return {
            "ok": False,
            "status": 0,
            "json": {},
            "latency_ms": _now_ms() - started,
            "error": f"{type(exc).__name__}: {exc}",
        }


def _make_router() -> "router.Router":
    instance = router.Router.__new__(router.Router)
    instance.targets = {"asr": "http://127.0.0.1:8126"}
    instance.cfg = {"targets": {"asr": "http://127.0.0.1:8126"}}
    instance.latest_service_status = {}
    instance.cloudflared_manager = None
    return instance


@dataclass
class DrillResult:
    name: str
    ok: bool
    duration_ms: int
    detail: Dict[str, Any]
    error: str = ""

    def as_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "ok": bool(self.ok),
            "duration_ms": int(self.duration_ms),
            "detail": self.detail if isinstance(self.detail, dict) else {},
            "error": str(self.error or ""),
        }


def _drill_nats_unavailable() -> DrillResult:
    started = _now_ms()
    name = "nats_unavailable_fallback"
    r = _make_router()
    services = {
        "asr": {
            "transport": "local",
            "base_url": "http://127.0.0.1:8126",
            "http_endpoint": "http://127.0.0.1:8126",
            "ws_endpoint": "",
            "fallback": {
                "selected_transport": "nats",
                "order": ["cloudflare", "upnp", "nats", "nkn", "local"],
                "cloudflare": {"public_base_url": "", "http_endpoint": "", "ws_endpoint": "", "error": "inactive"},
                "upnp": {"public_base_url": "", "http_endpoint": "", "ws_endpoint": ""},
                "nats": {"public_base_url": "", "http_endpoint": "", "ws_endpoint": "", "error": "broker unavailable"},
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
    asr = resolved.get("asr", {}) if isinstance(resolved, dict) else {}
    selected = str(asr.get("selected_transport") or asr.get("transport") or "")
    local_url = str(asr.get("base_url") or "")
    ok = bool(selected == "local" and local_url.startswith("http://127.0.0.1"))
    duration_ms = _now_ms() - started
    return DrillResult(
        name=name,
        ok=ok,
        duration_ms=duration_ms,
        detail={
            "selected_transport": selected,
            "base_url": local_url,
            "selection_reason": str(asr.get("selection_reason") or ""),
            "degraded_behavior": "fallback to local endpoint when NATS candidate is unavailable",
        },
        error="" if ok else "resolver did not demote unavailable NATS endpoint",
    )


def _drill_stale_cloudflare() -> DrillResult:
    started = _now_ms()
    name = "stale_cloudflare_tunnel_demotion"
    result = simulate_stale_tunnel.run_simulation()
    ok = bool(result.get("ok"))
    duration_ms = _now_ms() - started
    return DrillResult(
        name=name,
        ok=ok,
        duration_ms=duration_ms,
        detail={
            "checks": result.get("checks", []),
            "degraded_behavior": "stale cloudflare tunnel is demoted to next healthy transport",
        },
        error="" if ok else "stale cloudflare simulation failed",
    )


def _validate_shared_key_meta(metadata: Dict[str, Any]) -> Tuple[bool, List[str]]:
    errors: List[str] = []
    key_id = str(metadata.get("key_id") or "").strip()
    if len(key_id) < 8:
        errors.append("key_id must be at least 8 chars")
    scheme = str(metadata.get("scheme") or "").strip().lower()
    if scheme not in {"hmac-sha256", "ed25519"}:
        errors.append("unsupported scheme")
    key_hash = str(metadata.get("key_hash") or "").strip().lower()
    if len(key_hash) != 64 or any(ch not in "0123456789abcdef" for ch in key_hash):
        errors.append("key_hash must be 64 hex chars")
    issued_at = metadata.get("issued_at")
    if not isinstance(issued_at, int) or issued_at <= 0:
        errors.append("issued_at must be positive integer epoch seconds")
    return len(errors) == 0, errors


def _drill_invalid_shared_key_metadata() -> DrillResult:
    started = _now_ms()
    name = "invalid_shared_key_metadata_rejected"
    invalid = {
        "key_id": "bad",
        "scheme": "sha1",
        "key_hash": "1234",
        "issued_at": "yesterday",
    }
    valid, errors = _validate_shared_key_meta(invalid)
    ok = bool(not valid and len(errors) >= 3)
    duration_ms = _now_ms() - started
    return DrillResult(
        name=name,
        ok=ok,
        duration_ms=duration_ms,
        detail={
            "error_count": len(errors),
            "errors": errors,
            "degraded_behavior": "reject invalid key metadata and keep previous trust state",
        },
        error="" if ok else "invalid shared-key metadata was accepted",
    )


def _drill_backend_auth_denial(
    endpoint_update_url: str,
    timeout_seconds: float,
    simulate: bool,
) -> DrillResult:
    started = _now_ms()
    name = "backend_auth_denial_on_endpoint_update"

    if not endpoint_update_url:
        duration_ms = _now_ms() - started
        if not simulate:
            return DrillResult(
                name=name,
                ok=False,
                duration_ms=duration_ms,
                detail={},
                error="missing --endpoint-update-url for live auth-denial drill",
            )
        return DrillResult(
            name=name,
            ok=True,
            duration_ms=duration_ms,
            detail={
                "mode": "simulated",
                "expected_status": 401,
                "degraded_behavior": "endpoint update denied without auth token",
            },
        )

    payload = {"selectedTransport": "nkn", "network": "hydra", "staleRejectionCount": 0}
    probe = _http_json("POST", endpoint_update_url, payload=payload, timeout_seconds=timeout_seconds)
    status = int(probe.get("status") or 0)
    ok = status in {401, 403}
    duration_ms = _now_ms() - started
    return DrillResult(
        name=name,
        ok=ok,
        duration_ms=duration_ms,
        detail={
            "http_status": status,
            "latency_ms": int(probe.get("latency_ms") or 0),
            "response": probe.get("json") if isinstance(probe.get("json"), dict) else {},
            "degraded_behavior": "endpoint update denied without auth token",
        },
        error="" if ok else (probe.get("error") or f"expected 401/403, got {status}"),
    )


def _write_artifact(payload: Dict[str, Any], artifact_path: str) -> str:
    if artifact_path:
        path = Path(artifact_path).expanduser().resolve()
    else:
        stamp = time.strftime("%Y%m%d_%H%M%S", time.localtime())
        path = DEFAULT_ARTIFACT_DIR / f"hydra_noclip_failure_drill_{stamp}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return str(path)


def run_drills(args: argparse.Namespace) -> Dict[str, Any]:
    started = _now_ms()
    drills = [
        _drill_nats_unavailable(),
        _drill_stale_cloudflare(),
        _drill_invalid_shared_key_metadata(),
        _drill_backend_auth_denial(
            endpoint_update_url=args.endpoint_update_url,
            timeout_seconds=args.timeout_seconds,
            simulate=args.simulate,
        ),
    ]
    failures = [drill for drill in drills if not drill.ok]
    payload = {
        "ok": len(failures) == 0,
        "started_at_ms": started,
        "finished_at_ms": _now_ms(),
        "duration_ms": _now_ms() - started,
        "drill_results": [drill.as_dict() for drill in drills],
        "failed_drills": [drill.name for drill in failures],
        "error_rate": round(len(failures) / max(1, len(drills)), 4),
    }
    payload["artifact_path"] = _write_artifact(payload, args.artifact)
    return payload


def _main() -> int:
    parser = argparse.ArgumentParser(description="Hydra <-> NoClip failure drill harness")
    parser.add_argument(
        "--endpoint-update-url",
        default="",
        help="Optional live endpoint-update URL expected to deny unauthenticated write attempts",
    )
    parser.add_argument("--timeout-seconds", type=float, default=DEFAULT_TIMEOUT_SECONDS, help="HTTP timeout per drill request")
    parser.add_argument("--artifact", default="", help="Artifact output path (JSON)")
    parser.add_argument("--simulate", action="store_true", help="Allow simulated auth-denial drill when endpoint URL is unavailable")
    parser.add_argument("--json", action="store_true", help="Print full JSON result to stdout")
    args = parser.parse_args()

    result = run_drills(args)
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        for drill in result["drill_results"]:
            marker = "PASS" if drill["ok"] else "FAIL"
            print(f"[{marker}] {drill['name']} ({drill['duration_ms']}ms)")
            if drill.get("error"):
                print(f"       error={drill['error']}")
        print(f"overall={result['ok']} error_rate={result['error_rate']}")
        print(f"artifact={result['artifact_path']}")

    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(_main())
