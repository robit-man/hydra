#!/usr/bin/env python3
"""Hydra <-> NoClip interop smoke harness with artifact reporting."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

DEFAULT_ROUTER_BASE = "http://127.0.0.1:9071"
DEFAULT_TIMEOUT_SECONDS = 8.0
DEFAULT_MAX_STAGE_MS = 8000
DEFAULT_ARTIFACT_DIR = Path(__file__).resolve().parents[1] / ".logs"


def _now_ms() -> int:
    return int(time.time() * 1000)


def _load_interop_contract() -> Dict[str, str]:
    fallback = {"name": "hydra_noclip_interop", "version": "1.0.0"}
    contract_path = Path(__file__).resolve().parents[2] / "docs" / "HYDRA_NOCLIP_INTEROP_API_CONTRACT.json"
    try:
        payload = json.loads(contract_path.read_text(encoding="utf-8"))
    except Exception:
        return fallback
    interop = payload.get("interop_contract") if isinstance(payload, dict) else {}
    if not isinstance(interop, dict):
        return fallback
    name = str(interop.get("name") or fallback["name"])
    version = str(interop.get("version") or fallback["version"])
    return {"name": name, "version": version}


INTEROP_CONTRACT = _load_interop_contract()


def _http_json(
    method: str,
    url: str,
    *,
    payload: Optional[Dict[str, Any]] = None,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
) -> Dict[str, Any]:
    body: Optional[bytes] = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, method=method.upper(), data=body, headers=headers)
    started = _now_ms()
    try:
        with urllib.request.urlopen(req, timeout=max(1.0, float(timeout_seconds))) as resp:
            raw = resp.read()
            status = int(resp.getcode() or 0)
        latency_ms = _now_ms() - started
        parsed = json.loads(raw.decode("utf-8")) if raw else {}
        return {"ok": True, "status": status, "json": parsed, "latency_ms": latency_ms, "error": ""}
    except urllib.error.HTTPError as exc:
        latency_ms = _now_ms() - started
        text = ""
        try:
            text = exc.read().decode("utf-8", errors="replace")
        except Exception:
            text = ""
        parsed: Any = {}
        if text:
            try:
                parsed = json.loads(text)
            except Exception:
                parsed = {"raw": text}
        return {
            "ok": False,
            "status": int(exc.code or 0),
            "json": parsed if isinstance(parsed, dict) else {"raw": str(parsed)},
            "latency_ms": latency_ms,
            "error": f"HTTPError {exc.code}",
        }
    except Exception as exc:
        latency_ms = _now_ms() - started
        return {"ok": False, "status": 0, "json": {}, "latency_ms": latency_ms, "error": f"{type(exc).__name__}: {exc}"}


def _slo_gate(duration_ms: int, max_stage_ms: int) -> Tuple[bool, str]:
    if duration_ms <= int(max_stage_ms):
        return True, ""
    return False, f"stage exceeded latency SLO ({duration_ms}ms > {max_stage_ms}ms)"


@dataclass
class StageResult:
    name: str
    ok: bool
    mode: str
    duration_ms: int
    detail: Dict[str, Any]
    error: str = ""

    def as_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "ok": bool(self.ok),
            "mode": self.mode,
            "duration_ms": int(self.duration_ms),
            "detail": self.detail if isinstance(self.detail, dict) else {},
            "error": str(self.error or ""),
        }


def _stage_peer_discovery(
    *,
    router_base: str,
    timeout_seconds: float,
    simulate: bool,
    max_stage_ms: int,
) -> StageResult:
    started = _now_ms()
    name = "peer_discovery_handshake"
    if simulate:
        duration_ms = _now_ms() - started
        slo_ok, slo_error = _slo_gate(duration_ms, max_stage_ms)
        return StageResult(
            name=name,
            ok=slo_ok,
            mode="simulated",
            duration_ms=duration_ms,
            detail={
                "nkn_enabled": True,
                "nkn_ready": True,
                "addresses": ["hydra.sim.0123456789abcdef"],
                "interop_contract_version": INTEROP_CONTRACT["version"],
            },
            error=slo_error,
        )

    probe = _http_json("GET", f"{router_base.rstrip('/')}/nkn/info", timeout_seconds=timeout_seconds)
    payload = probe.get("json") if isinstance(probe.get("json"), dict) else {}
    nkn = payload.get("nkn") if isinstance(payload.get("nkn"), dict) else {}
    addresses = [str(addr) for addr in (nkn.get("addresses") or []) if str(addr)]
    contract_ok = str(payload.get("interop_contract_version") or "") == INTEROP_CONTRACT["version"]
    stage_ok = bool(probe.get("ok") and payload.get("status") == "success" and nkn.get("enabled") is True and bool(addresses) and contract_ok)
    duration_ms = _now_ms() - started
    slo_ok, slo_error = _slo_gate(duration_ms, max_stage_ms)
    if not slo_ok:
        stage_ok = False
    error = probe.get("error") if not stage_ok and probe.get("error") else ""
    if not stage_ok and not error and not contract_ok:
        error = "interop contract version mismatch"
    if not stage_ok and not error and not addresses:
        error = "no ready nkn addresses"
    if slo_error:
        error = slo_error
    return StageResult(
        name=name,
        ok=stage_ok,
        mode="live",
        duration_ms=duration_ms,
        detail={
            "http_status": int(probe.get("status") or 0),
            "latency_ms": int(probe.get("latency_ms") or 0),
            "nkn_enabled": bool(nkn.get("enabled")),
            "nkn_ready": bool(nkn.get("ready")),
            "address_count": len(addresses),
            "interop_contract_version": str(payload.get("interop_contract_version") or ""),
        },
        error=error,
    )


def _stage_endpoint_resolve_sync(
    *,
    router_base: str,
    timeout_seconds: float,
    simulate: bool,
    max_stage_ms: int,
) -> StageResult:
    started = _now_ms()
    name = "endpoint_resolve_state_sync"
    if simulate:
        duration_ms = _now_ms() - started
        slo_ok, slo_error = _slo_gate(duration_ms, max_stage_ms)
        return StageResult(
            name=name,
            ok=slo_ok,
            mode="simulated",
            duration_ms=duration_ms,
            detail={
                "mode": "local",
                "resolved_services": 3,
                "transport_counts": {"local": 3},
                "stale_rejection_count": 0,
            },
            error=slo_error,
        )

    probe = _http_json(
        "POST",
        f"{router_base.rstrip('/')}/nkn/resolve",
        payload={"refresh_local": True},
        timeout_seconds=timeout_seconds,
    )
    payload = probe.get("json") if isinstance(probe.get("json"), dict) else {}
    summary = payload.get("resolve_summary") if isinstance(payload.get("resolve_summary"), dict) else {}
    resolved = payload.get("resolved") if isinstance(payload.get("resolved"), dict) else {}
    rollout = payload.get("rollout_gates") if isinstance(payload.get("rollout_gates"), dict) else {}
    stage_ok = bool(
        probe.get("ok")
        and payload.get("status") == "success"
        and payload.get("mode") in {"local", "remote"}
        and bool(resolved)
        and isinstance(summary.get("transport_counts"), dict)
        and str(payload.get("interop_contract_version") or "") == INTEROP_CONTRACT["version"]
    )
    duration_ms = _now_ms() - started
    slo_ok, slo_error = _slo_gate(duration_ms, max_stage_ms)
    if not slo_ok:
        stage_ok = False
    error = probe.get("error") if not stage_ok and probe.get("error") else ""
    if not stage_ok and not error and not resolved:
        error = "resolved endpoint map empty"
    if slo_error:
        error = slo_error
    return StageResult(
        name=name,
        ok=stage_ok,
        mode="live",
        duration_ms=duration_ms,
        detail={
            "http_status": int(probe.get("status") or 0),
            "latency_ms": int(probe.get("latency_ms") or 0),
            "mode": str(payload.get("mode") or ""),
            "resolved_services": len(resolved),
            "transport_counts": summary.get("transport_counts") if isinstance(summary.get("transport_counts"), dict) else {},
            "stale_rejection_count": int(summary.get("stale_rejection_count") or 0),
            "rollout_ready": bool(rollout.get("ready")) if rollout else None,
        },
        error=error,
    )


def _build_overlay_mutation_envelope() -> Dict[str, Any]:
    ts = _now_ms()
    return {
        "type": "hybrid-bridge-update",
        "event": "interop.asset.pose",
        "messageId": f"smoke-pose-{ts}",
        "timestamp_ms": ts,
        "interop_contract": dict(INTEROP_CONTRACT),
        "interop_contract_version": INTEROP_CONTRACT["version"],
        "target": {
            "sessionId": "smoke-session",
            "overlayId": "smoke-overlay",
            "itemId": "smoke-item",
        },
        "pose": {
            "position": {"lat": 37.7749, "lon": -122.4194, "elevation_m": 12.5},
        },
    }


def _validate_overlay_mutation_envelope(envelope: Dict[str, Any]) -> Tuple[bool, str]:
    if not isinstance(envelope, dict):
        return False, "envelope not object"
    if str(envelope.get("event") or "") != "interop.asset.pose":
        return False, "unexpected event"
    if str(envelope.get("type") or "") != "hybrid-bridge-update":
        return False, "unexpected type"
    version = str(envelope.get("interop_contract_version") or "")
    if version != INTEROP_CONTRACT["version"]:
        return False, "contract version mismatch"
    target = envelope.get("target") if isinstance(envelope.get("target"), dict) else {}
    if not str(target.get("overlayId") or "") or not str(target.get("itemId") or ""):
        return False, "target missing overlay/item ids"
    pose = envelope.get("pose") if isinstance(envelope.get("pose"), dict) else {}
    position = pose.get("position") if isinstance(pose.get("position"), dict) else {}
    lat = position.get("lat")
    lon = position.get("lon")
    if not isinstance(lat, (int, float)) or not isinstance(lon, (int, float)):
        return False, "pose missing lat/lon"
    return True, ""


def _stage_overlay_mutation(
    *,
    overlay_ingress_url: str,
    timeout_seconds: float,
    simulate: bool,
    max_stage_ms: int,
    strict_live: bool,
) -> StageResult:
    started = _now_ms()
    name = "overlay_mutation_bridge_ingress"
    envelope = _build_overlay_mutation_envelope()
    valid, validation_error = _validate_overlay_mutation_envelope(envelope)
    if not valid:
        duration_ms = _now_ms() - started
        return StageResult(
            name=name,
            ok=False,
            mode="simulated",
            duration_ms=duration_ms,
            detail={"envelope": envelope},
            error=validation_error,
        )

    if simulate:
        duration_ms = _now_ms() - started
        slo_ok, slo_error = _slo_gate(duration_ms, max_stage_ms)
        return StageResult(
            name=name,
            ok=slo_ok,
            mode="simulated",
            duration_ms=duration_ms,
            detail={"envelope_checksum": hashlib.sha256(json.dumps(envelope, sort_keys=True).encode("utf-8")).hexdigest()},
            error=slo_error,
        )

    if not overlay_ingress_url:
        duration_ms = _now_ms() - started
        if strict_live:
            return StageResult(
                name=name,
                ok=False,
                mode="live",
                duration_ms=duration_ms,
                detail={"envelope": envelope},
                error="missing --overlay-ingress-url for live overlay mutation stage",
            )
        slo_ok, slo_error = _slo_gate(duration_ms, max_stage_ms)
        return StageResult(
            name=name,
            ok=slo_ok,
            mode="simulated-fallback",
            duration_ms=duration_ms,
            detail={"reason": "overlay ingress URL not provided", "envelope": envelope},
            error=slo_error,
        )

    probe = _http_json("POST", overlay_ingress_url, payload=envelope, timeout_seconds=timeout_seconds)
    payload = probe.get("json") if isinstance(probe.get("json"), dict) else {}
    accepted = bool(probe.get("ok")) and int(probe.get("status") or 0) in (200, 201, 202)
    status_token = str(payload.get("status") or payload.get("result") or "").lower()
    semantic_ok = status_token in ("ok", "success", "accepted", "applied")
    if not payload:
        semantic_ok = accepted
    stage_ok = bool(accepted and semantic_ok)
    duration_ms = _now_ms() - started
    slo_ok, slo_error = _slo_gate(duration_ms, max_stage_ms)
    if not slo_ok:
        stage_ok = False
    error = probe.get("error") if not stage_ok and probe.get("error") else ""
    if not stage_ok and not error:
        error = "overlay ingress did not acknowledge request"
    if slo_error:
        error = slo_error
    return StageResult(
        name=name,
        ok=stage_ok,
        mode="live",
        duration_ms=duration_ms,
        detail={
            "http_status": int(probe.get("status") or 0),
            "latency_ms": int(probe.get("latency_ms") or 0),
            "ack_status": status_token,
            "response_keys": sorted(payload.keys()) if isinstance(payload, dict) else [],
        },
        error=error,
    )


def _build_media_payload() -> Dict[str, Any]:
    audio_bytes = bytes(range(64))
    video_bytes = (b"\xff\xd8\xff\xe0" + bytes(range(32)) + b"\xff\xd9")
    return {
        "audio": {
            "format": "pcm16",
            "sampleRate": 16000,
            "channels": 1,
            "data": base64.b64encode(audio_bytes).decode("ascii"),
        },
        "video": {
            "mime": "image/jpeg",
            "width": 16,
            "height": 16,
            "data": base64.b64encode(video_bytes).decode("ascii"),
        },
    }


def _local_media_roundtrip(payload: Dict[str, Any]) -> Dict[str, Any]:
    audio_b64 = str((payload.get("audio") or {}).get("data") or "")
    video_b64 = str((payload.get("video") or {}).get("data") or "")
    audio_raw = base64.b64decode(audio_b64.encode("ascii")) if audio_b64 else b""
    video_raw = base64.b64decode(video_b64.encode("ascii")) if video_b64 else b""
    return {
        "audio_bytes": len(audio_raw),
        "video_bytes": len(video_raw),
        "audio_sha256": hashlib.sha256(audio_raw).hexdigest(),
        "video_sha256": hashlib.sha256(video_raw).hexdigest(),
    }


def _stage_media_roundtrip(
    *,
    media_echo_url: str,
    timeout_seconds: float,
    simulate: bool,
    max_stage_ms: int,
    strict_live: bool,
) -> StageResult:
    started = _now_ms()
    name = "audio_video_roundtrip"
    payload = _build_media_payload()
    local_stats = _local_media_roundtrip(payload)
    local_ok = bool(local_stats["audio_bytes"] > 0 and local_stats["video_bytes"] > 0)

    if simulate:
        duration_ms = _now_ms() - started
        slo_ok, slo_error = _slo_gate(duration_ms, max_stage_ms)
        return StageResult(
            name=name,
            ok=bool(local_ok and slo_ok),
            mode="simulated",
            duration_ms=duration_ms,
            detail={"local_roundtrip": local_stats},
            error=slo_error if not slo_ok else "",
        )

    if not media_echo_url:
        duration_ms = _now_ms() - started
        if strict_live:
            return StageResult(
                name=name,
                ok=False,
                mode="live",
                duration_ms=duration_ms,
                detail={"local_roundtrip": local_stats},
                error="missing --media-echo-url for live media roundtrip stage",
            )
        slo_ok, slo_error = _slo_gate(duration_ms, max_stage_ms)
        return StageResult(
            name=name,
            ok=bool(local_ok and slo_ok),
            mode="simulated-fallback",
            duration_ms=duration_ms,
            detail={"local_roundtrip": local_stats, "reason": "media echo URL not provided"},
            error=slo_error if not slo_ok else "",
        )

    probe = _http_json("POST", media_echo_url, payload=payload, timeout_seconds=timeout_seconds)
    resp = probe.get("json") if isinstance(probe.get("json"), dict) else {}
    echoed_audio = str((resp.get("audio") or {}).get("data") or "")
    echoed_video = str((resp.get("video") or {}).get("data") or "")
    remote_ok = bool(echoed_audio == (payload["audio"]["data"]) and echoed_video == (payload["video"]["data"]))
    stage_ok = bool(local_ok and probe.get("ok") and remote_ok)
    duration_ms = _now_ms() - started
    slo_ok, slo_error = _slo_gate(duration_ms, max_stage_ms)
    if not slo_ok:
        stage_ok = False
    error = probe.get("error") if not stage_ok and probe.get("error") else ""
    if not stage_ok and not error and not remote_ok:
        error = "remote media echo mismatch"
    if slo_error:
        error = slo_error
    return StageResult(
        name=name,
        ok=stage_ok,
        mode="live",
        duration_ms=duration_ms,
        detail={
            "http_status": int(probe.get("status") or 0),
            "latency_ms": int(probe.get("latency_ms") or 0),
            "local_roundtrip": local_stats,
            "remote_echo_ok": remote_ok,
        },
        error=error,
    )


def _write_artifact(payload: Dict[str, Any], artifact_path: str) -> str:
    if artifact_path:
        path = Path(artifact_path).expanduser().resolve()
    else:
        stamp = time.strftime("%Y%m%d_%H%M%S", time.localtime())
        path = DEFAULT_ARTIFACT_DIR / f"hydra_noclip_interop_smoke_{stamp}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return str(path)


def run_smoke(args: argparse.Namespace) -> Dict[str, Any]:
    started = _now_ms()
    stages: List[StageResult] = [
        _stage_peer_discovery(
            router_base=args.router_base,
            timeout_seconds=args.timeout_seconds,
            simulate=args.simulate,
            max_stage_ms=args.max_stage_ms,
        ),
        _stage_endpoint_resolve_sync(
            router_base=args.router_base,
            timeout_seconds=args.timeout_seconds,
            simulate=args.simulate,
            max_stage_ms=args.max_stage_ms,
        ),
        _stage_overlay_mutation(
            overlay_ingress_url=args.overlay_ingress_url,
            timeout_seconds=args.timeout_seconds,
            simulate=args.simulate,
            max_stage_ms=args.max_stage_ms,
            strict_live=args.strict_live,
        ),
        _stage_media_roundtrip(
            media_echo_url=args.media_echo_url,
            timeout_seconds=args.timeout_seconds,
            simulate=args.simulate,
            max_stage_ms=args.max_stage_ms,
            strict_live=args.strict_live,
        ),
    ]
    stage_failures = [stage for stage in stages if not stage.ok]
    elapsed_ms = _now_ms() - started
    artifact = {
        "ok": len(stage_failures) == 0,
        "interop_contract": dict(INTEROP_CONTRACT),
        "router_base": str(args.router_base),
        "mode": "simulated" if args.simulate else "live",
        "max_stage_ms": int(args.max_stage_ms),
        "started_at_ms": started,
        "finished_at_ms": _now_ms(),
        "duration_ms": elapsed_ms,
        "stage_results": [stage.as_dict() for stage in stages],
        "failed_stages": [stage.name for stage in stage_failures],
        "error_rate": round(len(stage_failures) / max(1, len(stages)), 4),
    }
    artifact["artifact_path"] = _write_artifact(artifact, args.artifact)
    return artifact


def _main() -> int:
    parser = argparse.ArgumentParser(description="Hydra <-> NoClip interop smoke harness")
    parser.add_argument("--router-base", default=DEFAULT_ROUTER_BASE, help="Hydra router control-plane base URL")
    parser.add_argument(
        "--overlay-ingress-url",
        default="",
        help="Optional live overlay ingress endpoint for mutation stage",
    )
    parser.add_argument(
        "--media-echo-url",
        default="",
        help="Optional live media echo endpoint for round-trip stage",
    )
    parser.add_argument("--timeout-seconds", type=float, default=DEFAULT_TIMEOUT_SECONDS, help="HTTP timeout per request")
    parser.add_argument("--max-stage-ms", type=int, default=DEFAULT_MAX_STAGE_MS, help="Per-stage latency SLO threshold")
    parser.add_argument("--artifact", default="", help="Artifact output path (JSON)")
    parser.add_argument("--simulate", action="store_true", help="Run deterministic simulation without live HTTP probes")
    parser.add_argument(
        "--strict-live",
        action="store_true",
        help="Fail live run when optional overlay/media URLs are not provided",
    )
    parser.add_argument("--json", action="store_true", help="Print full JSON result to stdout")
    args = parser.parse_args()

    result = run_smoke(args)
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        for stage in result["stage_results"]:
            marker = "PASS" if stage["ok"] else "FAIL"
            print(f"[{marker}] {stage['name']} ({stage['mode']}, {stage['duration_ms']}ms)")
            if stage.get("error"):
                print(f"       error={stage['error']}")
        print(f"overall={result['ok']} error_rate={result['error_rate']}")
        print(f"artifact={result['artifact_path']}")

    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(_main())
