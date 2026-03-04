#!/usr/bin/env python3
"""Unified rollout-gate runner for Hydra <-> NoClip interop."""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, Optional, Tuple

TOOLS_DIR = Path(__file__).resolve().parent
if str(TOOLS_DIR) not in sys.path:
    sys.path.insert(0, str(TOOLS_DIR))

import hydra_noclip_failure_drill as failure_drill  # type: ignore
import hydra_noclip_interop_smoke as interop_smoke  # type: ignore

DEFAULT_ROUTER_BASE = interop_smoke.DEFAULT_ROUTER_BASE
DEFAULT_TIMEOUT_SECONDS = interop_smoke.DEFAULT_TIMEOUT_SECONDS
DEFAULT_MAX_STAGE_MS = interop_smoke.DEFAULT_MAX_STAGE_MS
DEFAULT_ARTIFACT_DIR = Path(__file__).resolve().parents[1] / ".logs"
INTEROP_CONTRACT_VERSION = str((interop_smoke.INTEROP_CONTRACT or {}).get("version") or "")


def _now_ms() -> int:
    return int(time.time() * 1000)


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
        parsed = json.loads(raw.decode("utf-8")) if raw else {}
        return {
            "ok": True,
            "status": status,
            "json": parsed if isinstance(parsed, dict) else {},
            "latency_ms": _now_ms() - started,
            "error": "",
        }
    except urllib.error.HTTPError as exc:
        parsed: Dict[str, Any] = {}
        try:
            text = exc.read().decode("utf-8", errors="replace")
            decoded = json.loads(text) if text else {}
            if isinstance(decoded, dict):
                parsed = decoded
        except Exception:
            parsed = {}
        return {
            "ok": False,
            "status": int(exc.code or 0),
            "json": parsed,
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


def _status_ok(payload: Dict[str, Any]) -> bool:
    token = str(payload.get("status") or payload.get("result") or "").strip().lower()
    if token in {"ok", "success", "healthy", "ready", "pass"}:
        return True
    ok_value = payload.get("ok")
    return isinstance(ok_value, bool) and ok_value


def _extract_gate_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    gate = payload.get("rollout_gates")
    if isinstance(gate, dict):
        return gate
    gate = payload.get("interop_gate")
    if isinstance(gate, dict):
        return gate
    interop = payload.get("interop")
    if isinstance(interop, dict):
        gate = interop.get("rollout_gates")
        if isinstance(gate, dict):
            return gate
        gate = interop.get("interop_gate")
        if isinstance(gate, dict):
            return gate
    da3 = payload.get("da3")
    if isinstance(da3, dict):
        gate = da3.get("rollout_gates")
        if isinstance(gate, dict):
            return gate
        gate = da3.get("interop_gate")
        if isinstance(gate, dict):
            return gate
    return {}


@dataclass
class GateCheck:
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


def _check_router_health_rollout_gate(router_base: str, timeout_seconds: float, simulate: bool) -> GateCheck:
    started = _now_ms()
    name = "router_health_rollout_gate"
    if simulate:
        return GateCheck(
            name=name,
            ok=True,
            mode="simulated",
            duration_ms=_now_ms() - started,
            detail={
                "rollout_ready": True,
                "contract_version_ok": True,
                "resolved_services": 3,
                "pending_resolves_clear": True,
                "resolve_error_rate_ok": True,
                "interop_contract_version": INTEROP_CONTRACT_VERSION,
            },
        )

    probe = _http_json("GET", f"{router_base.rstrip('/')}/health", timeout_seconds=timeout_seconds)
    payload = probe.get("json") if isinstance(probe.get("json"), dict) else {}
    rollout = payload.get("rollout_gates") if isinstance(payload.get("rollout_gates"), dict) else {}
    resolve_summary = payload.get("resolve_summary") if isinstance(payload.get("resolve_summary"), dict) else {}
    resolved_services = int(rollout.get("resolved_services") or resolve_summary.get("service_count") or 0)
    stage_ok = bool(
        probe.get("ok")
        and payload.get("status") == "ok"
        and bool(rollout.get("ready"))
        and bool(rollout.get("contract_version_ok"))
        and bool(rollout.get("pending_resolves_clear"))
        and bool(rollout.get("resolve_error_rate_ok"))
        and resolved_services > 0
    )
    error = ""
    if not stage_ok:
        error = probe.get("error") or "router /health rollout gate is not ready"
    return GateCheck(
        name=name,
        ok=stage_ok,
        mode="live",
        duration_ms=_now_ms() - started,
        detail={
            "http_status": int(probe.get("status") or 0),
            "latency_ms": int(probe.get("latency_ms") or 0),
            "rollout_ready": bool(rollout.get("ready")),
            "contract_version_ok": bool(rollout.get("contract_version_ok")),
            "resolved_services": resolved_services,
            "pending_resolves_clear": bool(rollout.get("pending_resolves_clear")),
            "resolve_error_rate_ok": bool(rollout.get("resolve_error_rate_ok")),
            "interop_contract_version": str(payload.get("interop_contract_version") or ""),
        },
        error=error,
    )


def _check_router_resolve_rollout_gate(router_base: str, timeout_seconds: float, simulate: bool) -> GateCheck:
    started = _now_ms()
    name = "router_resolve_rollout_gate"
    if simulate:
        return GateCheck(
            name=name,
            ok=True,
            mode="simulated",
            duration_ms=_now_ms() - started,
            detail={
                "mode": "local",
                "rollout_ready": True,
                "contract_version_ok": True,
                "resolved_services": 3,
                "interop_contract_version": INTEROP_CONTRACT_VERSION,
            },
        )

    probe = _http_json(
        "POST",
        f"{router_base.rstrip('/')}/nkn/resolve",
        payload={"refresh_local": True},
        timeout_seconds=timeout_seconds,
    )
    payload = probe.get("json") if isinstance(probe.get("json"), dict) else {}
    rollout = payload.get("rollout_gates") if isinstance(payload.get("rollout_gates"), dict) else {}
    resolve_summary = payload.get("resolve_summary") if isinstance(payload.get("resolve_summary"), dict) else {}
    mode = str(payload.get("mode") or "").strip().lower()
    resolved_services = int(rollout.get("resolved_services") or resolve_summary.get("service_count") or 0)
    stage_ok = bool(
        probe.get("ok")
        and payload.get("status") == "success"
        and mode in {"local", "remote"}
        and bool(rollout.get("ready"))
        and bool(rollout.get("contract_version_ok"))
        and resolved_services > 0
        and bool(rollout.get("resolve_error_rate_ok"))
    )
    error = ""
    if not stage_ok:
        error = probe.get("error") or "router /nkn/resolve rollout gate is not ready"
    return GateCheck(
        name=name,
        ok=stage_ok,
        mode="live",
        duration_ms=_now_ms() - started,
        detail={
            "http_status": int(probe.get("status") or 0),
            "latency_ms": int(probe.get("latency_ms") or 0),
            "mode": mode,
            "rollout_ready": bool(rollout.get("ready")),
            "contract_version_ok": bool(rollout.get("contract_version_ok")),
            "resolved_services": resolved_services,
            "resolve_error_rate_ok": bool(rollout.get("resolve_error_rate_ok")),
            "interop_contract_version": str(payload.get("interop_contract_version") or ""),
        },
        error=error,
    )


def _check_backend_health(
    name: str,
    url: str,
    timeout_seconds: float,
    simulate: bool,
    required: bool,
) -> GateCheck:
    started = _now_ms()
    if simulate:
        return GateCheck(
            name=name,
            ok=True,
            mode="simulated",
            duration_ms=_now_ms() - started,
            detail={
                "url": url,
                "required_in_live": bool(required),
                "status_ok": True,
                "gate_payload_present": True,
                "gate_ready": True,
            },
        )

    if not url:
        if required:
            return GateCheck(
                name=name,
                ok=False,
                mode="live",
                duration_ms=_now_ms() - started,
                detail={"url": "", "required": True},
                error="missing required backend health URL",
            )
        return GateCheck(
            name=name,
            ok=True,
            mode="skipped",
            duration_ms=_now_ms() - started,
            detail={"url": "", "required": False, "skipped": True},
        )

    probe = _http_json("GET", url, timeout_seconds=timeout_seconds)
    payload = probe.get("json") if isinstance(probe.get("json"), dict) else {}
    gate = _extract_gate_payload(payload)
    gate_status = str(gate.get("status") or gate.get("result") or "").strip().lower() if gate else ""
    gate_ready = bool(gate.get("ready")) if gate else False
    if not gate_ready and gate_status in {"ok", "success", "ready", "pass", "green"}:
        gate_ready = True
    contract_reported = str(
        payload.get("interop_contract_version")
        or gate.get("contract_version_reported")
        or gate.get("contract_version")
        or ""
    )
    contract_ok = True
    if contract_reported and INTEROP_CONTRACT_VERSION:
        contract_ok = contract_reported == INTEROP_CONTRACT_VERSION
    stage_ok = bool(probe.get("ok") and _status_ok(payload) and bool(gate) and gate_ready and contract_ok)
    error = ""
    if not stage_ok:
        error = probe.get("error") or "backend health gate not ready"
    return GateCheck(
        name=name,
        ok=stage_ok,
        mode="live",
        duration_ms=_now_ms() - started,
        detail={
            "url": url,
            "http_status": int(probe.get("status") or 0),
            "latency_ms": int(probe.get("latency_ms") or 0),
            "status_ok": _status_ok(payload),
            "gate_payload_present": bool(gate),
            "gate_ready": gate_ready,
            "contract_version_reported": contract_reported,
            "contract_version_expected": INTEROP_CONTRACT_VERSION,
            "contract_version_ok": contract_ok,
        },
        error=error,
    )


def _resolve_artifact_paths(artifact: str) -> Tuple[Path, Path, Path]:
    if artifact:
        combined = Path(artifact).expanduser().resolve()
        stem = combined.stem if combined.suffix else combined.name
    else:
        stamp = time.strftime("%Y%m%d_%H%M%S", time.localtime())
        stem = f"hydra_noclip_rollout_gate_{stamp}"
        combined = DEFAULT_ARTIFACT_DIR / f"{stem}.json"
    smoke_path = combined.parent / f"{stem}.smoke.json"
    drill_path = combined.parent / f"{stem}.drills.json"
    return combined, smoke_path, drill_path


def _write_artifact(payload: Dict[str, Any], path: Path) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return str(path)


def _run_smoke(args: argparse.Namespace, artifact_path: Path) -> Dict[str, Any]:
    smoke_args = SimpleNamespace(
        router_base=args.router_base,
        overlay_ingress_url=args.overlay_ingress_url,
        media_echo_url=args.media_echo_url,
        timeout_seconds=args.timeout_seconds,
        max_stage_ms=args.max_stage_ms,
        artifact=str(artifact_path),
        simulate=args.simulate,
        strict_live=args.strict_live,
    )
    return interop_smoke.run_smoke(smoke_args)


def _run_failure_drills(args: argparse.Namespace, artifact_path: Path) -> Dict[str, Any]:
    drill_args = SimpleNamespace(
        endpoint_update_url=args.endpoint_update_url,
        timeout_seconds=args.timeout_seconds,
        artifact=str(artifact_path),
        simulate=args.simulate,
    )
    return failure_drill.run_drills(drill_args)


def run_rollout_gate(args: argparse.Namespace) -> Dict[str, Any]:
    started = _now_ms()
    combined_path, smoke_path, drill_path = _resolve_artifact_paths(args.artifact)
    smoke_result = _run_smoke(args, smoke_path)
    drill_result = _run_failure_drills(args, drill_path)

    gate_checks = [
        _check_router_health_rollout_gate(args.router_base, args.timeout_seconds, args.simulate),
        _check_router_resolve_rollout_gate(args.router_base, args.timeout_seconds, args.simulate),
        _check_backend_health(
            "backend_health_rollout_gate",
            args.backend_health_url,
            args.timeout_seconds,
            args.simulate,
            args.require_backend_health,
        ),
        _check_backend_health(
            "backend_da3_rollout_gate",
            args.da3_health_url,
            args.timeout_seconds,
            args.simulate,
            args.require_backend_health,
        ),
    ]

    failed_checks = []
    if not smoke_result.get("ok"):
        failed_checks.append("interop_smoke")
    if not drill_result.get("ok"):
        failed_checks.append("failure_drills")
    failed_checks.extend(check.name for check in gate_checks if not check.ok)

    payload = {
        "ok": len(failed_checks) == 0,
        "mode": "simulated" if args.simulate else "live",
        "interop_contract_version_expected": INTEROP_CONTRACT_VERSION,
        "router_base": args.router_base,
        "started_at_ms": started,
        "finished_at_ms": _now_ms(),
        "duration_ms": _now_ms() - started,
        "smoke_result": smoke_result,
        "failure_drill_result": drill_result,
        "gate_checks": [check.as_dict() for check in gate_checks],
        "failed_checks": failed_checks,
        "summary": {
            "smoke_ok": bool(smoke_result.get("ok")),
            "failure_drills_ok": bool(drill_result.get("ok")),
            "gate_checks_failed": len([check for check in gate_checks if not check.ok]),
            "gate_checks_total": len(gate_checks),
            "failed_checks_total": len(failed_checks),
        },
        "artifacts": {
            "consolidated": str(combined_path),
            "smoke": str(smoke_result.get("artifact_path") or smoke_path),
            "failure_drills": str(drill_result.get("artifact_path") or drill_path),
        },
    }
    payload["artifact_path"] = _write_artifact(payload, combined_path)
    return payload


def _print_human(result: Dict[str, Any]) -> None:
    smoke = result.get("smoke_result") if isinstance(result.get("smoke_result"), dict) else {}
    drills = result.get("failure_drill_result") if isinstance(result.get("failure_drill_result"), dict) else {}
    smoke_marker = "PASS" if smoke.get("ok") else "FAIL"
    drills_marker = "PASS" if drills.get("ok") else "FAIL"
    print(f"[{smoke_marker}] interop_smoke artifact={smoke.get('artifact_path', '')}")
    print(f"[{drills_marker}] failure_drills artifact={drills.get('artifact_path', '')}")
    for check in result.get("gate_checks", []):
        if not isinstance(check, dict):
            continue
        mode = str(check.get("mode") or "live")
        marker = "SKIP" if mode == "skipped" else ("PASS" if check.get("ok") else "FAIL")
        print(f"[{marker}] {check.get('name')} ({mode}, {check.get('duration_ms', 0)}ms)")
        if check.get("error"):
            print(f"       error={check.get('error')}")
    print(f"overall={result.get('ok')} failed_checks={','.join(result.get('failed_checks', [])) or 'none'}")
    print(f"artifact={result.get('artifact_path', '')}")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Unified Hydra <-> NoClip rollout gate runner")
    parser.add_argument("--router-base", default=DEFAULT_ROUTER_BASE, help="Hydra router control-plane base URL")
    parser.add_argument("--overlay-ingress-url", default="", help="Optional live overlay ingress endpoint")
    parser.add_argument("--media-echo-url", default="", help="Optional live media echo endpoint")
    parser.add_argument("--endpoint-update-url", default="", help="Optional backend endpoint-update URL for auth-denial drill")
    parser.add_argument("--backend-health-url", default="", help="Optional NoClip backend /health URL")
    parser.add_argument("--da3-health-url", default="", help="Optional NoClip backend /api/da3/health URL")
    parser.add_argument("--timeout-seconds", type=float, default=DEFAULT_TIMEOUT_SECONDS, help="HTTP timeout per request")
    parser.add_argument("--max-stage-ms", type=int, default=DEFAULT_MAX_STAGE_MS, help="Per-stage latency SLO for smoke stages")
    parser.add_argument("--artifact", default="", help="Consolidated artifact path (JSON)")
    parser.add_argument("--simulate", action="store_true", help="Run deterministic simulation with no live HTTP dependencies")
    parser.add_argument("--strict-live", action="store_true", help="Fail smoke stages when optional overlay/media URLs are missing")
    parser.add_argument(
        "--require-backend-health",
        action="store_true",
        help="Fail when backend health URLs are missing or backend gate payloads are not ready",
    )
    parser.add_argument("--json", action="store_true", help="Print full JSON result")
    return parser


def _main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    result = run_rollout_gate(args)
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        _print_human(result)
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(_main())
