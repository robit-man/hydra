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
from uuid import uuid4

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
    headers: Optional[Dict[str, str]] = None,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
) -> Dict[str, Any]:
    body: Optional[bytes] = None
    request_headers: Dict[str, str] = {"Accept": "application/json"}
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        request_headers["Content-Type"] = "application/json"
    if isinstance(headers, dict):
        for key, value in headers.items():
            if key:
                request_headers[str(key)] = str(value)
    req = urllib.request.Request(url, method=method.upper(), data=body, headers=request_headers)
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
    required: bool = True

    def as_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "ok": bool(self.ok),
            "mode": self.mode,
            "duration_ms": int(self.duration_ms),
            "detail": self.detail if isinstance(self.detail, dict) else {},
            "error": str(self.error or ""),
            "required": bool(self.required),
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
            required=bool(required),
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
                required=True,
            )
        return GateCheck(
            name=name,
            ok=True,
            mode="skipped",
            duration_ms=_now_ms() - started,
            detail={"url": "", "required": False, "skipped": True},
            required=False,
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
        required=bool(required),
    )


def _derive_marketplace_gate_url(raw_url: str, backend_health_url: str) -> str:
    direct = str(raw_url or "").strip()
    if direct:
        return direct
    health = str(backend_health_url or "").strip()
    if not health:
        return ""
    if health.endswith("/health"):
        return f"{health[:-len('/health')]}/marketplace/gates"
    return ""


def _check_marketplace_rollout_gate(
    *,
    marketplace_gate_url: str,
    timeout_seconds: float,
    simulate: bool,
    required: bool,
) -> GateCheck:
    started = _now_ms()
    name = "marketplace_rollout_gate"
    if simulate:
        return GateCheck(
            name=name,
            ok=True,
            mode="simulated",
            duration_ms=_now_ms() - started,
            detail={
                "url": marketplace_gate_url,
                "contract_version_ok": True,
                "provider_catalog_integrity_ok": True,
                "quote_settlement_integrity_ok": True,
                "credit_ledger_invariants_ok": True,
                "ticket_replay_defense_ok": True,
                "fraud_controls_ready": True,
                "publication_enabled": True,
                "billing_enabled": True,
            },
            required=bool(required),
        )
    if not marketplace_gate_url:
        if required:
            return GateCheck(
                name=name,
                ok=False,
                mode="live",
                duration_ms=_now_ms() - started,
                detail={"url": "", "required": True},
                error="missing required marketplace gate URL",
                required=True,
            )
        return GateCheck(
            name=name,
            ok=True,
            mode="skipped",
            duration_ms=_now_ms() - started,
            detail={"url": "", "required": False, "skipped": True},
            required=False,
        )

    probe = _http_json("GET", marketplace_gate_url, timeout_seconds=timeout_seconds)
    payload = probe.get("json") if isinstance(probe.get("json"), dict) else {}
    gates = payload.get("rollout_gates") if isinstance(payload.get("rollout_gates"), dict) else {}
    contract_reported = str(payload.get("interop_contract_version") or "")
    contract_ok = bool(gates.get("contract_version_ok"))
    if contract_reported and INTEROP_CONTRACT_VERSION:
        contract_ok = contract_ok and (contract_reported == INTEROP_CONTRACT_VERSION)
    required_flags = (
        "provider_catalog_integrity_ok",
        "quote_settlement_integrity_ok",
        "credit_ledger_invariants_ok",
        "ticket_replay_defense_ok",
        "fraud_controls_ready",
        "publication_enabled",
        "billing_enabled",
    )
    stage_ok = bool(
        probe.get("ok")
        and int(probe.get("status") or 0) == 200
        and contract_ok
        and all(bool(gates.get(flag)) for flag in required_flags)
    )
    return GateCheck(
        name=name,
        ok=stage_ok,
        mode="live",
        duration_ms=_now_ms() - started,
        detail={
            "url": marketplace_gate_url,
            "http_status": int(probe.get("status") or 0),
            "contract_version_ok": contract_ok,
            "contract_version_reported": contract_reported,
            **{flag: bool(gates.get(flag)) for flag in required_flags},
            "gate_ready": bool(gates.get("ready")),
            "interop_contract_version_expected": INTEROP_CONTRACT_VERSION,
        },
        error="" if stage_ok else (probe.get("error") or "marketplace rollout gates are not ready"),
        required=bool(required),
    )


def _check_smoke_marketplace_stage(smoke_result: Dict[str, Any]) -> GateCheck:
    started = _now_ms()
    stages = smoke_result.get("stage_results") if isinstance(smoke_result.get("stage_results"), list) else []
    target = None
    for stage in stages:
        if not isinstance(stage, dict):
            continue
        if stage.get("name") == "marketplace_quote_settlement_integrity":
            target = stage
            break
    stage_ok = bool(isinstance(target, dict) and target.get("ok") is True)
    detail = target if isinstance(target, dict) else {"missing_stage": "marketplace_quote_settlement_integrity"}
    return GateCheck(
        name="smoke_marketplace_quote_settlement_stage",
        ok=stage_ok,
        mode="derived",
        duration_ms=_now_ms() - started,
        detail=detail if isinstance(detail, dict) else {},
        error="" if stage_ok else "interop smoke artifact missing passing marketplace quote/settlement stage",
        required=True,
    )


def _check_failure_drill_marketplace_guards(drill_result: Dict[str, Any]) -> GateCheck:
    started = _now_ms()
    drill_rows = drill_result.get("drill_results") if isinstance(drill_result.get("drill_results"), list) else []
    required_drills = {
        "credit_double_spend_guard",
        "ticket_replay_rejected",
        "fraudulent_rapid_fire_invocation_throttled",
    }
    seen: Dict[str, bool] = {}
    for row in drill_rows:
        if not isinstance(row, dict):
            continue
        name = str(row.get("name") or "")
        if name in required_drills:
            seen[name] = bool(row.get("ok"))
    stage_ok = bool(all(seen.get(name) for name in required_drills))
    missing = sorted([name for name in required_drills if name not in seen])
    failing = sorted([name for name, passed in seen.items() if not passed])
    return GateCheck(
        name="failure_drill_marketplace_guards",
        ok=stage_ok,
        mode="derived",
        duration_ms=_now_ms() - started,
        detail={
            "required_drills": sorted(required_drills),
            "missing": missing,
            "failing": failing,
        },
        error="" if stage_ok else "marketplace guard failure drills missing or failing",
        required=True,
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
    marketplace_gate_url = _derive_marketplace_gate_url(args.marketplace_gate_url, args.backend_health_url)
    smoke_args = SimpleNamespace(
        router_base=args.router_base,
        overlay_ingress_url=args.overlay_ingress_url,
        media_echo_url=args.media_echo_url,
        marketplace_quote_url=args.marketplace_quote_url,
        marketplace_settle_url=args.marketplace_settle_url,
        marketplace_gate_url=marketplace_gate_url,
        marketplace_auth_token=args.marketplace_auth_token,
        timeout_seconds=args.timeout_seconds,
        max_stage_ms=args.max_stage_ms,
        artifact=str(artifact_path),
        simulate=args.simulate,
        strict_live=args.strict_live,
    )
    return interop_smoke.run_smoke(smoke_args)


def _run_failure_drills(args: argparse.Namespace, artifact_path: Path) -> Dict[str, Any]:
    marketplace_gate_url = _derive_marketplace_gate_url(args.marketplace_gate_url, args.backend_health_url)
    drill_args = SimpleNamespace(
        endpoint_update_url=args.endpoint_update_url,
        marketplace_gate_url=marketplace_gate_url,
        timeout_seconds=args.timeout_seconds,
        artifact=str(artifact_path),
        simulate=args.simulate,
    )
    return failure_drill.run_drills(drill_args)


def run_rollout_gate(args: argparse.Namespace) -> Dict[str, Any]:
    started = _now_ms()
    trace_id = f"rollout-{time.strftime('%Y%m%d_%H%M%S', time.localtime())}-{uuid4().hex[:8]}"
    combined_path, smoke_path, drill_path = _resolve_artifact_paths(args.artifact)
    smoke_result = _run_smoke(args, smoke_path)
    drill_result = _run_failure_drills(args, drill_path)
    marketplace_gate_url = _derive_marketplace_gate_url(args.marketplace_gate_url, args.backend_health_url)

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
        _check_marketplace_rollout_gate(
            marketplace_gate_url=marketplace_gate_url,
            timeout_seconds=args.timeout_seconds,
            simulate=args.simulate,
            required=args.require_marketplace_gate,
        ),
        _check_smoke_marketplace_stage(smoke_result),
        _check_failure_drill_marketplace_guards(drill_result),
    ]

    failed_checks = []
    if not smoke_result.get("ok"):
        failed_checks.append("interop_smoke")
    if not drill_result.get("ok"):
        failed_checks.append("failure_drills")
    failed_checks.extend(check.name for check in gate_checks if check.required and not check.ok)
    required_total = len([check for check in gate_checks if check.required]) + 2
    required_passed = required_total - len(failed_checks)
    rollback_instructions = [
        "1. Freeze marketplace publication: PATCH /marketplace/admin/gates {publicationEnabled:false}.",
        "2. Freeze marketplace billing: PATCH /marketplace/admin/gates {billingEnabled:false}.",
        "3. Keep read-only diagnostics live (/health, /marketplace/gates) while rollback proceeds.",
        "4. Replay settlement journal and verify credit ledger invariants before re-opening billing.",
        "5. Re-run rollout gate in --simulate and live mode before restoring traffic.",
    ]

    payload = {
        "ok": len(failed_checks) == 0,
        "trace_id": trace_id,
        "mode": "simulated" if args.simulate else "live",
        "interop_contract_version_expected": INTEROP_CONTRACT_VERSION,
        "router_base": args.router_base,
        "marketplace_gate_url": marketplace_gate_url,
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
            "gate_checks_failed": len([check for check in gate_checks if check.required and not check.ok]),
            "gate_checks_total": len(gate_checks),
            "failed_checks_total": len(failed_checks),
            "required_checks_total": required_total,
            "required_checks_passed": required_passed,
        },
        "audit_bundle": {
            "trace_id": trace_id,
            "required_checks": [
                "interop_smoke",
                "failure_drills",
                *[check.name for check in gate_checks if check.required],
            ],
            "optional_checks": [check.name for check in gate_checks if not check.required],
            "failed_required_checks": failed_checks,
            "raw_errors": {
                "smoke_failed_stages": smoke_result.get("failed_stages", []),
                "drill_failed": drill_result.get("failed_drills", []),
                "gate_errors": [
                    {
                        "name": check.name,
                        "error": check.error,
                    }
                    for check in gate_checks
                    if check.error
                ],
            },
            "rollback_instructions": rollback_instructions,
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
        required = bool(check.get("required", True))
        req_tag = "required" if required else "optional"
        print(f"[{marker}] {check.get('name')} ({mode}, {req_tag}, {check.get('duration_ms', 0)}ms)")
        if check.get("error"):
            print(f"       error={check.get('error')}")
    if result.get("trace_id"):
        print(f"trace_id={result.get('trace_id')}")
    print(f"overall={result.get('ok')} failed_checks={','.join(result.get('failed_checks', [])) or 'none'}")
    print(f"artifact={result.get('artifact_path', '')}")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Unified Hydra <-> NoClip rollout gate runner")
    parser.add_argument("--router-base", default=DEFAULT_ROUTER_BASE, help="Hydra router control-plane base URL")
    parser.add_argument("--overlay-ingress-url", default="", help="Optional live overlay ingress endpoint")
    parser.add_argument("--media-echo-url", default="", help="Optional live media echo endpoint")
    parser.add_argument("--marketplace-quote-url", default="", help="Optional live quote endpoint")
    parser.add_argument("--marketplace-settle-url", default="", help="Optional live settlement endpoint template")
    parser.add_argument("--marketplace-gate-url", default="", help="Optional marketplace rollout gate endpoint")
    parser.add_argument("--marketplace-auth-token", default="", help="Optional bearer token for marketplace quote/settle probes")
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
    parser.add_argument(
        "--require-marketplace-gate",
        action="store_true",
        help="Fail when marketplace rollout gate URL is missing or required marketplace gates are not ready",
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
