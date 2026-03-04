#!/usr/bin/env python3
"""Probe Hydra marketplace catalog completeness and endpoint reachability."""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Tuple


def _http_json(url: str, *, timeout_s: float = 6.0) -> Dict[str, Any]:
    req = urllib.request.Request(url, method="GET", headers={"Accept": "application/json"})
    started = time.time()
    try:
        with urllib.request.urlopen(req, timeout=max(1.0, float(timeout_s))) as resp:
            raw = resp.read()
            status = int(resp.getcode() or 0)
        latency_ms = round((time.time() - started) * 1000.0, 2)
        body = json.loads(raw.decode("utf-8")) if raw else {}
        if not isinstance(body, dict):
            body = {"raw": body}
        return {"ok": True, "status": status, "json": body, "error": "", "latency_ms": latency_ms}
    except urllib.error.HTTPError as exc:
        latency_ms = round((time.time() - started) * 1000.0, 2)
        text = ""
        try:
            text = exc.read().decode("utf-8", errors="replace")
        except Exception:
            text = ""
        payload: Dict[str, Any] = {}
        if text:
            try:
                parsed = json.loads(text)
                if isinstance(parsed, dict):
                    payload = parsed
                else:
                    payload = {"raw": parsed}
            except Exception:
                payload = {"raw": text}
        return {"ok": False, "status": int(exc.code or 0), "json": payload, "error": f"HTTPError {exc.code}", "latency_ms": latency_ms}
    except Exception as exc:
        latency_ms = round((time.time() - started) * 1000.0, 2)
        return {"ok": False, "status": 0, "json": {}, "error": f"{type(exc).__name__}: {exc}", "latency_ms": latency_ms}


def _is_nkn_candidate(value: str) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    if text.startswith("nkn://"):
        return True
    if "://" in text:
        return False
    if any(ch in text for ch in ("/", "?", "#", " ")):
        return False
    return bool(re.match(r"^[a-zA-Z0-9._-]{8,256}$", text))


def _http_candidate_reachable(url: str, timeout_s: float = 2.5) -> bool:
    value = str(url or "").strip()
    if not value:
        return False
    parsed = urllib.parse.urlparse(value)
    if parsed.scheme not in {"http", "https"}:
        return False
    probe_urls: List[str] = []
    base = value.rstrip("/")
    probe_urls.append(base + "/health")
    probe_urls.append(base + "/healthz")
    probe_urls.append(base)
    for probe in probe_urls:
        result = _http_json(probe, timeout_s=timeout_s)
        if result.get("ok") and int(result.get("status") or 0) < 500:
            return True
        if int(result.get("status") or 0) in {401, 403}:
            return True
    return False


def _candidate_reachable(transport: str, endpoint: str, timeout_s: float) -> bool:
    key = str(transport or "").strip().lower()
    value = str(endpoint or "").strip()
    if not value:
        return False
    if key == "nkn":
        return _is_nkn_candidate(value)
    if key in {"cloudflare", "local", "upnp", "nats"}:
        if value.startswith("http://") or value.startswith("https://"):
            return _http_candidate_reachable(value, timeout_s=timeout_s)
        return True
    if value.startswith("http://") or value.startswith("https://"):
        return _http_candidate_reachable(value, timeout_s=timeout_s)
    return bool(value)


def _resolved_map(snapshot_payload: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    snapshot = snapshot_payload.get("snapshot") if isinstance(snapshot_payload.get("snapshot"), dict) else {}
    resolved = snapshot.get("resolved") if isinstance(snapshot.get("resolved"), dict) else {}
    out: Dict[str, Dict[str, Any]] = {}
    for name, item in resolved.items():
        if isinstance(item, dict):
            out[str(name)] = item
    return out


def _catalog_services(catalog_payload: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    services = catalog_payload.get("services") if isinstance(catalog_payload.get("services"), list) else []
    out: Dict[str, Dict[str, Any]] = {}
    for item in services:
        if not isinstance(item, dict):
            continue
        service_id = str(item.get("service_id") or item.get("service") or "").strip()
        if not service_id:
            continue
        out[service_id] = item
    return out


def _print_line(text: str) -> None:
    sys.stdout.write(text + "\n")


def run(args: argparse.Namespace) -> int:
    base = str(args.router_base or "http://127.0.0.1:9071").rstrip("/")
    refresh_q = "1" if args.refresh else "0"
    catalog_url = f"{base}/marketplace/catalog?refresh={refresh_q}&include_unhealthy=1"
    snapshot_url = f"{base}/services/snapshot?refresh={refresh_q}"

    catalog_probe = _http_json(catalog_url, timeout_s=args.timeout_s)
    snapshot_probe = _http_json(snapshot_url, timeout_s=args.timeout_s)
    failures: List[str] = []
    warnings: List[str] = []

    if not catalog_probe.get("ok"):
        failures.append(f"catalog endpoint request failed: {catalog_probe.get('error')}")
    if not snapshot_probe.get("ok"):
        failures.append(f"snapshot endpoint request failed: {snapshot_probe.get('error')}")

    catalog_json = catalog_probe.get("json") if isinstance(catalog_probe.get("json"), dict) else {}
    snapshot_json = snapshot_probe.get("json") if isinstance(snapshot_probe.get("json"), dict) else {}

    if str(catalog_json.get("status") or "").lower() != "success":
        failures.append(f"catalog status expected 'success', got {catalog_json.get('status')!r}")
    if str(snapshot_json.get("status") or "").lower() != "success":
        failures.append(f"snapshot status expected 'success', got {snapshot_json.get('status')!r}")

    provider = catalog_json.get("provider") if isinstance(catalog_json.get("provider"), dict) else {}
    provider_nkn = str(provider.get("router_nkn") or "").strip()
    provider_fp = str(provider.get("provider_key_fingerprint") or "").strip()
    if not provider_nkn:
        warnings.append("provider.router_nkn missing")
    if not provider_fp:
        warnings.append("provider.provider_key_fingerprint missing")

    services = _catalog_services(catalog_json)
    resolved = _resolved_map(snapshot_json)
    if not services:
        failures.append("catalog.services is empty")

    healthy_services = 0
    healthy_with_reachable = 0
    consistency_matches = 0

    for service_id, entry in sorted(services.items()):
        healthy = bool(entry.get("healthy"))
        enabled = bool(entry.get("enabled", True))
        if healthy and enabled:
            healthy_services += 1
        selected_transport = str(entry.get("selected_transport") or "").strip().lower()
        endpoint_candidates = entry.get("endpoint_candidates") if isinstance(entry.get("endpoint_candidates"), dict) else {}
        reachability = {}
        for transport in ("cloudflare", "nkn", "local", "upnp", "nats"):
            endpoint = str(endpoint_candidates.get(transport) or "").strip()
            reachability[transport] = _candidate_reachable(transport, endpoint, timeout_s=args.endpoint_timeout_s)

        if healthy and enabled:
            if any(reachability.values()):
                healthy_with_reachable += 1
            else:
                failures.append(f"{service_id}: healthy service has no reachable endpoint candidates")

        if selected_transport and not str(endpoint_candidates.get(selected_transport) or "").strip():
            failures.append(f"{service_id}: selected_transport={selected_transport} missing candidate endpoint")

        resolved_entry = resolved.get(service_id) if isinstance(resolved.get(service_id), dict) else {}
        if not resolved_entry:
            failures.append(f"{service_id}: missing from /services/snapshot resolved map")
            continue
        resolved_transport = str(
            resolved_entry.get("selected_transport")
            or resolved_entry.get("transport")
            or ""
        ).strip().lower()
        if selected_transport and resolved_transport and selected_transport != resolved_transport:
            failures.append(
                f"{service_id}: catalog selected_transport={selected_transport} != snapshot selected_transport={resolved_transport}"
            )
        else:
            consistency_matches += 1

    summary = {
        "catalog_http_status": int(catalog_probe.get("status") or 0),
        "catalog_latency_ms": float(catalog_probe.get("latency_ms") or 0.0),
        "snapshot_http_status": int(snapshot_probe.get("status") or 0),
        "snapshot_latency_ms": float(snapshot_probe.get("latency_ms") or 0.0),
        "service_count": len(services),
        "healthy_services": healthy_services,
        "healthy_with_reachable_candidate": healthy_with_reachable,
        "consistency_matches": consistency_matches,
        "warning_count": len(warnings),
        "failure_count": len(failures),
    }

    _print_line("Hydra Catalog Probe")
    _print_line(json.dumps(summary, indent=2))
    if warnings:
        _print_line("Warnings:")
        for item in warnings:
            _print_line(f"- {item}")
    if failures:
        _print_line("Failures:")
        for item in failures:
            _print_line(f"- {item}")

    if failures:
        return 1
    if args.strict and warnings:
        return 2
    return 0


def parse_args(argv: List[str]) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Validate Hydra /marketplace/catalog completeness and consistency.")
    ap.add_argument("--router-base", default="http://127.0.0.1:9071", help="Router control-plane base URL")
    ap.add_argument("--timeout-s", type=float, default=8.0, help="HTTP timeout for control-plane probes")
    ap.add_argument("--endpoint-timeout-s", type=float, default=2.5, help="HTTP timeout for endpoint candidate probes")
    ap.add_argument("--refresh", action="store_true", help="Force fresh snapshot/catalog refresh")
    ap.add_argument("--strict", action="store_true", help="Treat warnings as failure (exit code 2)")
    return ap.parse_args(argv)


def main(argv: List[str]) -> int:
    args = parse_args(argv)
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
