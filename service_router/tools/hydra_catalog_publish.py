#!/usr/bin/env python3
"""Manual publisher for Hydra router marketplace catalog sync endpoint."""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request
from typing import Any, Dict, List


def _split_targets(values: List[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for raw in values:
        for part in str(raw or "").replace("\n", ",").split(","):
            text = part.strip()
            if not text or text in seen:
                continue
            seen.add(text)
            out.append(text)
    return out


def _request_json(url: str, payload: Dict[str, Any], timeout_s: float) -> Dict[str, Any]:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        method="POST",
        data=data,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            raw = resp.read()
            status = int(resp.getcode() or 0)
    except urllib.error.HTTPError as exc:
        status = int(exc.code or 0)
        raw = exc.read()
    except urllib.error.URLError as exc:
        return {
            "_http_status": 0,
            "status": "error",
            "error": f"connection_error: {exc.reason}",
            "result": {
                "ok": False,
                "error": f"connection_error: {exc.reason}",
            },
        }
    body: Dict[str, Any]
    try:
        parsed = json.loads(raw.decode("utf-8")) if raw else {}
        body = parsed if isinstance(parsed, dict) else {"raw": parsed}
    except Exception:
        body = {"raw": raw.decode("utf-8", errors="replace")}
    body["_http_status"] = status
    return body


def _request_get_json(url: str, timeout_s: float) -> Dict[str, Any]:
    req = urllib.request.Request(
        url,
        method="GET",
        headers={
            "Accept": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            raw = resp.read()
            status = int(resp.getcode() or 0)
    except urllib.error.HTTPError as exc:
        status = int(exc.code or 0)
        raw = exc.read()
    except urllib.error.URLError as exc:
        return {
            "_http_status": 0,
            "status": "error",
            "error": f"connection_error: {exc.reason}",
        }
    body: Dict[str, Any]
    try:
        parsed = json.loads(raw.decode("utf-8")) if raw else {}
        body = parsed if isinstance(parsed, dict) else {"raw": parsed}
    except Exception:
        body = {"raw": raw.decode("utf-8", errors="replace")}
    body["_http_status"] = status
    return body


def parse_args(argv: List[str]) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Trigger Hydra router catalog publication to marketplace targets.")
    ap.add_argument("--router-base", default="http://127.0.0.1:9071", help="Hydra router base URL")
    ap.add_argument(
        "--target-url",
        dest="target_urls",
        action="append",
        default=[],
        help="Target ingest URL (repeatable, supports comma-separated values)",
    )
    ap.add_argument("--timeout-s", type=float, default=8.0, help="HTTP timeout for router call")
    ap.add_argument("--publish-timeout-s", type=float, default=6.0, help="Per-target publish timeout")
    ap.add_argument("--publish-nats", action="store_true", help="Also publish catalog/status envelopes via NATS")
    ap.add_argument("--nats-only", action="store_true", help="Publish only over NATS (skip HTTP target URLs)")
    ap.add_argument("--nats-timeout-s", type=float, default=3.0, help="NATS publish timeout")
    ap.add_argument("--nats-catalog-subject", default="", help="Override NATS catalog subject")
    ap.add_argument("--nats-status-subject", default="", help="Override NATS status subject")
    ap.add_argument("--skip-status", action="store_true", help="Publish catalog event only (skip status event)")
    ap.add_argument("--auth-token", default="", help="Optional per-request auth token override")
    ap.add_argument("--include-unhealthy", action="store_true", help="Include unhealthy services in payload")
    ap.add_argument("--dry-run", action="store_true", help="Build payload without sending target HTTP requests")
    ap.add_argument("--force-refresh", action="store_true", help="Force fresh router snapshot before publish")
    ap.add_argument("--diagnostics", action="store_true", help="Fetch router marketplace sync diagnostics and exit")
    ap.add_argument("--json", action="store_true", help="Emit full JSON response")
    return ap.parse_args(argv)


def main(argv: List[str]) -> int:
    args = parse_args(argv)
    base = str(args.router_base or "http://127.0.0.1:9071").rstrip("/")
    if args.diagnostics:
        diag = _request_get_json(f"{base}/marketplace/sync", timeout_s=max(1.0, args.timeout_s))
        if args.json:
            sys.stdout.write(json.dumps(diag, indent=2) + "\n")
        else:
            status = int(diag.get("_http_status") or 0)
            nats = diag.get("nats") if isinstance(diag.get("nats"), dict) else {}
            nats_state = nats.get("state") if isinstance(nats.get("state"), dict) else {}
            summary = {
                "http_status": status,
                "status": diag.get("status"),
                "http_in_flight": bool((diag.get("state") or {}).get("in_flight")) if isinstance(diag.get("state"), dict) else False,
                "nats_enabled": bool((nats.get("config") or {}).get("enabled")) if isinstance(nats.get("config"), dict) else False,
                "nats_connected": bool(nats_state.get("connected")),
                "nats_publish_count": int(nats_state.get("publish_count") or 0),
                "nats_receive_count": int(nats_state.get("receive_count") or 0),
                "remote_catalog_count": int((nats.get("remote_catalogs") or {}).get("count") or 0)
                if isinstance(nats.get("remote_catalogs"), dict)
                else 0,
            }
            sys.stdout.write("Hydra Marketplace Sync Diagnostics\n")
            sys.stdout.write(json.dumps(summary, indent=2) + "\n")
        diag_status = int(diag.get("_http_status") or 0)
        return 0 if 200 <= diag_status < 400 else 1

    url = f"{base}/marketplace/catalog/publish"
    targets = _split_targets(args.target_urls)
    publish_http = not bool(args.nats_only)
    publish_nats = bool(args.publish_nats or args.nats_only)
    payload: Dict[str, Any] = {
        "publish_http": publish_http,
        "publish_nats": publish_nats,
        "target_urls": targets,
        "timeout_seconds": float(max(1.0, args.publish_timeout_s)),
        "dry_run": bool(args.dry_run),
        "force_refresh": bool(args.force_refresh),
        "include_unhealthy": bool(args.include_unhealthy),
    }
    if args.auth_token:
        payload["auth_token"] = str(args.auth_token)
    if publish_nats:
        payload["nats"] = {
            "timeout_seconds": float(max(0.5, args.nats_timeout_s)),
            "include_catalog": True,
            "include_status": not bool(args.skip_status),
            "catalog_subject": str(args.nats_catalog_subject or "").strip(),
            "status_subject": str(args.nats_status_subject or "").strip(),
        }

    response = _request_json(url, payload, timeout_s=max(1.0, args.timeout_s))
    status = int(response.get("_http_status") or 0)
    result = response.get("result") if isinstance(response.get("result"), dict) else {}
    http_result = response.get("http_result") if isinstance(response.get("http_result"), dict) else {}
    nats_result = response.get("nats_result") if isinstance(response.get("nats_result"), dict) else {}
    selected = []
    if publish_http:
        selected.append(http_result if http_result else result)
    if publish_nats:
        selected.append(nats_result if nats_result else ({} if publish_http else result))
    selected_ok = [bool(item.get("ok")) for item in selected if isinstance(item, dict) and item]
    ok = bool(200 <= status < 300 and (not selected_ok or all(selected_ok)))

    if args.json:
        sys.stdout.write(json.dumps(response, indent=2) + "\n")
    else:
        summary = {
            "http_status": status,
            "status": response.get("status"),
            "ok": ok,
            "publish_http": publish_http,
            "publish_nats": publish_nats,
            "http_attempted": int((http_result or result).get("attempted") or 0) if publish_http else 0,
            "http_sent": int((http_result or result).get("sent") or 0) if publish_http else 0,
            "http_failed": int((http_result or result).get("failed") or 0) if publish_http else 0,
            "nats_attempted": int(nats_result.get("attempted") or 0) if publish_nats else 0,
            "nats_sent": int(nats_result.get("sent") or 0) if publish_nats else 0,
            "nats_failed": int(nats_result.get("failed") or 0) if publish_nats else 0,
            "error": response.get("error") or result.get("error") or "",
            "dry_run": bool(result.get("dry_run") or nats_result.get("dry_run")),
        }
        sys.stdout.write("Hydra Catalog Publish\n")
        sys.stdout.write(json.dumps(summary, indent=2) + "\n")
        details = (http_result or result).get("results") if isinstance((http_result or result).get("results"), list) else []
        if details:
            sys.stdout.write("HTTP Targets:\n")
            for item in details:
                if not isinstance(item, dict):
                    continue
                sys.stdout.write(
                    "- {url} ok={ok} status={status} latency_ms={latency} error={error}\n".format(
                        url=item.get("url") or "",
                        ok=bool(item.get("ok")),
                        status=int(item.get("status") or 0),
                        latency=item.get("latency_ms") or 0,
                        error=item.get("error") or "",
                    )
                )
        if publish_nats:
            nats_details = nats_result.get("results") if isinstance(nats_result.get("results"), list) else []
            if nats_details:
                sys.stdout.write("NATS Events:\n")
                for item in nats_details:
                    if not isinstance(item, dict):
                        continue
                    sys.stdout.write(
                        "- {event} -> {subject} ok={ok} status={status} latency_ms={latency} error={error}\n".format(
                            event=item.get("event") or "",
                            subject=item.get("subject") or "",
                            ok=bool(item.get("ok")),
                            status=int(item.get("status") or 0),
                            latency=item.get("latency_ms") or 0,
                            error=item.get("error") or "",
                        )
                    )

    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
