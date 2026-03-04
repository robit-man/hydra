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
    ap.add_argument("--auth-token", default="", help="Optional per-request auth token override")
    ap.add_argument("--include-unhealthy", action="store_true", help="Include unhealthy services in payload")
    ap.add_argument("--dry-run", action="store_true", help="Build payload without sending target HTTP requests")
    ap.add_argument("--force-refresh", action="store_true", help="Force fresh router snapshot before publish")
    ap.add_argument("--json", action="store_true", help="Emit full JSON response")
    return ap.parse_args(argv)


def main(argv: List[str]) -> int:
    args = parse_args(argv)
    base = str(args.router_base or "http://127.0.0.1:9071").rstrip("/")
    url = f"{base}/marketplace/catalog/publish"
    targets = _split_targets(args.target_urls)
    payload: Dict[str, Any] = {
        "target_urls": targets,
        "timeout_seconds": float(max(1.0, args.publish_timeout_s)),
        "dry_run": bool(args.dry_run),
        "force_refresh": bool(args.force_refresh),
        "include_unhealthy": bool(args.include_unhealthy),
    }
    if args.auth_token:
        payload["auth_token"] = str(args.auth_token)

    response = _request_json(url, payload, timeout_s=max(1.0, args.timeout_s))
    status = int(response.get("_http_status") or 0)
    result = response.get("result") if isinstance(response.get("result"), dict) else {}
    ok = bool(result.get("ok")) and 200 <= status < 300

    if args.json:
        sys.stdout.write(json.dumps(response, indent=2) + "\n")
    else:
        summary = {
            "http_status": status,
            "status": response.get("status"),
            "ok": bool(result.get("ok")),
            "attempted": int(result.get("attempted") or 0),
            "sent": int(result.get("sent") or 0),
            "failed": int(result.get("failed") or 0),
            "error": result.get("error") or response.get("error") or "",
            "dry_run": bool(result.get("dry_run")),
        }
        sys.stdout.write("Hydra Catalog Publish\n")
        sys.stdout.write(json.dumps(summary, indent=2) + "\n")
        details = result.get("results") if isinstance(result.get("results"), list) else []
        if details:
            sys.stdout.write("Targets:\n")
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

    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
