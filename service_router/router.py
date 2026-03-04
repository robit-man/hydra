#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Unified NKN router — multiplex Whisper ASR, Piper TTS, and Ollama over multiple NKN identities.

Features
- Spins up any number of NKN bridge sidecars (redundant addresses) from a single process
- Resilient Node.js bridge watcher with durable DM queue and exponential restart backoff
- Shared HTTP worker pool per node with streaming support (NDJSON lines, SSE, base64 chunks)
- Service helpers covering asr.* events and generic relay.http requests
- Curses dashboard listing all active addresses, queue depth, and recent activity
"""

import argparse
import atexit
import base64
import codecs
import contextlib
import hashlib
import hmac
import json
import logging
import math
import os
import queue
import re
import secrets
import shlex
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.parse
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Deque, Dict, IO, List, Optional, Tuple
from collections import deque

# ──────────────────────────────────────────────────────────────
# Lightweight venv bootstrap so the router stays self-contained
# ──────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
REPO_ROOT = BASE_DIR.parent  # hydra repo root (contains .git)
VENV_DIR = BASE_DIR / ".venv_router"
BIN_DIR = VENV_DIR / ("Scripts" if os.name == "nt" else "bin")
PY_BIN = BIN_DIR / ("python.exe" if os.name == "nt" else "python")
PIP_BIN = BIN_DIR / ("pip.exe" if os.name == "nt" else "pip")


def _skip_bootstrap() -> bool:
    if os.environ.get("HYDRA_ROUTER_SKIP_BOOTSTRAP", "").strip().lower() in {"1", "true", "yes", "on"}:
        return True
    if os.environ.get("PYTEST_CURRENT_TEST"):
        return True
    return "pytest" in sys.modules


def _in_venv() -> bool:
    try:
        return Path(sys.executable).resolve() == PY_BIN.resolve()
    except Exception:
        return False


def _ensure_venv() -> None:
    if VENV_DIR.exists():
        return
    import venv

    venv.EnvBuilder(with_pip=True).create(VENV_DIR)
    subprocess.check_call([str(PY_BIN), "-m", "pip", "install", "--upgrade", "pip"], cwd=BASE_DIR)


def _ensure_deps() -> None:
    need = []
    for mod in ("requests", "python-dotenv", "qrcode", "flask", "werkzeug"):
        try:
            if mod == "python-dotenv":
                __import__("dotenv")
            else:
                __import__(mod)
        except Exception:
            need.append(mod)
    if need:
        subprocess.check_call([str(PIP_BIN), "install", *need], cwd=BASE_DIR)


BOOTSTRAP_SKIPPED = _skip_bootstrap()

if not BOOTSTRAP_SKIPPED and not _in_venv():
    _ensure_venv()
    os.execv(str(PY_BIN), [str(PY_BIN), *sys.argv])

if not BOOTSTRAP_SKIPPED:
    _ensure_deps()

import requests  # type: ignore
try:
    import qrcode  # type: ignore
except Exception:  # pragma: no cover
    qrcode = None  # type: ignore
from cloudflared_manager import CloudflaredManager  # type: ignore

try:
    import curses  # type: ignore
except Exception:  # pragma: no cover
    curses = None

# ──────────────────────────────────────────────────────────────
# Configuration bootstrap
# ──────────────────────────────────────────────────────────────
CONFIG_PATH = BASE_DIR / "router_config.json"
STATE_CONFIG_PATH = BASE_DIR / ".router_config.json"
DEFAULT_TARGETS = {
    "ollama": "http://127.0.0.1:11434",
    "asr": "http://127.0.0.1:8126",
    "tts": "http://127.0.0.1:8123",
    "mcp": "http://127.0.0.1:9003",
    "web_scrape": "http://127.0.0.1:8130",
    "depth_any": "http://127.0.0.1:5000",
}
ROUTER_CONFIG_SCHEMA_VERSION = 2
ROUTER_CONFIG_MIGRATION_MAP = {
    0: "legacy_flat_config",
    1: "nested_schema_v1",
    2: "typed_schema_v2",
}
INTEROP_CONTRACT = {
    "name": "hydra_noclip_interop",
    "version": "1.0.0",
    "compat_min_version": "1.0.0",
    "namespace": "hydra.noclip.marketplace.v1",
    "schema": "hydra_noclip_marketplace_contract_v1",
}
DEFAULT_TICKET_KID = "noclip-dev-k1"
DEFAULT_TICKET_SECRET = "dev-hydra-noclip-shared-ticket-key-change-me-0123456789abcdef"
AUTH_ALLOWED_SCOPES = {"infer", "overlay.write", "stream"}
AUTH_ALLOWED_AUDIENCE = {"public", "friends", "private"}

ROUTER_SECTION_FIELD_SPECS: Dict[str, Dict[str, Dict[str, Any]]] = {
    "api": {
        "enable": {"type": "bool", "default": True, "legacy": ("enable_api", "api_enable")},
        "host": {"type": "str", "default": "127.0.0.1", "legacy": ("listen_host", "api_host")},
        "port": {"type": "int", "default": 9071, "min": 1, "max": 65535, "legacy": ("listen_port", "api_port")},
    },
    "nkn": {
        "enable": {"type": "bool", "default": True, "legacy": ("enable_nkn", "nkn_enable")},
        "dm_retries": {"type": "int", "default": 2, "min": 1, "max": 12},
        "resolve_timeout_seconds": {"type": "int", "default": 20, "min": 2, "max": 120, "legacy": ("resolve_timeout",)},
        "rpc_max_request_b": {"type": "int", "default": 512 * 1024, "min": 1024, "max": 64 * 1024 * 1024},
        "rpc_max_response_b": {"type": "int", "default": 2 * 1024 * 1024, "min": 1024, "max": 128 * 1024 * 1024},
    },
    "http": {
        "workers": {"type": "int", "default": 4, "min": 1, "max": 128, "legacy": ("http_workers",)},
        "max_body_b": {"type": "int", "default": 2 * 1024 * 1024, "min": 4096, "max": 256 * 1024 * 1024},
        "verify_default": {"type": "bool", "default": True},
        "chunk_raw_b": {"type": "int", "default": 12 * 1024, "min": 1024, "max": 4 * 1024 * 1024},
        "chunk_upload_b": {"type": "int", "default": 600 * 1024, "min": 4 * 1024, "max": 16 * 1024 * 1024},
        "heartbeat_s": {"type": "int", "default": 10, "min": 2, "max": 300},
        "batch_lines": {"type": "int", "default": 24, "min": 1, "max": 1024},
        "batch_latency": {"type": "float", "default": 0.08, "min": 0.01, "max": 30.0},
        "retries": {"type": "int", "default": 4, "min": 0, "max": 20},
        "retry_backoff": {"type": "float", "default": 0.5, "min": 0.0, "max": 30.0},
        "retry_cap": {"type": "float", "default": 4.0, "min": 0.1, "max": 120.0},
    },
    "bridge": {
        "num_subclients": {"type": "int", "default": 2, "min": 1, "max": 32, "legacy": ("subclients",)},
        "seed_ws": {"type": "str", "default": ""},
        "self_probe_ms": {"type": "int", "default": 12000, "min": 1000, "max": 300000},
        "self_probe_fails": {"type": "int", "default": 3, "min": 1, "max": 20},
    },
    "watchdog": {
        "port_reclaim_enabled": {"type": "bool", "default": True},
        "port_reclaim_force": {"type": "bool", "default": False},
        "activation_timeout_seconds": {"type": "float", "default": 30.0, "min": 1.0, "max": 600.0},
        "activation_stability_seconds": {"type": "float", "default": 6.0, "min": 0.5, "max": 120.0},
        "health_check_interval_seconds": {"type": "float", "default": 2.0, "min": 0.2, "max": 120.0},
        "health_failure_threshold": {"type": "int", "default": 3, "min": 1, "max": 20},
        "reclaim_sigint_wait_seconds": {"type": "float", "default": 0.7, "min": 0.0, "max": 30.0},
        "reclaim_sigterm_wait_seconds": {"type": "float", "default": 1.0, "min": 0.0, "max": 30.0},
        "reclaim_sigkill_wait_seconds": {"type": "float", "default": 1.0, "min": 0.0, "max": 30.0},
    },
    "cloudflared": {
        "enable": {"type": "bool", "default": False, "legacy": ("enable_tunnel", "cloudflared_enable")},
        "auto_install_cloudflared": {"type": "bool", "default": False},
        "binary_path": {"type": "str", "default": ""},
        "protocol": {"type": "str", "default": "http2"},
        "restart_initial_seconds": {"type": "float", "default": 2.0, "min": 0.5, "max": 300.0},
        "restart_cap_seconds": {"type": "float", "default": 90.0, "min": 1.0, "max": 3600.0},
    },
    "feature_flags": {
        "router_control_plane_api": {"type": "bool", "default": True},
        "resolver_auto_apply": {"type": "bool", "default": True},
        "cloudflared_manager": {"type": "bool", "default": False},
    },
    "marketplace": {
        "enable_catalog": {"type": "bool", "default": True},
        "provider_id": {"type": "str", "default": "hydra-router"},
        "provider_label": {"type": "str", "default": "Hydra Router"},
        "provider_network": {"type": "str", "default": "hydra"},
        "provider_contact": {"type": "str", "default": ""},
        "default_currency": {"type": "str", "default": "USDC"},
        "default_unit": {"type": "str", "default": "request"},
        "default_price_per_unit": {"type": "float", "default": 0.0, "min": 0.0, "max": 1000000.0},
        "include_unhealthy": {"type": "bool", "default": True},
        "catalog_ttl_seconds": {"type": "int", "default": 20, "min": 2, "max": 600},
    },
    "marketplace_sync": {
        "enable_auto_publish": {"type": "bool", "default": False},
        "target_urls": {"type": "str", "default": ""},
        "auth_token": {"type": "str", "default": ""},
        "auth_token_env": {"type": "str", "default": "HYDRA_MARKETPLACE_SYNC_TOKEN"},
        "auth_header": {"type": "str", "default": "Authorization"},
        "auth_scheme": {"type": "str", "default": "Bearer"},
        "publish_interval_seconds": {"type": "int", "default": 45, "min": 5, "max": 3600},
        "publish_timeout_seconds": {"type": "float", "default": 6.0, "min": 1.0, "max": 120.0},
        "max_backoff_seconds": {"type": "int", "default": 300, "min": 5, "max": 7200},
        "include_unhealthy": {"type": "bool", "default": True},
    },
    "auth": {
        "require_service_rpc_ticket": {"type": "bool", "default": True},
        "require_billing_preflight": {"type": "bool", "default": True},
        "require_quote_for_billable": {"type": "bool", "default": True},
        "require_charge_for_billable": {"type": "bool", "default": True},
        "allow_unauthenticated_resolve": {"type": "bool", "default": True},
        "clock_skew_seconds": {"type": "int", "default": 30, "min": 0, "max": 300},
        "replay_cache_seconds": {"type": "int", "default": 900, "min": 30, "max": 3600},
        "ticket_ttl_seconds": {"type": "int", "default": 300, "min": 30, "max": 1800},
        "active_kid": {"type": "str", "default": DEFAULT_TICKET_KID},
        "default_scope": {"type": "str", "default": "infer"},
    },
}

ROUTER_TARGET_LEGACY_KEYS: Dict[str, Tuple[str, ...]] = {
    "ollama": ("ollama_url", "ollama_endpoint", "ollama_base"),
    "asr": ("asr_url", "whisper_url", "asr_endpoint"),
    "tts": ("tts_url", "piper_url", "tts_endpoint"),
    "mcp": ("mcp_url", "mcp_endpoint"),
    "web_scrape": ("web_scrape_url", "browser_url", "scrape_url"),
    "depth_any": ("depth_any_url", "depth_url", "pointcloud_url"),
}
SENSITIVE_VALUE_PLACEHOLDER = "[redacted]"
SENSITIVE_FIELD_EXACT = {
    "seed_hex",
    "seed",
    "password",
    "passphrase",
    "api_key",
    "apikey",
    "token",
    "access_token",
    "refresh_token",
    "secret",
    "client_secret",
    "private_key",
    "authorization",
    "auth_header",
    "bearer",
}
SENSITIVE_FIELD_EXCEPTIONS = {
    "seed_persisted",
}
SENSITIVE_FIELD_REGEX = re.compile(
    r"(seed_hex|(?:^|_)seed(?:$|_)|password|passphrase|api_?key|token|secret|private_?key|authorization|auth_header|bearer)"
)


def _parse_semver_tuple(value: Any) -> Tuple[int, int, int]:
    text = str(value or "").strip()
    if not text:
        return (0, 0, 0)
    m = re.match(r"^v?(?P<major>\d+)(?:\.(?P<minor>\d+))?(?:\.(?P<patch>\d+))?", text, re.IGNORECASE)
    if not m:
        return (0, 0, 0)
    major = int(m.group("major") or 0)
    minor = int(m.group("minor") or 0)
    patch = int(m.group("patch") or 0)
    return (major, minor, patch)


def _semver_gte(left: Any, right: Any) -> bool:
    return _parse_semver_tuple(left) >= _parse_semver_tuple(right)


def _base64url_decode(value: Any) -> bytes:
    text = str(value or "").strip()
    if not text:
        return b""
    pad = "=" * ((4 - (len(text) % 4)) % 4)
    return base64.urlsafe_b64decode((text + pad).encode("ascii", errors="ignore"))


def _base64url_encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode("ascii").rstrip("=")


def _normalize_scope(value: Any, default: str = "infer") -> str:
    raw = str(value or "").strip().lower()
    if raw in {"asr", "tts", "llm", "request"}:
        raw = "infer"
    if raw in AUTH_ALLOWED_SCOPES:
        return raw
    return default if default in AUTH_ALLOWED_SCOPES else "infer"


def _normalize_audience(value: Any, default: str = "public") -> str:
    raw = str(value or "").strip().lower()
    if raw == "friend":
        raw = "friends"
    if raw in AUTH_ALLOWED_AUDIENCE:
        return raw
    return default if default in AUTH_ALLOWED_AUDIENCE else "public"


def _normalize_ticket_key_map(raw_keys: Any, fallback: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    src = raw_keys if isinstance(raw_keys, dict) else {}
    defaults = fallback if isinstance(fallback, dict) else {}
    out: Dict[str, str] = {}
    for kid, secret in src.items():
        kid_text = str(kid or "").strip()
        secret_text = str(secret or "").strip()
        if not kid_text or not secret_text:
            continue
        out[kid_text] = secret_text
    if out:
        return out
    default_out: Dict[str, str] = {}
    for kid, secret in defaults.items():
        kid_text = str(kid or "").strip()
        secret_text = str(secret or "").strip()
        if not kid_text or not secret_text:
            continue
        default_out[kid_text] = secret_text
    if default_out:
        return default_out
    return {DEFAULT_TICKET_KID: DEFAULT_TICKET_SECRET}


def _normalize_ticket_kid_list(raw: Any, fallback: Optional[List[str]] = None) -> List[str]:
    values: List[str] = []
    if isinstance(raw, list):
        values = [str(item or "").strip() for item in raw]
    elif isinstance(raw, str):
        values = [part.strip() for part in raw.split(",")]
    out: List[str] = []
    seen = set()
    for value in values:
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)
    if out:
        return out
    fallback_values = [str(item or "").strip() for item in (fallback or [])]
    out = []
    seen = set()
    for value in fallback_values:
        if not value or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out

SERVICE_TARGETS = {
    "whisper_asr": {
        "target": "asr",
        "aliases": ["whisper_asr", "asr", "whisper"],
        "ports": list(range(8126, 8136)),  # Port range 8126-8135 (allow fallback ports)
        "endpoint": "http://127.0.0.1:8126",
    },
    "piper_tts": {
        "target": "tts",
        "aliases": ["piper_tts", "tts", "piper"],
        "ports": list(range(8123, 8133)),  # Port range 8123-8132
        "endpoint": "http://127.0.0.1:8123",
    },
    "ollama_farm": {
        "target": "ollama",
        "aliases": ["ollama_farm", "ollama", "llm"],
        "ports": [11434, 8080] + list(range(11435, 11445)),  # Primary ports + fallback range
        "endpoint": "http://127.0.0.1:11434",
    },
    "mcp_server": {
        "target": "mcp",
        "aliases": ["mcp_server", "mcp", "context"],
        "ports": list(range(9003, 9013)),  # Port range 9003-9012
        "endpoint": "http://127.0.0.1:9003",
    },
    "web_scrape": {
        "target": "web_scrape",
        "aliases": ["web_scrape", "browser", "chrome", "scrape"],
        "ports": list(range(8130, 8140)),  # Port range 8130-8139
        "endpoint": "http://127.0.0.1:8130",
    },
    "depth_any": {
        "target": "depth_any",
        "aliases": ["depth_any", "depth", "pointcloud"],
        "ports": list(range(5000, 5010)),  # Port range 5000-5009 (matches find_available_port)
        "endpoint": "http://127.0.0.1:5000",
    },
}
MARKETPLACE_VISIBILITY = {"public", "friends", "private"}


def _normalize_marketplace_visibility(value: Any, default: str = "public") -> str:
    text = str(value or "").strip().lower()
    if text in MARKETPLACE_VISIBILITY:
        return text
    return default if default in MARKETPLACE_VISIBILITY else "public"


def _normalize_marketplace_tags(raw_tags: Any, fallback: Optional[List[str]] = None) -> List[str]:
    values: List[str]
    if isinstance(raw_tags, list):
        values = [str(item or "").strip().lower() for item in raw_tags]
    elif isinstance(raw_tags, str):
        values = [part.strip().lower() for part in raw_tags.split(",")]
    else:
        values = []
    out: List[str] = []
    seen = set()
    for value in values:
        if not value:
            continue
        token = re.sub(r"[^a-z0-9_.:-]+", "_", value).strip("_.:-")
        if not token or token in seen:
            continue
        seen.add(token)
        out.append(token)
        if len(out) >= 16:
            break
    if out:
        return out
    return list(fallback or [])


def _normalize_marketplace_sync_targets(raw_urls: Any, fallback: Optional[List[str]] = None) -> List[str]:
    values: List[str] = []
    if isinstance(raw_urls, list):
        values = [str(item or "").strip() for item in raw_urls]
    elif isinstance(raw_urls, str):
        values = [part.strip() for part in re.split(r"[,\n\r\t ]+", raw_urls)]
    elif raw_urls is not None:
        values = [str(raw_urls).strip()]
    if not values:
        values = [str(item or "").strip() for item in (fallback or [])]

    out: List[str] = []
    seen = set()
    for value in values:
        text = str(value or "").strip()
        if not text:
            continue
        parsed = urllib.parse.urlparse(text)
        if parsed.scheme not in {"http", "https"}:
            continue
        if not parsed.netloc:
            continue
        normalized = urllib.parse.urlunparse(parsed._replace(fragment="")).rstrip("/")
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        out.append(normalized)
        if len(out) >= 32:
            break
    return out


def _default_service_publication() -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for service_name, info in SERVICE_TARGETS.items():
        target = str((info or {}).get("target") or service_name)
        category = target or service_name
        default_unit = "request"
        if service_name == "whisper_asr":
            default_unit = "audio_second"
        elif service_name == "piper_tts":
            default_unit = "text_char"
        elif service_name == "depth_any":
            default_unit = "image"
        out[service_name] = {
            "enabled": True,
            "visibility": "public",
            "repository": "hydra",
            "category": category,
            "capacity_hint": 1,
            "tags": _normalize_marketplace_tags([category, service_name], fallback=[service_name]),
            "pricing": {
                "currency": "USDC",
                "unit": default_unit,
                "base_price": 0.0,
                "min_units": 1,
                "quote_public": True,
            },
        }
    return out

DAEMON_SENTINEL = Path.home() / ".unified_router_daemon.json"


def generate_seed_hex() -> str:
    return secrets.token_hex(32)


class DaemonManager:
    """Lightweight sentinel-based daemon tracker."""

    def __init__(self, sentinel: Optional[Path] = None):
        self.sentinel = Path(sentinel or DAEMON_SENTINEL)

    def check(self) -> Optional[dict]:
        if not self.sentinel.exists():
            return None
        try:
            data = json.loads(self.sentinel.read_text())
        except Exception:
            data = {"error": "unreadable sentinel"}
        data.setdefault("path", str(self.sentinel))
        return data

    def enable(self, base_dir: Path, config_path: Path) -> dict:
        info = {
            "enabled": True,
            "ts": int(time.time()),
            "base_dir": str(base_dir),
            "config": str(config_path),
            "path": str(self.sentinel),
            "note": "Sentinel for external daemon integration."
        }
        self.sentinel.parent.mkdir(parents=True, exist_ok=True)
        self.sentinel.write_text(json.dumps(info, indent=2))
        return info

    def disable(self) -> None:
        if self.sentinel.exists():
            self.sentinel.unlink()

    def path(self) -> Path:
        return self.sentinel


# ──────────────────────────────────────────────────────────────
# QR helpers
# ──────────────────────────────────────────────────────────────
def _qr_matrix(text: str, error: str = "H", border: int = 2) -> List[List[bool]]:
    if qrcode is None:
        raise RuntimeError("qrcode dependency is not available")
    qr = qrcode.QRCode(
        version=None,
        error_correction={
            "L": qrcode.constants.ERROR_CORRECT_L,
            "M": qrcode.constants.ERROR_CORRECT_M,
            "Q": qrcode.constants.ERROR_CORRECT_Q,
            "H": qrcode.constants.ERROR_CORRECT_H,
        }.get(error.upper(), qrcode.constants.ERROR_CORRECT_H),
        box_size=1,
        border=max(0, border),
    )
    qr.add_data(text)
    qr.make(fit=True)
    return qr.get_matrix()


def render_qr_ascii(text: str, scale: int = 1, invert: bool = False) -> str:
    matrix = _qr_matrix(text)
    scale = max(1, int(scale))
    block_full = "█"
    block_up = "▀"
    block_down = "▄"
    blank = " "

    def pix(val: bool) -> bool:
        return not val if invert else val

    h = len(matrix)
    if not h:
        return text
    w = len(matrix[0])
    lines: List[str] = []
    for y in range(0, h, 2):
        top = matrix[y]
        bottom = matrix[y + 1] if (y + 1) < h else [False] * w
        row_chars: List[str] = []
        for x in range(w):
            t = pix(top[x])
            b = pix(bottom[x])
            if t and b:
                ch = block_full
            elif t:
                ch = block_up
            elif b:
                ch = block_down
            else:
                ch = blank
            row_chars.append(ch * scale)
        row = "".join(row_chars)
        for _ in range(scale):
            lines.append(row)
    lines.append("(scan with camera)")
    return "\n".join(lines)


# ──────────────────────────────────────────────────────────────
# Service watchdog (embedded)
# ──────────────────────────────────────────────────────────────
SERVICES_ROOT = BASE_DIR / ".services"
LOGS_ROOT = BASE_DIR / ".logs"
METADATA_ROOT = SERVICES_ROOT / "meta"
BACKUP_ROOT = BASE_DIR / ".backups"
BACKUP_KEEP = 5
WATCHDOG_RUNTIME_ROOT = BASE_DIR / ".watchdog_runtime"
WATCHDOG_DESIRED_STATE_FILE = WATCHDOG_RUNTIME_ROOT / "desired_state.json"
WATCHDOG_LOCK_FILE = WATCHDOG_RUNTIME_ROOT / "watchdog.lock"


@dataclass
class ServiceDefinition:
    name: str
    repo_url: str
    script_path: str
    description: str
    preserve_repo: bool = False  # If True, keep full repo structure instead of extracting only script
    default_stream: bool = False  # If True, prefer streaming responses (chunks) by default
    health_mode: str = "process"  # process | tcp | http
    health_port: int = 0
    health_path: str = "/health"
    activation_timeout_seconds: float = 30.0
    activation_stability_seconds: float = 6.0
    health_check_interval_seconds: float = 2.0
    health_failure_threshold: int = 3

    @property
    def script_name(self) -> str:
        return Path(self.script_path).name


@dataclass
class ServiceState:
    definition: ServiceDefinition
    workdir: Path
    script_path: Path
    log_path: Path
    metadata_path: Path
    process: Optional[subprocess.Popen] = None
    supervisor: Optional[threading.Thread] = None
    stop_event: threading.Event = field(default_factory=threading.Event)
    restart_count: int = 0
    last_exit_code: Optional[int] = None
    last_exit_at: Optional[float] = None
    running_since: Optional[float] = None
    last_error: Optional[str] = None
    log_handle: Optional[IO[str]] = None
    terminal_proc: Optional[subprocess.Popen] = None
    terminal_pid_path: Optional[Path] = None
    fallback_mode: bool = False
    restart_attempts: int = 0
    desired_enabled: bool = True
    state: str = "stopped"  # stopped | launching | activating | running | degraded | stopping | error
    state_reason: str = ""
    last_state_change_at: float = 0.0
    activation_deadline: float = 0.0
    activation_method: str = ""
    process_stable_since: float = 0.0
    activation_checks: int = 0
    activation_failures: int = 0
    health_checks: int = 0
    health_failures: int = 0
    consecutive_health_failures: int = 0
    last_health_probe_at: float = 0.0
    last_health_ok_at: float = 0.0
    last_health_error: str = ""
    resolved_health_port: int = 0
    last_port_discovery_at: float = 0.0

    def snapshot(self) -> Dict[str, object]:
        running = (self.process is not None and self.process.poll() is None) or self.fallback_mode
        if self.fallback_mode:
            status = "system fallback"
        elif self.state:
            status = self.state
        elif running:
            status = "running"
        else:
            status = self.last_error or "stopped"
        return {
            "name": self.definition.name,
            "description": self.definition.description,
            "script": str(self.script_path),
            "log": str(self.log_path),
            "running": running,
            "pid": self.process.pid if self.process and self.process.poll() is None else None,
            "restart_count": self.restart_count,
            "running_since": self.running_since,
            "last_exit_code": self.last_exit_code,
            "last_exit_at": self.last_exit_at,
            "last_error": self.last_error,
            "status": status,
            "state": self.state,
            "state_reason": self.state_reason,
            "last_state_change_at": self.last_state_change_at,
            "desired_enabled": bool(self.desired_enabled),
            "terminal_alive": self.terminal_proc is not None and self.terminal_proc.poll() is None,
            "fallback": self.fallback_mode,
            "health_mode": self.definition.health_mode,
            "health_port": self.resolved_health_port or self.definition.health_port,
            "activation_method": self.activation_method,
            "activation_checks": self.activation_checks,
            "activation_failures": self.activation_failures,
            "health_checks": self.health_checks,
            "health_failures": self.health_failures,
            "consecutive_health_failures": self.consecutive_health_failures,
            "last_health_probe_at": self.last_health_probe_at,
            "last_health_ok_at": self.last_health_ok_at,
            "last_health_error": self.last_health_error,
        }


class ServiceWatchdog:
    """Supervises a set of long-running Python services."""

    TERMINAL_TEMPLATES = [
        ["x-terminal-emulator", "-T", "{title}", "-e", "bash", "-lc", "{cmd}"],
        ["gnome-terminal", "--title", "{title}", "--", "bash", "-lc", "{cmd}"],
        ["konsole", "-T", "{title}", "-e", "bash", "-lc", "{cmd}"],
        ["xterm", "-T", "{title}", "-e", "bash", "-lc", "{cmd}"],
        ["alacritty", "-t", "{title}", "-e", "bash", "-lc", "{cmd}"],
    ]

    DEFINITIONS: List[ServiceDefinition] = [
        ServiceDefinition(
            name="piper_tts",
            repo_url="https://github.com/robit-man/piper-tts-service.git",
            script_path="tts/tts_service.py",
            description="Piper text-to-speech REST service",
            health_mode="http",
            health_port=8123,
            health_path="/health",
        ),
        ServiceDefinition(
            name="whisper_asr",
            repo_url="https://github.com/robit-man/whisper-asr-service.git",
            script_path="asr/asr_service.py",
            description="Whisper ASR streaming/batch REST service",
            health_mode="http",
            health_port=8126,
            health_path="/health",
        ),
        ServiceDefinition(
            name="ollama_farm",
            repo_url="https://github.com/robit-man/ollama-nkn-relay.git",
            script_path="farm/ollama_farm.py",
            description="Ollama parallel proxy with concurrency guard",
            health_mode="http",
            health_port=11434,
            health_path="/",
            activation_timeout_seconds=20.0,
        ),
        ServiceDefinition(
            name="mcp_server",
            repo_url="https://github.com/robit-man/hydra-mcp-server.git",
            script_path="mcp_server/mcp_service.py",
            description="Hydra MCP context server with WebSocket + REST APIs",
            health_mode="http",
            health_port=9003,
            health_path="/healthz",
        ),
        ServiceDefinition(
            name="web_scrape",
            repo_url="https://github.com/robit-man/web-scrape-service.git",
            script_path="scrape/web_scrape.py",
            description="Headless Chrome scrape/control service",
            health_mode="http",
            health_port=8130,
            health_path="/health",
        ),
        ServiceDefinition(
            name="depth_any",
            repo_url="https://github.com/robit-man/Depth-Anything-3.git",
            script_path="app.py",
            description="Depth Anything 3 depth estimation and pointcloud generation",
            preserve_repo=True,  # Requires full repo structure for dependencies
            default_stream=False,  # Opt-in to streaming per request; UI expects non-stream for metadata calls
            health_mode="http",
            health_port=5000,
            health_path="/api/v1/health",
            activation_timeout_seconds=60.0,
            activation_stability_seconds=12.0,
        ),
    ]

    def __init__(
        self,
        base_dir: Optional[Path] = None,
        enable_logs: bool = True,
        watchdog_config: Optional[Dict[str, Any]] = None,
    ):
        self.base_dir = Path(base_dir or BASE_DIR)
        self.enable_logs = enable_logs
        self.watchdog_config = dict(watchdog_config or {})
        SERVICES_ROOT.mkdir(parents=True, exist_ok=True)
        LOGS_ROOT.mkdir(parents=True, exist_ok=True)
        METADATA_ROOT.mkdir(parents=True, exist_ok=True)
        WATCHDOG_RUNTIME_ROOT.mkdir(parents=True, exist_ok=True)
        self._states: Dict[str, ServiceState] = {}
        self._global_stop = threading.Event()
        self._lock = threading.Lock()
        self._terminal_template = self._detect_terminal()
        self._update_thread: Optional[threading.Thread] = None
        self._repo_thread: Optional[threading.Thread] = None
        self._core_repo_block_reason: Optional[str] = None
        self._restart_pending: bool = False
        self._owned_pids: set[int] = set()
        self._desired_enabled: Dict[str, bool] = {}
        self._desired_state_path = WATCHDOG_DESIRED_STATE_FILE
        self._lock_file_path = WATCHDOG_LOCK_FILE
        self._lock_handle: Optional[IO[str]] = None
        self._lock_active = False

        self._reclaim_enabled = bool(self.watchdog_config.get("port_reclaim_enabled", True))
        self._reclaim_force = bool(self.watchdog_config.get("port_reclaim_force", False))
        self._activation_timeout_default = float(self.watchdog_config.get("activation_timeout_seconds", 30.0))
        self._activation_stability_default = float(self.watchdog_config.get("activation_stability_seconds", 6.0))
        self._health_interval_default = float(self.watchdog_config.get("health_check_interval_seconds", 2.0))
        self._health_fail_threshold_default = max(1, int(self.watchdog_config.get("health_failure_threshold", 3)))
        self._reclaim_sigint_wait_s = max(0.1, float(self.watchdog_config.get("reclaim_sigint_wait_seconds", 0.7)))
        self._reclaim_sigterm_wait_s = max(0.1, float(self.watchdog_config.get("reclaim_sigterm_wait_seconds", 1.0)))
        self._reclaim_sigkill_wait_s = max(0.1, float(self.watchdog_config.get("reclaim_sigkill_wait_seconds", 1.0)))

        self._acquire_instance_lock()
        atexit.register(self._release_instance_lock)
        if not self._terminal_template:
            print("[watchdog] No terminal emulator found; log windows will not be opened.")

    def ensure_sources(self, service_config: Optional[Dict[str, bool]] = None) -> None:
        if not shutil.which("git"):
            raise SystemExit("git is required for ServiceWatchdog; please install git")

        desired = self._load_desired_state()
        self._desired_enabled = {}
        for definition in self.DEFINITIONS:
            ui_default = True if service_config is None else bool(service_config.get(definition.name, True))
            desired_enabled = bool(desired.get(definition.name, ui_default))
            self._desired_enabled[definition.name] = desired_enabled
            if not desired_enabled:
                continue
            state = self._prepare_service(definition)
            state.desired_enabled = True
            self._set_state(state, "stopped", "initialized")
            self._states[definition.name] = state
        self._save_desired_state()

    def start_all(self) -> None:
        for state in self._states.values():
            if not state.desired_enabled:
                self._set_state(state, "stopped", "disabled")
                continue
            if state.supervisor and state.supervisor.is_alive():
                continue
            state.stop_event.clear()
            t = threading.Thread(target=self._run_service_loop, args=(state,), daemon=True)
            state.supervisor = t
            t.start()
        if not self._update_thread or not self._update_thread.is_alive():
            self._update_thread = threading.Thread(target=self._poll_updates_loop, daemon=True)
            self._update_thread.start()
        if not self._repo_thread or not self._repo_thread.is_alive():
            self._repo_thread = threading.Thread(target=self._poll_core_repo_loop, daemon=True)
            self._repo_thread.start()

    def start_service(self, name: str) -> None:
        """Start (or restart) a single service supervisor loop."""
        definition = next((d for d in self.DEFINITIONS if d.name == name), None)
        if not definition:
            return
        with self._lock:
            state = self._states.get(name)
            if not state:
                state = self._prepare_service(definition)
                self._states[name] = state
            state.desired_enabled = True
            self._set_desired_enabled(name, True)
            state.stop_event.clear()
            state.last_error = None
            if not state.supervisor or not state.supervisor.is_alive():
                t = threading.Thread(target=self._run_service_loop, args=(state,), daemon=True)
                state.supervisor = t
                t.start()
        if not self._update_thread or not self._update_thread.is_alive():
            self._update_thread = threading.Thread(target=self._poll_updates_loop, daemon=True)
            self._update_thread.start()
        if not self._repo_thread or not self._repo_thread.is_alive():
            self._repo_thread = threading.Thread(target=self._poll_core_repo_loop, daemon=True)
            self._repo_thread.start()

    def stop_service(self, name: str, timeout: float = 10.0) -> None:
        """Gracefully stop a single service supervisor loop and process."""
        with self._lock:
            state = self._states.get(name)
            if not state:
                self._set_desired_enabled(name, False)
                return
            self._set_desired_enabled(name, False)
            state.desired_enabled = False
            self._set_state(state, "stopping", "stop requested")
            state.stop_event.set()
            self._terminate_process(state, timeout=timeout)
            state.process = None
            state.terminal_proc = None
            self._cleanup_terminal_tail(state)
            if state.supervisor and state.supervisor.is_alive():
                state.supervisor.join(timeout=timeout)
            state.running_since = None
            self._set_state(state, "stopped", "disabled")

    def shutdown(self, timeout: float = 15.0) -> None:
        self._global_stop.set()
        if self._update_thread and self._update_thread.is_alive():
            self._update_thread.join(timeout=timeout)
        if self._repo_thread and self._repo_thread.is_alive():
            self._repo_thread.join(timeout=timeout)
        for state in self._states.values():
            state.desired_enabled = False
            state.stop_event.set()
            self._set_state(state, "stopping", "shutdown requested")
            self._terminate_process(state, timeout=timeout)
            state.terminal_proc = None
            self._cleanup_terminal_tail(state)
            if state.supervisor and state.supervisor.is_alive():
                state.supervisor.join(timeout=timeout)
            self._set_state(state, "stopped", "shutdown complete")
        self._save_desired_state()
        self._release_instance_lock()

    def get_snapshot(self) -> List[Dict[str, object]]:
        snapshots = [state.snapshot() for state in self._states.values()]
        known = {snap["name"] for snap in snapshots}
        for definition in self.DEFINITIONS:
            if definition.name in known:
                continue
            desired = bool(self._desired_enabled.get(definition.name, True))
            snapshots.append(
                {
                    "name": definition.name,
                    "description": definition.description,
                    "script": definition.script_path,
                    "log": str(LOGS_ROOT / f"{definition.name}.log"),
                    "running": False,
                    "pid": None,
                    "restart_count": 0,
                    "running_since": None,
                    "last_exit_code": None,
                    "last_exit_at": None,
                    "last_error": None,
                    "status": "stopped",
                    "state": "stopped",
                    "state_reason": "disabled" if not desired else "not initialized",
                    "last_state_change_at": 0.0,
                    "desired_enabled": desired,
                    "terminal_alive": False,
                    "fallback": False,
                    "health_mode": definition.health_mode,
                    "health_port": definition.health_port,
                    "activation_method": "",
                    "activation_checks": 0,
                    "activation_failures": 0,
                    "health_checks": 0,
                    "health_failures": 0,
                    "consecutive_health_failures": 0,
                    "last_health_probe_at": 0.0,
                    "last_health_ok_at": 0.0,
                    "last_health_error": "",
                }
            )
        return snapshots

    def desired_state(self) -> Dict[str, bool]:
        return dict(self._desired_enabled)

    # internal helpers -------------------------------------------------
    def _acquire_instance_lock(self) -> None:
        try:
            WATCHDOG_RUNTIME_ROOT.mkdir(parents=True, exist_ok=True)
            handle = open(self._lock_file_path, "a+", encoding="utf-8")
            try:
                if os.name == "nt":
                    import msvcrt  # type: ignore

                    msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl  # type: ignore

                    fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except (BlockingIOError, OSError):
                handle.seek(0)
                raw = handle.read().strip()
                existing_pid = None
                with contextlib.suppress(Exception):
                    existing_pid = int(raw) if raw else None
                if existing_pid and not self._is_pid_running(existing_pid):
                    suffix = f" (pid {existing_pid} not alive; stale lock suspected)"
                else:
                    suffix = f" (pid {existing_pid})" if existing_pid else ""
                handle.close()
                raise RuntimeError(f"Another watchdog instance is already running{suffix}")
            handle.seek(0)
            handle.truncate()
            handle.write(str(os.getpid()))
            handle.flush()
            self._lock_handle = handle
            self._lock_active = True
        except RuntimeError:
            raise
        except Exception as exc:
            raise RuntimeError(f"Failed to acquire watchdog lock: {exc}") from exc

    def _release_instance_lock(self) -> None:
        if not self._lock_active:
            return
        handle = self._lock_handle
        if handle is not None:
            with contextlib.suppress(Exception):
                if os.name == "nt":
                    import msvcrt  # type: ignore

                    handle.seek(0)
                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl  # type: ignore

                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            with contextlib.suppress(Exception):
                handle.close()
        self._lock_handle = None
        self._lock_active = False
        with contextlib.suppress(Exception):
            if self._lock_file_path.exists():
                raw = self._lock_file_path.read_text(encoding="utf-8").strip()
                if raw == str(os.getpid()):
                    self._lock_file_path.unlink()

    @staticmethod
    def _is_pid_running(pid: int) -> bool:
        if pid <= 0:
            return False
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False

    def _load_desired_state(self) -> Dict[str, bool]:
        path = self._desired_state_path
        if not path.exists():
            return {}
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(payload, dict):
                raise ValueError("desired state payload must be an object")
            out: Dict[str, bool] = {}
            for key, value in payload.items():
                if isinstance(key, str):
                    out[key] = bool(value)
            return out
        except Exception as exc:
            print(f"[watchdog] Warning: failed to read desired state ({exc}); using safe defaults")
            return {}

    def _save_desired_state(self) -> None:
        payload = {definition.name: bool(self._desired_enabled.get(definition.name, True)) for definition in self.DEFINITIONS}
        tmp_path = self._desired_state_path.with_suffix(".tmp")
        try:
            tmp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
            tmp_path.replace(self._desired_state_path)
        except Exception as exc:
            print(f"[watchdog] Warning: failed to write desired state ({exc})")
            with contextlib.suppress(Exception):
                if tmp_path.exists():
                    tmp_path.unlink()

    def _set_desired_enabled(self, service_name: str, enabled: bool) -> None:
        self._desired_enabled[service_name] = bool(enabled)
        self._save_desired_state()

    def _set_state(self, state: ServiceState, new_state: str, reason: str = "", error: Optional[str] = None) -> None:
        old = state.state
        now = time.time()
        state.state = new_state
        state.state_reason = reason
        if error is not None:
            state.last_error = error or None
        if old != new_state:
            state.last_state_change_at = now
            print(f"[watchdog] {state.definition.name}: {old or 'unknown'} -> {new_state} ({reason})")
    def _detect_terminal(self) -> Optional[List[str]]:
        for template in self.TERMINAL_TEMPLATES:
            if shutil.which(template[0]):
                return template
        return None

    def _prepare_service(self, definition: ServiceDefinition) -> ServiceState:
        svc_dir = SERVICES_ROOT / definition.name
        script_dest = svc_dir / definition.script_name
        svc_dir.mkdir(parents=True, exist_ok=True)
        log_path = LOGS_ROOT / f"{definition.name}.log"
        meta_path = METADATA_ROOT / f"{definition.name}.json"

        if not script_dest.exists():
            self._fetch_and_extract(definition, svc_dir, script_dest, meta_path)
        else:
            self._write_metadata(meta_path, definition, "cached")

        return ServiceState(
            definition=definition,
            workdir=svc_dir,
            script_path=script_dest,
            log_path=log_path,
            metadata_path=meta_path,
        )

    def _fetch_and_extract(self, definition: ServiceDefinition, svc_dir: Path, script_dest: Path, meta_path: Path) -> None:
        # Check if we should preserve the full repository structure
        if definition.preserve_repo:
            # Clone entire repo into service directory
            if svc_dir.exists():
                shutil.rmtree(svc_dir, ignore_errors=True)
            subprocess.check_call(["git", "clone", "--depth", "1", definition.repo_url, str(svc_dir)])
            source_file = svc_dir / definition.script_path
            if not source_file.exists():
                shutil.rmtree(svc_dir, ignore_errors=True)
                raise FileNotFoundError(f"Service script {definition.script_path} not found in repo {definition.repo_url}")
            self._write_metadata(meta_path, definition, "fetched")
            return

        # Standard behavior: extract only the script file
        tmp_dir = SERVICES_ROOT / f"tmp_{definition.name}_{int(time.time())}"
        if tmp_dir.exists():
            shutil.rmtree(tmp_dir, ignore_errors=True)
        subprocess.check_call(["git", "clone", "--depth", "1", definition.repo_url, str(tmp_dir)])
        source_file = tmp_dir / definition.script_path
        if not source_file.exists():
            shutil.rmtree(tmp_dir, ignore_errors=True)
            raise FileNotFoundError(f"Service script {definition.script_path} not found in repo {definition.repo_url}")
        svc_dir.mkdir(parents=True, exist_ok=True)
        shutil.move(str(source_file), str(script_dest))
        self._write_metadata(meta_path, definition, "fetched")
        shutil.rmtree(tmp_dir, ignore_errors=True)

    def _write_metadata(self, path: Path, definition: ServiceDefinition, status: str) -> None:
        meta = {
            "name": definition.name,
            "repo": definition.repo_url,
            "script": definition.script_path,
            "preserve_repo": definition.preserve_repo,
            "status": status,
            "ts": int(time.time()),
        }
        path.write_text(json.dumps(meta, indent=2))

    def _run_git(self, workdir: Path, args: List[str]) -> str:
        proc = subprocess.run(
            ["git", "-C", str(workdir), *args],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if proc.returncode != 0:
            raise subprocess.CalledProcessError(proc.returncode, proc.args, output=proc.stdout, stderr=proc.stderr)
        return proc.stdout.strip()

    def _core_repo_blocker(self, repo_dir: Path) -> Optional[str]:
        """
        Return a reason string if the core repo should not be auto-pulled.
        Avoids triggering git when locks/operations are in progress or when the
        repo isn't writable (prevents noisy ORIG_HEAD.lock failures).
        """
        git_dir = repo_dir / ".git"
        if not git_dir.exists():
            return "not a git repository"
        lock_names = ["index.lock", "HEAD.lock", "ORIG_HEAD.lock"]
        for name in lock_names:
            if (git_dir / name).exists():
                return f"lock present ({name})"
        busy_markers = ["rebase-apply", "rebase-merge", "MERGE_HEAD"]
        for name in busy_markers:
            if (git_dir / name).exists():
                return f"repository busy ({name})"
        # Ensure we can write to both the repo and .git metadata
        if not os.access(repo_dir, os.W_OK) or not os.access(git_dir, os.W_OK):
            return "repository not writable"
        return None

    def _backup_repo(self, repo_dir: Path) -> Optional[Path]:
        try:
            BACKUP_ROOT.mkdir(parents=True, exist_ok=True)
            ts = int(time.time())
            backup_dir = BACKUP_ROOT / f"hydra_{ts}"
            if backup_dir.exists():
                shutil.rmtree(backup_dir, ignore_errors=True)
            ignore_dirs = [
                BACKUP_ROOT.name,  # prevent recursive explosion
                ".logs",
                ".services",
                ".venv_router",
                ".venv",
                "__pycache__",
                ".cache",
            ]
            shutil.copytree(
                repo_dir,
                backup_dir,
                dirs_exist_ok=False,
                ignore=shutil.ignore_patterns(*ignore_dirs),
            )
            self._prune_backups()
            return backup_dir
        except Exception:
            return None

    def _prune_backups(self) -> None:
        """Keep only the most recent BACKUP_KEEP backups to limit disk usage."""
        try:
            backups = sorted([p for p in BACKUP_ROOT.glob("hydra_*") if p.is_dir()], key=lambda p: p.stat().st_mtime, reverse=True)
            for stale in backups[BACKUP_KEEP:]:
                shutil.rmtree(stale, ignore_errors=True)
        except Exception:
            pass

    def _restore_repo(self, repo_dir: Path, backup_dir: Path) -> bool:
        try:
            if not backup_dir.exists():
                return False
            # Danger: remove current repo and restore backup
            shutil.rmtree(repo_dir, ignore_errors=True)
            shutil.copytree(backup_dir, repo_dir, dirs_exist_ok=False)
            return True
        except Exception:
            return False

    def _restart_router(self) -> None:
        """Exec-restart the router process after a safe pull."""
        if self._restart_pending:
            return
        self._restart_pending = True
        print("[watchdog] Pull applied; restarting router…")
        sys.stdout.flush()
        sys.stderr.flush()
        os.execv(sys.executable, [sys.executable, *sys.argv])

    def _maybe_update_service(self, state: ServiceState) -> None:
        """
        Check for upstream updates on preserve_repo services.
        If updates are found, pull them and restart the service.
        """
        if not state.definition.preserve_repo:
            return
        git_dir = state.workdir / ".git"
        if not git_dir.exists():
            return
        try:
            self._run_git(state.workdir, ["fetch", "--prune", "--quiet"])
            try:
                branch = self._run_git(state.workdir, ["rev-parse", "--abbrev-ref", "HEAD"])
            except Exception:
                branch = "main"
            try:
                local = self._run_git(state.workdir, ["rev-parse", "HEAD"])
                remote = self._run_git(state.workdir, ["rev-parse", f"{branch}@{{upstream}}"])
            except Exception:
                return
            if local == remote:
                return
            print(f"[watchdog] Updates detected for {state.definition.name}; pulling…")
            self._run_git(state.workdir, ["pull", "--rebase", "--autostash"])
            if state.process and state.process.poll() is None:
                print(f"[watchdog] Restarting {state.definition.name} after update")
                self._terminate_process(state)
            # The supervisor loop will restart it; if not running, start a fresh loop.
            if not state.supervisor or not state.supervisor.is_alive():
                state.stop_event.clear()
                t = threading.Thread(target=self._run_service_loop, args=(state,), daemon=True)
                state.supervisor = t
                t.start()
        except Exception as exc:
            state.last_error = f"update check failed: {exc}"

    def _poll_updates_loop(self) -> None:
        """Periodic git update checks for running services."""
        interval = 300
        while not self._global_stop.is_set():
            for state in list(self._states.values()):
                if self._global_stop.is_set():
                    break
                if not state.desired_enabled:
                    continue
                try:
                    self._maybe_update_service(state)
                except Exception:
                    # best-effort; errors are recorded per state
                    pass
            self._global_stop.wait(interval)

    def _poll_core_repo_loop(self) -> None:
        """Monitor the main hydra repo for updates; auto-pull with backup/rollback."""
        repo_dir = REPO_ROOT
        interval = 300
        while not self._global_stop.is_set():
            try:
                git_dir = repo_dir / '.git'
                if git_dir.exists():
                    blocker = self._core_repo_blocker(repo_dir)
                    if blocker:
                        if blocker != self._core_repo_block_reason:
                            print(f"[watchdog] Skipping core repo pull: {blocker}")
                        self._core_repo_block_reason = blocker
                        self._global_stop.wait(interval)
                        continue
                    self._core_repo_block_reason = None
                    self._run_git(repo_dir, ["fetch", "--prune", "--quiet"])
                    try:
                        branch = self._run_git(repo_dir, ["rev-parse", "--abbrev-ref", "HEAD"])
                    except Exception:
                        branch = "main"
                    try:
                        local = self._run_git(repo_dir, ["rev-parse", "HEAD"])
                        remote = self._run_git(repo_dir, ["rev-parse", f"{branch}@{{upstream}}"])
                    except Exception:
                        local = remote = None
                    if local and remote and local != remote:
                        backup = self._backup_repo(repo_dir)
                        try:
                            self._run_git(repo_dir, ["pull", "--rebase", "--autostash"])
                            backup = None
                            # restart the router to pick up changes
                            self._restart_router()
                        except subprocess.CalledProcessError as exc:
                            if backup:
                                self._restore_repo(repo_dir, backup)
                            detail = (exc.stderr or exc.output or "").strip()
                            msg = detail if detail else str(exc)
                            print(f"[watchdog] core repo pull failed: {msg}")
                        except Exception as exc:
                            if backup:
                                self._restore_repo(repo_dir, backup)
                            print(f"[watchdog] core repo pull failed: {exc}")
                else:
                    # Not a git repo; skip
                    pass
            except Exception:
                pass
            self._global_stop.wait(interval)

    def _run_service_loop(self, state: ServiceState) -> None:
        backoff = 1.0
        state.restart_attempts = 0
        while not self._global_stop.is_set() and not state.stop_event.is_set():
            if not state.desired_enabled:
                self._set_state(state, "stopped", "disabled")
                self._global_stop.wait(0.5)
                continue
            try:
                if state.definition.name == "ollama_farm":
                    if self._handle_ollama(state):
                        backoff = 1.0
                        state.restart_attempts = 0
                        self._global_stop.wait(2.0)
                        continue
                else:
                    if self._manage_standard_service(state):
                        backoff = 1.0
                        state.restart_attempts = 0
                        continue
            except Exception as exc:
                state.last_error = str(exc)
                state.last_exit_at = time.time()
                crashed_pid = int(getattr(state.process, "pid", 0) or 0)
                state.process = None
                if crashed_pid > 0:
                    self._owned_pids.discard(crashed_pid)
                self._close_log(state)
                state.last_exit_code = None
                state.restart_count += 1
                self._set_state(state, "error", f"supervisor exception: {exc}", error=str(exc))
                self._global_stop.wait(min(backoff, 60.0))
                backoff = min(backoff * 2.0, 60.0)
                state.restart_attempts += 1
                continue
            state.restart_count += 1
            state.restart_attempts += 1
            state.last_error = state.last_error or "Repeated startup failures"
            state.process = None
            state.running_since = None
            self._close_log(state)
            self._set_state(state, "error", f"restart in {min(backoff, 60.0):.1f}s", error=state.last_error)
            self._global_stop.wait(min(backoff, 60.0))
            backoff = min(backoff * 2.0, 60.0)
        state.running_since = None
        if state.state != "stopped":
            self._set_state(state, "stopped", "loop ended")

    def _preferred_python(self, state: ServiceState) -> Path:
        """Choose a Python interpreter for the service.

        For preserve_repo services (e.g., depth_any), prefer a repo-local venv so their
        bootstrap scripts can install and use their own dependencies. Otherwise, stick to
        the router venv.
        """
        if state.definition.preserve_repo:
            candidates = []
            # repo-local venv
            venv_dir = state.workdir / ".venv"
            if os.name == "nt":
                candidates.append(venv_dir / "Scripts" / "python.exe")
            else:
                candidates.append(venv_dir / "bin" / "python")
            # system interpreters
            for exe in ("python3", "python"):
                found = shutil.which(exe)
                if found:
                    candidates.append(Path(found))
            # fallback to current
            candidates.append(Path(sys.executable))
            for cand in candidates:
                if cand and Path(cand).exists():
                    return Path(cand)
        return Path(sys.executable)

    def _start_process(self, state: ServiceState) -> None:
        if state.process and state.process.poll() is None:
            return
        python_path = self._preferred_python(state)
        if not python_path.exists():
            raise RuntimeError("Python executable not found for watchdog launch")
        log_file = open(state.log_path, "a", buffering=1, encoding="utf-8", errors="replace")
        state.log_handle = log_file
        cmd = [str(python_path), str(state.script_path)]
        env = os.environ.copy()
        # Ensure service bootstrap is not polluted by the router venv
        env.pop("VIRTUAL_ENV", None)
        env.pop("PYTHONHOME", None)
        state.process = subprocess.Popen(
            cmd,
            cwd=state.workdir,
            stdout=log_file,
            stderr=log_file,
            text=True,
            bufsize=1,
            env=env,
        )
        if state.process and state.process.pid:
            self._owned_pids.add(int(state.process.pid))
        state.running_since = time.time()
        state.process_stable_since = state.running_since
        state.activation_checks = 0
        state.activation_failures = 0
        state.last_error = None
        log_file.write(f"\n[{time.strftime('%Y-%m-%d %H:%M:%S')}] watchdog: started {cmd}\n")
        log_file.flush()
        self._ensure_terminal_tail(state)

    def _handle_ollama(self, state: ServiceState) -> bool:
        proc = state.process
        if proc and proc.poll() is None:
            if self._ollama_health_ok():
                state.fallback_mode = False
                state.consecutive_health_failures = 0
                self._set_state(state, "running", "ollama_farm healthy")
                return True
            state.health_failures += 1
            state.consecutive_health_failures += 1
            state.last_health_error = "ollama health probe failed"
            self._set_state(state, "degraded", "ollama health probe failed")
            if state.consecutive_health_failures >= max(2, self._health_fail_threshold_default * 2):
                state.last_error = "ollama health failures exceeded threshold"
                self._set_state(state, "error", state.last_error, error=state.last_error)
                self._terminate_process(state)
                return False
            return True

        if state.fallback_mode:
            if self._ollama_health_ok():
                self._set_state(state, "running", "using system ollama")
                state.last_error = None
            else:
                self._set_state(state, "degraded", "system ollama health probe failed")
                state.last_error = "system ollama unhealthy"
            return True

        if self._ollama_health_ok():
            state.fallback_mode = True
            state.last_error = None
            state.running_since = time.time()
            self._set_state(state, "running", "using existing local ollama")
            return True

        ok_ports, reclaim_reason = self._free_ports([11434, 8080], state=state)
        if not ok_ports:
            state.last_error = reclaim_reason
            self._set_state(state, "error", reclaim_reason, error=reclaim_reason)
            return False
        state.fallback_mode = False
        self._set_state(state, "launching", "starting ollama_farm")
        self._start_process(state)
        proc = state.process
        if not proc:
            state.last_error = "ollama_farm failed to spawn"
            self._set_state(state, "error", state.last_error, error=state.last_error)
            return False
        self._set_state(state, "activating", "waiting for ollama health")
        ready = self._wait_for_ollama_health(timeout=20)
        if ready:
            state.last_error = None
            state.restart_attempts = 0
            self._set_state(state, "running", "ollama health passed", error="")
            return True

        state.last_error = "ollama_farm failed to start; falling back"
        self._set_state(state, "degraded", state.last_error, error="")
        self._terminate_process(state)
        if self._ollama_health_ok():
            state.fallback_mode = True
            state.running_since = time.time()
            state.process = None
            self._close_log(state)
            state.restart_attempts = 0
            self._set_state(state, "degraded", "process failed; using system fallback", error="")
            return True
        state.last_error = "ollama fallback unavailable"
        self._set_state(state, "error", state.last_error, error=state.last_error)
        return False

    def _manage_standard_service(self, state: ServiceState) -> bool:
        service_ports = self._service_ports(state.definition.name)
        ok_ports, reclaim_reason = self._free_ports(service_ports, state=state)
        if not ok_ports:
            state.last_error = reclaim_reason
            self._set_state(state, "error", reclaim_reason, error=reclaim_reason)
            return False
        self._set_state(state, "launching", "starting process")
        self._start_process(state)
        proc = state.process
        if not proc:
            state.last_error = "spawn failed"
            self._set_state(state, "error", "spawn failed", error=state.last_error)
            return False
        activation_timeout = max(
            3.0,
            float(state.definition.activation_timeout_seconds or self._activation_timeout_default),
        )
        stability_window = max(
            1.5,
            float(state.definition.activation_stability_seconds or self._activation_stability_default),
        )
        health_interval = max(
            0.6,
            float(state.definition.health_check_interval_seconds or self._health_interval_default),
        )
        health_threshold = max(
            1,
            int(state.definition.health_failure_threshold or self._health_fail_threshold_default),
        )
        hard_restart_threshold = max(2, health_threshold * 2)

        state.activation_deadline = time.time() + activation_timeout
        state.last_health_probe_at = 0.0
        state.last_health_error = ""
        state.consecutive_health_failures = 0
        self._set_state(state, "activating", "health probe pending")

        while not self._global_stop.is_set() and not state.stop_event.is_set() and state.desired_enabled:
            if proc.poll() is not None:
                ret = int(proc.returncode or 0)
                state.last_exit_code = ret
                state.last_exit_at = time.time()
                state.process = None
                self._owned_pids.discard(int(proc.pid or 0))
                self._close_log(state)
                if state.stop_event.is_set() or self._global_stop.is_set() or not state.desired_enabled:
                    self._set_state(state, "stopped", "process exited after stop request", error="")
                    return True
                state.last_error = f"Exited with code {ret}"
                self._set_state(state, "error", state.last_error, error=state.last_error)
                return False

            self._discover_runtime_health_port(state)
            now = time.time()
            should_probe = state.last_health_probe_at <= 0 or (now - state.last_health_probe_at) >= health_interval
            if not should_probe:
                self._global_stop.wait(0.15)
                continue

            state.last_health_probe_at = now
            probe_ok, probe_detail = self._health_probe_with_runtime(state)

            if state.state in ("launching", "activating"):
                state.activation_checks += 1
                if probe_ok:
                    state.activation_method = f"probe:{probe_detail}"
                    state.last_health_ok_at = now
                    state.last_health_error = ""
                    state.consecutive_health_failures = 0
                    self._set_state(state, "running", f"activated ({probe_detail})", error="")
                    continue

                state.activation_failures += 1
                state.last_health_error = probe_detail
                alive_for = now - (state.process_stable_since or now)
                if alive_for >= stability_window:
                    state.activation_method = f"stable-process:{alive_for:.1f}s"
                    self._set_state(
                        state,
                        "degraded",
                        f"activation fallback by process stability ({probe_detail})",
                        error="",
                    )
                    continue

                if now >= state.activation_deadline:
                    state.last_error = f"Activation timeout ({probe_detail})"
                    self._set_state(state, "error", state.last_error, error=state.last_error)
                    self._terminate_process(state)
                    return False
                continue

            state.health_checks += 1
            if probe_ok:
                state.last_health_ok_at = now
                state.last_health_error = ""
                state.consecutive_health_failures = 0
                if state.state != "running":
                    self._set_state(state, "running", f"health restored ({probe_detail})", error="")
                continue

            state.health_failures += 1
            state.consecutive_health_failures += 1
            state.last_health_error = probe_detail
            if state.consecutive_health_failures >= health_threshold and state.state != "degraded":
                self._set_state(
                    state,
                    "degraded",
                    f"health degraded ({state.consecutive_health_failures}/{hard_restart_threshold}): {probe_detail}",
                    error="",
                )
            if state.consecutive_health_failures >= hard_restart_threshold:
                state.last_error = f"Health failures exceeded threshold ({probe_detail})"
                self._set_state(state, "error", state.last_error, error=state.last_error)
                self._terminate_process(state)
                return False

        self._set_state(state, "stopping", "stop requested")
        self._terminate_process(state)
        self._set_state(state, "stopped", "stop complete", error="")
        return True

    def _service_ports(self, service_name: str) -> List[int]:
        info = SERVICE_TARGETS.get(service_name) or {}
        ports = info.get("ports")
        if isinstance(ports, list) and ports:
            out: List[int] = []
            for raw in ports:
                with contextlib.suppress(Exception):
                    port = int(raw)
                    if 1 <= port <= 65535:
                        out.append(port)
            if out:
                return sorted(set(out))
        definition = next((d for d in self.DEFINITIONS if d.name == service_name), None)
        if definition and definition.health_port > 0:
            return [int(definition.health_port)]
        return []

    def _terminate_process(self, state: ServiceState, timeout: float = 5.0) -> None:
        proc = state.process
        if proc and proc.poll() is None:
            pid = int(proc.pid or 0)
            with contextlib.suppress(Exception):
                proc.send_signal(signal.SIGINT)
            try:
                proc.wait(timeout=min(timeout, 2.0))
            except Exception:
                with contextlib.suppress(Exception):
                    proc.terminate()
                try:
                    proc.wait(timeout=min(timeout, 2.0))
                except Exception:
                    with contextlib.suppress(Exception):
                        proc.kill()
            if pid > 0:
                self._owned_pids.discard(pid)
        state.process = None
        self._close_log(state)
        state.process_stable_since = 0.0

    def _wait_for_ollama_health(self, timeout: float) -> bool:
        start = time.time()
        while time.time() - start < timeout:
            if self._ollama_health_ok():
                return True
            time.sleep(2)
        return False

    def _ollama_health_ok(self) -> bool:
        import urllib.request

        try:
            with urllib.request.urlopen("http://127.0.0.1:11434/", timeout=3) as resp:
                body = resp.read().decode("utf-8", "ignore")
                return "Ollama is running" in body
        except Exception:
            return False

    def _resolve_health_port(self, state: ServiceState) -> int:
        if state.resolved_health_port > 0:
            return int(state.resolved_health_port)
        try:
            static_port = int(state.definition.health_port or 0)
        except Exception:
            static_port = 0
        return static_port if 1 <= static_port <= 65535 else 0

    def _discover_runtime_health_port(self, state: ServiceState) -> int:
        proc = state.process
        if not proc or proc.poll() is not None:
            return self._resolve_health_port(state)
        pid = int(proc.pid or 0)
        if pid <= 0:
            return self._resolve_health_port(state)
        ports: List[int] = []
        if shutil.which("lsof"):
            try:
                out = subprocess.check_output(
                    ["lsof", "-Pan", "-p", str(pid), "-iTCP", "-sTCP:LISTEN"],
                    text=True,
                    stderr=subprocess.DEVNULL,
                )
                for line in out.splitlines():
                    match = re.search(r":(\d+)\s+\(LISTEN\)", line)
                    if not match:
                        continue
                    with contextlib.suppress(Exception):
                        port = int(match.group(1))
                        if 1 <= port <= 65535:
                            ports.append(port)
            except Exception:
                pass
        if not ports and shutil.which("ss"):
            try:
                out = subprocess.check_output(["ss", "-ltnp"], text=True, stderr=subprocess.DEVNULL)
                for line in out.splitlines():
                    if f"pid={pid}," not in line and f"pid={pid})" not in line:
                        continue
                    match = re.search(r":(\d+)\s+", line)
                    if not match:
                        continue
                    with contextlib.suppress(Exception):
                            port = int(match.group(1))
                            if 1 <= port <= 65535:
                                ports.append(port)
            except Exception:
                pass
        if not ports:
            try:
                lines = state.log_path.read_text(encoding="utf-8", errors="ignore").splitlines()[-120:]
                patterns = [
                    r"Running on .*:(\d+)",
                    r"listening on .*:(\d+)",
                    r"Listening on port (\d+)",
                    r"http://[^:]+:(\d+)",
                ]
                for line in reversed(lines):
                    for pattern in patterns:
                        match = re.search(pattern, line, re.IGNORECASE)
                        if not match:
                            continue
                        with contextlib.suppress(Exception):
                            port = int(match.group(1))
                            if 1 <= port <= 65535:
                                ports.append(port)
                    if ports:
                        break
            except Exception:
                pass
        if ports:
            preferred = int(state.definition.health_port or 0)
            selected = preferred if preferred in ports else sorted(set(ports))[0]
            state.resolved_health_port = selected
            state.last_port_discovery_at = time.time()
        return self._resolve_health_port(state)

    def _probe_http_health(self, port: int, path: str) -> Tuple[bool, str]:
        if port <= 0:
            return False, "missing health port"
        health_path = str(path or "/").strip() or "/"
        if not health_path.startswith("/"):
            health_path = f"/{health_path}"
        url = f"http://127.0.0.1:{port}{health_path}"
        try:
            resp = requests.get(url, timeout=2.0)
            if resp.status_code < 400:
                return True, f"http:{resp.status_code}"
            if resp.status_code in (401, 403):
                return True, f"http:{resp.status_code} auth-gated"
            return False, f"http:{resp.status_code}"
        except Exception as exc:
            return False, f"{type(exc).__name__}: {exc}"

    def _health_probe_with_runtime(self, state: ServiceState) -> Tuple[bool, str]:
        proc = state.process
        if not proc or proc.poll() is not None:
            return False, "process not running"
        mode = str(state.definition.health_mode or "process").strip().lower()
        if mode == "process":
            return True, "process"
        port = self._discover_runtime_health_port(state)
        if mode == "tcp":
            if port <= 0:
                return False, "tcp:no-port"
            return (self._port_in_use(port), f"tcp:{port}")
        if mode == "http":
            return self._probe_http_health(port, state.definition.health_path)
        return True, "process"

    def _wait_for_pid_exit(self, pid: int, timeout_s: float) -> bool:
        deadline = time.time() + max(0.1, float(timeout_s))
        while time.time() < deadline:
            if not self._is_pid_running(pid):
                return True
            time.sleep(0.1)
        return not self._is_pid_running(pid)

    def _terminate_pid_for_reclaim(self, pid: int) -> bool:
        if pid <= 0 or pid == os.getpid():
            return False
        if not self._is_pid_running(pid):
            return True
        with contextlib.suppress(Exception):
            os.kill(pid, signal.SIGINT)
        if self._wait_for_pid_exit(pid, self._reclaim_sigint_wait_s):
            return True
        with contextlib.suppress(Exception):
            os.kill(pid, signal.SIGTERM)
        if self._wait_for_pid_exit(pid, self._reclaim_sigterm_wait_s):
            return True
        with contextlib.suppress(Exception):
            os.kill(pid, signal.SIGKILL)
        return self._wait_for_pid_exit(pid, self._reclaim_sigkill_wait_s)

    def _read_process_commandline(self, pid: int) -> str:
        if pid <= 0:
            return ""
        proc_cmdline = Path(f"/proc/{pid}/cmdline")
        try:
            if proc_cmdline.exists():
                raw = proc_cmdline.read_bytes()
                return raw.replace(b"\x00", b" ").decode("utf-8", errors="ignore").strip()
        except Exception:
            pass
        try:
            result = subprocess.run(
                ["ps", "-p", str(pid), "-o", "command="],
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                text=True,
                check=False,
                timeout=2.5,
            )
            return (result.stdout or "").strip()
        except Exception:
            return ""

    def _pid_likely_owned_by_service(self, pid: int, state: ServiceState) -> bool:
        if pid <= 0:
            return False
        if pid == os.getpid():
            return False
        if pid in self._owned_pids:
            return True
        if state.process and state.process.pid == pid:
            return True
        cmdline = self._read_process_commandline(pid)
        if not cmdline:
            return False
        normalized = cmdline.replace("\\", "/").lower()
        script_abs = str(state.script_path).replace("\\", "/").lower()
        script_rel = str(state.definition.script_path).replace("\\", "/").lower()
        script_name = Path(script_abs).name
        if script_abs and script_abs in normalized:
            return True
        if script_rel and script_rel in normalized:
            return True
        if script_name and (
            normalized.endswith(script_name)
            or f"/{script_name}" in normalized
            or f" {script_name} " in normalized
        ):
            return True
        return False

    def _classify_port_owners(self, port: int, state: Optional[ServiceState]) -> Tuple[List[int], List[int], List[int]]:
        observed = [pid for pid in self._find_pids_on_port(port) if pid != os.getpid()]
        owned: List[int] = []
        foreign: List[int] = []
        for pid in observed:
            if state and self._pid_likely_owned_by_service(pid, state):
                owned.append(pid)
            else:
                foreign.append(pid)
        return sorted(set(observed)), sorted(set(owned)), sorted(set(foreign))

    def _free_ports(
        self,
        ports: List[int],
        state: Optional[ServiceState] = None,
        force: Optional[bool] = None,
    ) -> Tuple[bool, str]:
        if not self._reclaim_enabled:
            for port in ports:
                if port > 0 and self._port_in_use(port):
                    return False, f"port reclaim disabled and port {port} is already in use"
            return True, ""

        reclaim_force = self._reclaim_force if force is None else bool(force)
        for port in ports:
            if port <= 0:
                continue
            observed, owned, foreign = self._classify_port_owners(port, state)
            if not observed:
                continue
            reclaim_targets = list(owned)
            if reclaim_force:
                reclaim_targets.extend(foreign)
            reclaim_targets = sorted(set(reclaim_targets))
            policy = "force" if reclaim_force else "owned-only"
            print(
                f"[watchdog] Port reclaim decision svc={state.definition.name if state else 'unknown'} "
                f"port={port} policy={policy} observed={observed} owned={owned} foreign={foreign} "
                f"targets={reclaim_targets}"
            )
            for pid in reclaim_targets:
                ok = self._terminate_pid_for_reclaim(pid)
                result = "ok" if ok else "failed"
                print(f"[watchdog] Port reclaim result port={port} pid={pid} result={result}")

            remaining = [pid for pid in self._find_pids_on_port(port) if pid != os.getpid()]
            if remaining:
                hint = ""
                if not reclaim_force and any(pid in foreign for pid in remaining):
                    hint = " (foreign process; enable force reclaim to override)"
                reason = f"port {port} in use by pid(s) {','.join(str(pid) for pid in remaining)}{hint}"
                return False, reason
        return True, ""

    def _port_in_use(self, port: int) -> bool:
        import socket

        with contextlib.closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
            sock.settimeout(0.2)
            try:
                return sock.connect_ex(("127.0.0.1", port)) == 0
            except Exception:
                return False

    def _find_pids_on_port(self, port: int) -> List[int]:
        pids: set[int] = set()
        if shutil.which("lsof"):
            try:
                out = subprocess.check_output(["lsof", "-ti", f":{port}"], text=True)
                for line in out.splitlines():
                    with contextlib.suppress(Exception):
                        pid = int(line.strip())
                        if pid > 0:
                            pids.add(pid)
            except subprocess.CalledProcessError:
                pass
        if not pids and shutil.which("fuser"):
            try:
                out = subprocess.check_output(["fuser", "-n", "tcp", str(port)], text=True)
                for token in out.split():
                    with contextlib.suppress(Exception):
                        pid = int(token)
                        if pid > 0:
                            pids.add(pid)
            except subprocess.CalledProcessError:
                pass
        if not pids and shutil.which("ss"):
            try:
                out = subprocess.check_output(["ss", "-ltnp", "sport", "=", f":{int(port)}"], text=True, stderr=subprocess.DEVNULL)
                for line in out.splitlines():
                    for match in re.findall(r"pid=(\d+)", line):
                        with contextlib.suppress(Exception):
                            pid = int(match)
                            if pid > 0:
                                pids.add(pid)
            except Exception:
                pass
        return sorted(pids)

    def _ensure_terminal_tail(self, state: ServiceState) -> None:
        if not self._terminal_template:
            return
        if state.terminal_proc and state.terminal_proc.poll() is None:
            return
        title = f"{state.definition.name} logs"
        pid_path = LOGS_ROOT / f"{state.definition.name}.tail.pid"
        with contextlib.suppress(FileNotFoundError):
            pid_path.unlink()
        state.terminal_pid_path = pid_path
        quoted_pid = shlex.quote(str(pid_path))
        quoted_log = shlex.quote(str(state.log_path))
        cmd = (
            f"printf '%d\\n' $$ > {quoted_pid} && "
            f"exec tail -n 200 -f {quoted_log}"
        )
        args = [segment.format(title=title, cmd=cmd) for segment in self._terminal_template]
        try:
            state.terminal_proc = subprocess.Popen(args, cwd=state.workdir, start_new_session=True)
        except Exception as exc:
            state.last_error = f"terminal launch failed: {exc}"
            state.terminal_pid_path = None
            return
        # Best-effort wait for the PID file to appear so we can reap the tail later.
        for _ in range(20):
            if pid_path.exists():
                break
            time.sleep(0.1)

    def _close_log(self, state: ServiceState) -> None:
        if state.log_handle:
            with contextlib.suppress(Exception):
                state.log_handle.flush()
                state.log_handle.close()
            state.log_handle = None
        self._cleanup_terminal_tail(state)
        term = state.terminal_proc
        if term:
            if term.poll() is None:
                with contextlib.suppress(Exception):
                    term.terminate()
                try:
                    term.wait(timeout=2)
                except Exception:
                    with contextlib.suppress(Exception):
                        term.kill()
            state.terminal_proc = None

    def _cleanup_terminal_tail(self, state: ServiceState) -> None:
        pid_path = state.terminal_pid_path
        if not pid_path:
            return
        pid = self._read_pid_file(pid_path)
        if pid:
            self._terminate_tail_pid(pid, state.log_path)
        with contextlib.suppress(Exception):
            pid_path.unlink()
        state.terminal_pid_path = None

    def _read_pid_file(self, path: Path) -> Optional[int]:
        try:
            raw = path.read_text().strip()
            return int(raw) if raw else None
        except Exception:
            return None

    def _terminate_tail_pid(self, pid: int, log_path: Path) -> None:
        if pid <= 0:
            return
        if not self._pid_targets_log(pid, log_path):
            return
        with contextlib.suppress(ProcessLookupError):
            os.kill(pid, signal.SIGTERM)
        for _ in range(20):
            if not self._pid_alive(pid):
                break
            time.sleep(0.1)
        else:
            with contextlib.suppress(ProcessLookupError):
                os.kill(pid, signal.SIGKILL)

    def _pid_alive(self, pid: int) -> bool:
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False

    def _pid_targets_log(self, pid: int, log_path: Path) -> bool:
        proc_root = Path("/proc")
        if not proc_root.exists():  # Fallback for platforms without /proc
            return True
        cmdline_path = proc_root / str(pid) / "cmdline"
        try:
            data = cmdline_path.read_bytes()
        except Exception:
            return False
        parts = [segment.decode("utf-8", "ignore") for segment in data.split(b"\0") if segment]
        if not parts:
            return False
        if "tail" not in parts[0]:
            return False
        target = str(log_path)
        return any(target in segment for segment in parts[1:])


# Router logging setup
ROUTER_LOG = LOGS_ROOT / "router.log"
ROUTER_LOG.parent.mkdir(parents=True, exist_ok=True)
LOGGER = logging.getLogger("unified_router")
if not LOGGER.handlers:
    file_handler = logging.FileHandler(ROUTER_LOG, encoding="utf-8")
    file_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    LOGGER.addHandler(file_handler)
LOGGER.setLevel(logging.INFO)


def _json_clone(payload: Any) -> Any:
    try:
        return json.loads(json.dumps(payload))
    except Exception:
        return payload


def _normalize_seed_hex(value: Any) -> str:
    text = str(value or "").strip().lower().replace("0x", "")
    if re.fullmatch(r"[0-9a-f]{64}", text or ""):
        return text
    return ""


def _is_sensitive_field_name(name: Any) -> bool:
    key = str(name or "").strip().lower().replace("-", "_")
    if not key:
        return False
    if key in SENSITIVE_FIELD_EXCEPTIONS:
        return False
    if key in SENSITIVE_FIELD_EXACT:
        return True
    return bool(SENSITIVE_FIELD_REGEX.search(key))


def _redact_sensitive_fields(payload: Any) -> Any:
    if isinstance(payload, dict):
        redacted: Dict[str, Any] = {}
        for key, value in payload.items():
            key_text = str(key)
            if _is_sensitive_field_name(key_text):
                redacted[key_text] = SENSITIVE_VALUE_PLACEHOLDER
            else:
                redacted[key_text] = _redact_sensitive_fields(value)
        return redacted
    if isinstance(payload, list):
        return [_redact_sensitive_fields(item) for item in payload]
    if isinstance(payload, tuple):
        return tuple(_redact_sensitive_fields(item) for item in payload)
    return payload


def _coerce_config_value(path: str, raw_value: Any, source_kind: str, spec: Dict[str, Any]) -> Tuple[Any, bool, Optional[str]]:
    value_type = str(spec.get("type") or "").strip().lower()
    default = spec.get("default")
    minimum = spec.get("min")
    maximum = spec.get("max")
    canonical = source_kind == "canonical"
    changed = source_kind != "canonical"

    def _range_fail(kind: str, parsed: Any) -> str:
        return f"{path}: {kind} out of range ({parsed!r}; expected {minimum!r}..{maximum!r})"

    if raw_value is None and source_kind == "default":
        return default, changed, None

    if value_type == "bool":
        if isinstance(raw_value, bool):
            parsed = raw_value
        elif isinstance(raw_value, (int, float)) and raw_value in (0, 1):
            parsed = bool(raw_value)
            changed = True
        elif isinstance(raw_value, str):
            text = raw_value.strip().lower()
            if text in ("1", "true", "yes", "on"):
                parsed = True
                changed = changed or (text != "true")
            elif text in ("0", "false", "no", "off"):
                parsed = False
                changed = changed or (text != "false")
            else:
                parsed = None
        else:
            parsed = None
        if parsed is None:
            msg = f"{path}: expected boolean, got {raw_value!r}"
            return default, True, msg if canonical else None
        if not isinstance(raw_value, bool):
            changed = True
        return parsed, changed, None

    if value_type == "int":
        try:
            if isinstance(raw_value, bool):
                raise ValueError("bool not allowed for int")
            parsed = int(raw_value)
        except Exception:
            msg = f"{path}: expected integer, got {raw_value!r}"
            return default, True, msg if canonical else None
        if minimum is not None and parsed < int(minimum):
            return default, True, _range_fail("integer", parsed) if canonical else None
        if maximum is not None and parsed > int(maximum):
            return default, True, _range_fail("integer", parsed) if canonical else None
        if parsed != raw_value or not isinstance(raw_value, int) or isinstance(raw_value, bool):
            changed = True
        return parsed, changed, None

    if value_type == "float":
        try:
            if isinstance(raw_value, bool):
                raise ValueError("bool not allowed for float")
            parsed = float(raw_value)
        except Exception:
            msg = f"{path}: expected number, got {raw_value!r}"
            return default, True, msg if canonical else None
        if minimum is not None and parsed < float(minimum):
            return default, True, _range_fail("number", parsed) if canonical else None
        if maximum is not None and parsed > float(maximum):
            return default, True, _range_fail("number", parsed) if canonical else None
        if parsed != raw_value or not isinstance(raw_value, (int, float)) or isinstance(raw_value, bool):
            changed = True
        return parsed, changed, None

    if value_type == "str":
        parsed = str(raw_value or "").strip()
        if not isinstance(raw_value, str):
            changed = True
        if isinstance(raw_value, str) and parsed != raw_value:
            changed = True
        if not parsed and str(default or "").strip():
            parsed = str(default).strip()
            changed = True
        return parsed, changed, None

    msg = f"{path}: unsupported schema type '{value_type}'"
    return default, True, msg


def _normalize_service_relay_sections(raw_cfg: Dict[str, Any], defaults: Dict[str, Any]) -> Tuple[Dict[str, dict], Dict[str, str], List[dict], bool, List[str], List[str]]:
    warnings: List[str] = []
    errors: List[str] = []
    changed = False

    raw_relays_value = raw_cfg.get("service_relays", {})
    raw_nodes_value = raw_cfg.get("nodes", [])
    raw_assignments_value = raw_cfg.get("service_assignments", {})

    raw_relays = raw_relays_value if isinstance(raw_relays_value, dict) else {}
    raw_nodes = raw_nodes_value if isinstance(raw_nodes_value, list) else []
    raw_assignments = raw_assignments_value if isinstance(raw_assignments_value, dict) else {}

    if "service_relays" in raw_cfg and not isinstance(raw_relays_value, dict):
        errors.append(f"service_relays: expected object, got {type(raw_relays_value).__name__}")
    if "nodes" in raw_cfg and not isinstance(raw_nodes_value, list):
        errors.append(f"nodes: expected list, got {type(raw_nodes_value).__name__}")
    if "service_assignments" in raw_cfg and not isinstance(raw_assignments_value, dict):
        errors.append(f"service_assignments: expected object, got {type(raw_assignments_value).__name__}")

    legacy_nodes: Dict[str, str] = {}
    for idx, node in enumerate(raw_nodes):
        if not isinstance(node, dict):
            warnings.append(f"nodes[{idx}] ignored (expected object)")
            changed = True
            continue
        name = str(node.get("name") or "").strip()
        seed = _normalize_seed_hex(node.get("seed_hex"))
        if not name:
            continue
        if seed:
            legacy_nodes[name] = seed
        elif node.get("seed_hex") not in (None, "", seed):
            warnings.append(f"nodes[{idx}].seed_hex ignored (invalid seed)")
            changed = True

    service_names = [definition.name for definition in ServiceWatchdog.DEFINITIONS]
    default_relays = defaults.get("service_relays", {}) if isinstance(defaults.get("service_relays"), dict) else {}

    normalized_relays: Dict[str, dict] = {}
    normalized_assignments: Dict[str, str] = {}
    normalized_nodes: List[dict] = []
    now = int(time.time())

    for service_name in service_names:
        default_entry = default_relays.get(service_name, {})
        entry_value = raw_relays.get(service_name, {})
        entry = entry_value if isinstance(entry_value, dict) else {}
        if service_name in raw_relays and not isinstance(entry_value, dict):
            errors.append(f"service_relays.{service_name}: expected object, got {type(entry_value).__name__}")

        seed_source = "default"
        seed_key = "service_relays.default"
        raw_seed = entry.get("seed_hex")
        if raw_seed not in (None, ""):
            seed_source = "canonical"
            seed_key = f"service_relays.{service_name}.seed_hex"
        else:
            assigned_name = str(raw_assignments.get(service_name) or "").strip()
            if assigned_name and assigned_name in legacy_nodes:
                raw_seed = legacy_nodes.get(assigned_name)
                seed_source = "legacy"
                seed_key = f"nodes[{assigned_name}]"
            else:
                raw_seed = default_entry.get("seed_hex")
        seed_hex = _normalize_seed_hex(raw_seed)
        if not seed_hex:
            if seed_source == "canonical":
                errors.append(f"{seed_key}: expected 64-char lowercase hex seed")
            else:
                seed_hex = _normalize_seed_hex(default_entry.get("seed_hex")) or generate_seed_hex()
                changed = True
                warnings.append(f"Generated replacement seed for service '{service_name}'")
        elif seed_source == "legacy":
            changed = True
            warnings.append(f"Promoted legacy seed mapping for service '{service_name}' from nodes/service_assignments")

        relay_name = Router._relay_name_static(service_name, seed_hex)
        existing_name = str(entry.get("name") or "").strip()
        if existing_name and existing_name != relay_name:
            changed = True
            warnings.append(f"Normalized relay name for service '{service_name}' to '{relay_name}'")
        elif not existing_name:
            changed = True

        created_raw = entry.get("created_at", default_entry.get("created_at", now))
        try:
            created_at = int(created_raw)
        except Exception:
            created_at = now
            changed = True
            if "created_at" in entry:
                warnings.append(f"service_relays.{service_name}.created_at invalid; reset to current time")
        if created_at <= 0:
            created_at = now
            changed = True

        normalized_entry = {
            "seed_hex": seed_hex,
            "name": relay_name,
            "created_at": created_at,
        }
        normalized_relays[service_name] = normalized_entry
        normalized_assignments[service_name] = relay_name
        normalized_nodes.append({"name": relay_name, "seed_hex": seed_hex})

    for service_name in raw_relays.keys():
        if service_name not in normalized_relays:
            warnings.append(f"service_relays.{service_name} removed (unknown service)")
            changed = True

    return normalized_relays, normalized_assignments, normalized_nodes, changed, warnings, errors


def _normalize_router_config(raw_cfg: Any) -> Tuple[dict, bool, List[str], List[str]]:
    defaults = _default_config()
    warnings: List[str] = []
    errors: List[str] = []
    changed = False

    if not isinstance(raw_cfg, dict):
        warnings.append("Config root was not an object; replaced with defaults")
        raw: Dict[str, Any] = {}
        changed = True
    else:
        raw = _json_clone(raw_cfg)

    schema_raw = raw.get("schema", raw.get("schema_version", raw.get("version", ROUTER_CONFIG_SCHEMA_VERSION)))
    try:
        schema_in = int(schema_raw)
    except Exception:
        schema_in = 0
        changed = True
        warnings.append(f"Config schema value {schema_raw!r} invalid; treating as legacy schema 0")
    if schema_in != ROUTER_CONFIG_SCHEMA_VERSION:
        changed = True
        src_tag = ROUTER_CONFIG_MIGRATION_MAP.get(schema_in, f"unknown_v{schema_in}")
        dst_tag = ROUTER_CONFIG_MIGRATION_MAP.get(ROUTER_CONFIG_SCHEMA_VERSION, f"v{ROUTER_CONFIG_SCHEMA_VERSION}")
        warnings.append(f"Migrating config schema from {src_tag} to {dst_tag}")

    normalized: Dict[str, Any] = {"schema": ROUTER_CONFIG_SCHEMA_VERSION}

    # Targets
    raw_targets_value = raw.get("targets", {})
    raw_targets = raw_targets_value if isinstance(raw_targets_value, dict) else {}
    if "targets" in raw and not isinstance(raw_targets_value, dict):
        errors.append(f"targets: expected object, got {type(raw_targets_value).__name__}")
    targets_out: Dict[str, str] = {}
    for target_key, default_target in defaults.get("targets", {}).items():
        source_kind = "default"
        raw_value = default_target
        if target_key in raw_targets:
            source_kind = "canonical"
            raw_value = raw_targets.get(target_key)
        else:
            for legacy_key in ROUTER_TARGET_LEGACY_KEYS.get(target_key, ()):
                if legacy_key in raw:
                    source_kind = "legacy"
                    raw_value = raw.get(legacy_key)
                    warnings.append(f"Promoted legacy key '{legacy_key}' -> 'targets.{target_key}'")
                    break
        text = str(raw_value or "").strip()
        if not text:
            if source_kind == "canonical":
                errors.append(f"targets.{target_key}: expected non-empty URL string")
                text = str(default_target)
            else:
                text = str(default_target)
                if source_kind == "legacy":
                    warnings.append(f"targets.{target_key}: invalid legacy value {raw_value!r}; using default")
            changed = True
        if source_kind != "canonical":
            changed = True
        targets_out[target_key] = text
    for extra_key, extra_value in raw_targets.items():
        if extra_key in targets_out:
            continue
        text = str(extra_value or "").strip()
        if not text:
            warnings.append(f"targets.{extra_key} ignored (empty value)")
            changed = True
            continue
        targets_out[extra_key] = text
    normalized["targets"] = targets_out

    # Typed sections
    known_top = {
        "schema",
        "schema_version",
        "version",
        "targets",
        "service_relays",
        "nodes",
        "service_assignments",
        "service_publication",
        "ticket_keys",
        "ticket_accepted_kids",
    }
    known_top.update(ROUTER_SECTION_FIELD_SPECS.keys())
    known_legacy_top = set()
    for section_name, section_spec in ROUTER_SECTION_FIELD_SPECS.items():
        section_raw_value = raw.get(section_name, {})
        section_raw = section_raw_value if isinstance(section_raw_value, dict) else {}
        if section_name in raw and not isinstance(section_raw_value, dict):
            errors.append(f"{section_name}: expected object, got {type(section_raw_value).__name__}")
        out_section: Dict[str, Any] = {}
        allowed_keys = set(section_spec.keys())
        for field_name, field_spec in section_spec.items():
            legacy_keys = tuple(field_spec.get("legacy") or ())
            known_legacy_top.update(legacy_keys)
            source_kind = "default"
            raw_value = field_spec.get("default")
            if field_name in section_raw:
                raw_value = section_raw.get(field_name)
                source_kind = "canonical"
            else:
                for legacy_key in legacy_keys:
                    if legacy_key in section_raw:
                        raw_value = section_raw.get(legacy_key)
                        source_kind = "legacy"
                        warnings.append(f"Promoted legacy key '{section_name}.{legacy_key}' -> '{section_name}.{field_name}'")
                        break
                    if legacy_key in raw:
                        raw_value = raw.get(legacy_key)
                        source_kind = "legacy"
                        warnings.append(f"Promoted legacy key '{legacy_key}' -> '{section_name}.{field_name}'")
                        break
            value, field_changed, field_error = _coerce_config_value(
                f"{section_name}.{field_name}",
                raw_value,
                source_kind,
                field_spec,
            )
            if field_error:
                errors.append(field_error)
            if source_kind == "legacy" and field_error is None:
                changed = True
            if source_kind == "legacy" and field_error is not None:
                warnings.append(f"{section_name}.{field_name}: invalid promoted legacy value; using default")
            if field_changed:
                changed = True
            out_section[field_name] = value

        for raw_key in section_raw.keys():
            if raw_key in allowed_keys:
                continue
            if any(raw_key == lk for spec in section_spec.values() for lk in tuple(spec.get("legacy") or ())):
                continue
            warnings.append(f"{section_name}.{raw_key} ignored (unknown key)")
            changed = True

        normalized[section_name] = out_section

    known_legacy_top.update(key for values in ROUTER_TARGET_LEGACY_KEYS.values() for key in values)
    for top_key in raw.keys():
        if top_key in known_top or top_key in known_legacy_top:
            continue
        warnings.append(f"{top_key} ignored (unknown top-level key)")
        changed = True

    raw_publication_value = raw.get("service_publication", defaults.get("service_publication", {}))
    raw_publication = raw_publication_value if isinstance(raw_publication_value, dict) else {}
    if "service_publication" in raw and not isinstance(raw_publication_value, dict):
        errors.append(f"service_publication: expected object, got {type(raw_publication_value).__name__}")
    default_publication = defaults.get("service_publication", {}) if isinstance(defaults.get("service_publication"), dict) else {}
    normalized_publication: Dict[str, Dict[str, Any]] = {}
    for service_name in SERVICE_TARGETS.keys():
        raw_entry = raw_publication.get(service_name, default_publication.get(service_name, {}))
        if raw_entry is None:
            raw_entry = {}
        if not isinstance(raw_entry, dict):
            errors.append(f"service_publication.{service_name}: expected object, got {type(raw_entry).__name__}")
            raw_entry = default_publication.get(service_name, {}) if isinstance(default_publication.get(service_name), dict) else {}
        normalized_publication[service_name] = _json_clone(raw_entry) if isinstance(raw_entry, dict) else {}
    for extra_key in raw_publication.keys():
        if extra_key in normalized_publication:
            continue
        warnings.append(f"service_publication.{extra_key} ignored (unknown service)")
        changed = True
    normalized["service_publication"] = normalized_publication

    raw_ticket_keys_value = raw.get("ticket_keys", defaults.get("ticket_keys", {}))
    raw_ticket_keys = raw_ticket_keys_value if isinstance(raw_ticket_keys_value, dict) else {}
    if "ticket_keys" in raw and not isinstance(raw_ticket_keys_value, dict):
        errors.append(f"ticket_keys: expected object, got {type(raw_ticket_keys_value).__name__}")
    default_ticket_keys = defaults.get("ticket_keys", {}) if isinstance(defaults.get("ticket_keys"), dict) else {}
    normalized["ticket_keys"] = _normalize_ticket_key_map(raw_ticket_keys, fallback=default_ticket_keys)
    if raw_ticket_keys and normalized["ticket_keys"] != raw_ticket_keys:
        changed = True
        warnings.append("ticket_keys normalized (empty kid/secret entries removed)")

    raw_ticket_kids_value = raw.get("ticket_accepted_kids", defaults.get("ticket_accepted_kids", []))
    if "ticket_accepted_kids" in raw and not isinstance(raw_ticket_kids_value, (list, str)):
        errors.append(
            "ticket_accepted_kids: expected list or comma-delimited string, "
            f"got {type(raw_ticket_kids_value).__name__}"
        )
    default_ticket_kids = defaults.get("ticket_accepted_kids", []) if isinstance(defaults.get("ticket_accepted_kids"), list) else []
    normalized_ticket_kids = _normalize_ticket_kid_list(raw_ticket_kids_value, fallback=default_ticket_kids)
    normalized["ticket_accepted_kids"] = normalized_ticket_kids

    relays, assignments, nodes, relay_changed, relay_warnings, relay_errors = _normalize_service_relay_sections(raw, defaults)
    normalized["service_relays"] = relays
    normalized["service_assignments"] = assignments
    normalized["nodes"] = nodes
    if relay_changed:
        changed = True
    warnings.extend(relay_warnings)
    errors.extend(relay_errors)

    return normalized, changed, warnings, errors


def _default_config() -> dict:
    service_relays = {}
    nodes = []
    assignments = {}
    now = int(time.time())
    targets = dict(DEFAULT_TARGETS)
    service_publication = _default_service_publication()

    for definition in ServiceWatchdog.DEFINITIONS:
        svc = definition.name
        seed = generate_seed_hex()
        name = Router._relay_name_static(svc, seed)
        relay_entry = {
            "seed_hex": seed.lower().replace("0x", ""),
            "name": name,
            "created_at": now,
        }
        service_relays[svc] = relay_entry
        nodes.append({"name": name, "seed_hex": relay_entry["seed_hex"]})
        assignments[svc] = name

    return {
        "schema": ROUTER_CONFIG_SCHEMA_VERSION,
        "targets": targets,
        "api": {
            "enable": True,
            "host": "127.0.0.1",
            "port": 9071,
        },
        "nkn": {
            "enable": True,
            "dm_retries": 2,
            "resolve_timeout_seconds": 20,
            "rpc_max_request_b": 512 * 1024,
            "rpc_max_response_b": 2 * 1024 * 1024,
        },
        "http": {
            "workers": 4,
            "max_body_b": 2 * 1024 * 1024,
            "verify_default": True,
            "chunk_raw_b": 12 * 1024,
            "chunk_upload_b": 600 * 1024,
            "heartbeat_s": 10,
            "batch_lines": 24,
            "batch_latency": 0.08,
            "retries": 4,
            "retry_backoff": 0.5,
            "retry_cap": 4.0,
        },
        "bridge": {
            "num_subclients": 2,
            "seed_ws": "",
            "self_probe_ms": 12000,
            "self_probe_fails": 3,
        },
        "watchdog": {
            "port_reclaim_enabled": True,
            "port_reclaim_force": False,
            "activation_timeout_seconds": 30.0,
            "activation_stability_seconds": 6.0,
            "health_check_interval_seconds": 2.0,
            "health_failure_threshold": 3,
            "reclaim_sigint_wait_seconds": 0.7,
            "reclaim_sigterm_wait_seconds": 1.0,
            "reclaim_sigkill_wait_seconds": 1.0,
        },
        "cloudflared": {
            "enable": False,
            "auto_install_cloudflared": False,
            "binary_path": "",
            "protocol": "http2",
            "restart_initial_seconds": 2.0,
            "restart_cap_seconds": 90.0,
        },
        "feature_flags": {
            "router_control_plane_api": True,
            "resolver_auto_apply": True,
            "cloudflared_manager": False,
        },
        "marketplace": {
            "enable_catalog": True,
            "provider_id": "hydra-router",
            "provider_label": "Hydra Router",
            "provider_network": "hydra",
            "provider_contact": "",
            "default_currency": "USDC",
            "default_unit": "request",
            "default_price_per_unit": 0.0,
            "include_unhealthy": True,
            "catalog_ttl_seconds": 20,
        },
        "marketplace_sync": {
            "enable_auto_publish": False,
            "target_urls": "",
            "auth_token": "",
            "auth_token_env": "HYDRA_MARKETPLACE_SYNC_TOKEN",
            "auth_header": "Authorization",
            "auth_scheme": "Bearer",
            "publish_interval_seconds": 45,
            "publish_timeout_seconds": 6.0,
            "max_backoff_seconds": 300,
            "include_unhealthy": True,
        },
        "auth": {
            "require_service_rpc_ticket": True,
            "require_billing_preflight": True,
            "require_quote_for_billable": True,
            "require_charge_for_billable": True,
            "allow_unauthenticated_resolve": True,
            "clock_skew_seconds": 30,
            "replay_cache_seconds": 900,
            "ticket_ttl_seconds": 300,
            "active_kid": DEFAULT_TICKET_KID,
            "default_scope": "infer",
        },
        "ticket_keys": {
            DEFAULT_TICKET_KID: DEFAULT_TICKET_SECRET,
        },
        "ticket_accepted_kids": [DEFAULT_TICKET_KID],
        "service_publication": service_publication,
        "service_relays": service_relays,
        "nodes": nodes,
        "service_assignments": assignments,
    }


def load_config() -> dict:
    created_default = False
    if not CONFIG_PATH.exists():
        default_cfg = _default_config()
        CONFIG_PATH.write_text(json.dumps(default_cfg, indent=2))
        created_default = True
        print(f"→ wrote default config {CONFIG_PATH}")

    try:
        raw_cfg = json.loads(CONFIG_PATH.read_text())
    except Exception as exc:
        raise SystemExit(f"Invalid JSON in config {CONFIG_PATH}: {exc}") from exc

    normalized, changed, warnings, errors = _normalize_router_config(raw_cfg)
    for warning in warnings:
        LOGGER.warning("Config migration: %s", warning)

    if errors:
        details = "\n".join(f"- {item}" for item in errors)
        raise SystemExit(
            f"Config validation failed for {CONFIG_PATH}:\n{details}\n"
            "Fix the listed keys or remove the file to regenerate defaults."
        )

    if changed or created_default:
        try:
            CONFIG_PATH.write_text(json.dumps(normalized, indent=2))
            LOGGER.info("Config normalized and saved to %s", CONFIG_PATH)
        except Exception as exc:
            raise SystemExit(f"Failed to persist normalized config {CONFIG_PATH}: {exc}") from exc

    return normalized


# ──────────────────────────────────────────────────────────────
# Node.js bridge scaffold (shared for all RelayNodes)
# ──────────────────────────────────────────────────────────────
BRIDGE_DIR = BASE_DIR / "bridge-node"
BRIDGE_JS = BRIDGE_DIR / "nkn_bridge.js"
PKG_JSON = BRIDGE_DIR / "package.json"
BRIDGE_SRC = r"""
'use strict';
const nkn = require('nkn-sdk');
const readline = require('readline');

const SEED_HEX = (process.env.NKN_SEED_HEX || '').toLowerCase().replace(/^0x/,'');
const IDENT = String(process.env.NKN_IDENTIFIER || 'relay');
const NUM = parseInt(process.env.NKN_NUM_SUBCLIENTS || '2', 10) || 2;
const SEED_WS = (process.env.NKN_BRIDGE_SEED_WS || '').split(',').map(s=>s.trim()).filter(Boolean);
const PROBE_EVERY_MS = parseInt(process.env.NKN_SELF_PROBE_MS || '12000', 10);
const PROBE_FAILS_EXIT = parseInt(process.env.NKN_SELF_PROBE_FAILS || '3', 10);

function out(obj){
  try{ process.stdout.write(JSON.stringify(obj)+'\n'); }
  catch(e){ /* ignore */ }
}

function spawn(){
  if(!/^[0-9a-f]{64}$/.test(SEED_HEX)){
    out({type:'crit', msg:'bad seed'});
    process.exit(1);
  }

  const client = new nkn.MultiClient({
    seed: SEED_HEX,
    identifier: IDENT,
    numSubClients: NUM,
    seedWsAddr: SEED_WS.length ? SEED_WS : undefined,
    wsConnHeartbeatTimeout: 120000,
  });

  let probeFails = 0;
  let probeTimer = null;
  function startProbe(){
    stopProbe();
    probeTimer = setInterval(async ()=>{
      try {
        await client.send(String(client.addr||''), JSON.stringify({event:'relay.selfprobe', ts: Date.now()}), {noReply:true});
        probeFails = 0;
        out({type:'status', state:'probe_ok'});
      } catch (e){
        probeFails++;
        out({type:'status', state:'probe_fail', fails:probeFails, msg:String(e&&e.message||e)});
        if (probeFails >= PROBE_FAILS_EXIT){
          out({type:'status', state:'probe_exit'});
          process.exit(3);
        }
      }
    }, PROBE_EVERY_MS);
  }
  function stopProbe(){
    if (probeTimer){ clearInterval(probeTimer); probeTimer=null; }
  }

  client.on('connect', ()=>{
    out({type:'ready', address:String(client.addr||''), ts: Date.now()});
    startProbe();
  });
  client.on('error', (e)=>{ out({type:'status', state:'error', msg:String(e&&e.message||e)}); process.exit(2); });
  client.on('close', ()=>{ out({type:'status', state:'close'}); process.exit(2); });

  client.on('message', (a, b)=>{
    try{
      let src, payload;
      if (a && typeof a==='object' && a.payload!==undefined){ src=String(a.src||''); payload=a.payload; }
      else { src=String(a||''); payload=b; }
      const s = Buffer.isBuffer(payload) ? payload.toString('utf8') : (typeof payload==='string'? payload : String(payload));
      let parsed=null; try{ parsed=JSON.parse(s); }catch{}
      out({type:'nkn-dm', src, msg: parsed || {event:'<non-json>', raw:s}});
    }catch(e){ out({type:'err', msg:String(e&&e.message||e)}); }
  });

  const rl = readline.createInterface({input: process.stdin});
  rl.on('line', line=>{
    let cmd; try{ cmd=JSON.parse(line); }catch{return; }
    if(cmd && cmd.type==='dm' && cmd.to && cmd.data){
      const opts = cmd.opts || {noReply:true};
      client.send(cmd.to, JSON.stringify(cmd.data), opts).catch(err=>{
        out({type:'status', state:'send_error', msg:String(err&&err.message||err)});
      });
    }
  });

  process.on('exit', ()=>{ stopProbe(); if(probeTimer){ clearInterval(probeTimer);} });
  process.on('unhandledRejection', e=>{ out({type:'status', state:'unhandledRejection', msg:String(e)}); process.exit(1); });
  process.on('uncaughtException', e=>{ out({type:'status', state:'uncaughtException', msg:String(e)}); process.exit(1); });
}

spawn();
"""


def ensure_bridge() -> None:
    if not BRIDGE_DIR.exists():
        BRIDGE_DIR.mkdir(parents=True)
    if not shutil.which("node"):
        raise SystemExit("Node.js binary 'node' not found; install Node.js to run the router.")
    if not shutil.which("npm"):
        raise SystemExit("npm not found; install Node.js/npm to run the router.")
    if not PKG_JSON.exists():
        subprocess.check_call(["npm", "init", "-y"], cwd=BRIDGE_DIR)
        subprocess.check_call(["npm", "install", "nkn-sdk@^1.3.6"], cwd=BRIDGE_DIR)
    if not BRIDGE_JS.exists() or BRIDGE_JS.read_text() != BRIDGE_SRC:
        BRIDGE_JS.write_text(BRIDGE_SRC)


# ──────────────────────────────────────────────────────────────
# Stats tracking for address book and service usage
# ──────────────────────────────────────────────────────────────
STATS_DIR = BASE_DIR / ".stats"
STATS_DIR.mkdir(parents=True, exist_ok=True)
SERVICE_STATS_FILE = STATS_DIR / "service_stats.jsonl"
ADDRESS_BOOK_FILE = STATS_DIR / "address_book.jsonl"
EGRESS_STATS_FILE = STATS_DIR / "egress_stats.jsonl"


class StatsTracker:
    """Track service utilization, address book, and egress bandwidth."""

    def __init__(self):
        self.lock = threading.Lock()
        # In-memory stats for fast access
        self.service_history: Dict[str, List[Tuple[float, int]]] = {}  # service -> [(timestamp, requests_count)]
        self.address_book: Dict[str, Dict[str, Any]] = {}  # nkn_addr -> {first_seen, last_seen, services: {svc: {...}}}
        self.egress_stats: Dict[str, Dict[str, Any]] = {}  # service -> {bytes_sent, request_count, users: {addr: bytes}}

        # Load existing stats
        self._load_stats()

    def _load_stats(self):
        """Load stats from jsonl files."""
        try:
            if SERVICE_STATS_FILE.exists():
                for line in SERVICE_STATS_FILE.read_text().splitlines():
                    if not line.strip():
                        continue
                    data = json.loads(line)
                    svc = data.get("service")
                    if svc:
                        self.service_history.setdefault(svc, []).append((data.get("ts", 0), data.get("count", 1)))
        except Exception:
            pass

        try:
            if ADDRESS_BOOK_FILE.exists():
                for line in ADDRESS_BOOK_FILE.read_text().splitlines():
                    if not line.strip():
                        continue
                    data = json.loads(line)
                    addr = data.get("addr")
                    if addr:
                        # Normalize legacy entries
                        data.setdefault("services", {})
                        data.setdefault("total_requests", 0)
                        data.setdefault("bytes_in", 0)
                        data.setdefault("bytes_out", 0)
                        data.setdefault("active_seconds", 0.0)
                        self.address_book[addr] = data
        except Exception:
            pass

        try:
            if EGRESS_STATS_FILE.exists():
                for line in EGRESS_STATS_FILE.read_text().splitlines():
                    if not line.strip():
                        continue
                    data = json.loads(line)
                    svc = data.get("service")
                    if svc:
                        self.egress_stats[svc] = data
        except Exception:
            pass

    def touch_address(self, nkn_addr: str, service: Optional[str] = None) -> None:
        """Ensure an address is present in the address book."""
        if not nkn_addr:
            return
        with self.lock:
            now = time.time()
            entry = self.address_book.setdefault(
                nkn_addr,
                {
                    "addr": nkn_addr,
                    "first_seen": now,
                    "last_seen": now,
                    "services": {},
                    "total_requests": 0,
                    "bytes_in": 0,
                    "bytes_out": 0,
                    "active_seconds": 0.0,
                },
            )
            entry["last_seen"] = now
            if service:
                entry["services"].setdefault(service, {}).setdefault("count", 0)

    def record_request(self, service: str, nkn_addr: str, bytes_out: int = 0, bytes_in: int = 0, duration_s: float = 0.0):
        """Record a service request from an NKN address with byte/duration detail."""
        with self.lock:
            now = time.time()

            # Update service history
            self.service_history.setdefault(service, []).append((now, 1))
            # Keep only last 24 hours
            cutoff = now - 86400
            self.service_history[service] = [(ts, cnt) for ts, cnt in self.service_history[service] if ts > cutoff]

            # Update address book
            if nkn_addr and nkn_addr != "—":
                entry = self.address_book.setdefault(
                    nkn_addr,
                    {
                        "addr": nkn_addr,
                        "first_seen": now,
                        "last_seen": now,
                        "services": {},
                        "total_requests": 0,
                        "bytes_in": 0,
                        "bytes_out": 0,
                        "active_seconds": 0.0,
                    },
                )
                entry["last_seen"] = now
                entry["total_requests"] = entry.get("total_requests", 0) + 1
                entry["bytes_in"] = entry.get("bytes_in", 0) + bytes_in
                entry["bytes_out"] = entry.get("bytes_out", 0) + bytes_out
                entry["active_seconds"] = entry.get("active_seconds", 0.0) + max(0.0, duration_s)

                svc_entry = entry["services"].setdefault(
                    service,
                    {"count": 0, "bytes_in": 0, "bytes_out": 0, "first_seen": now, "last_seen": now, "active_seconds": 0.0},
                )
                svc_entry["count"] = svc_entry.get("count", 0) + 1
                svc_entry["bytes_in"] = svc_entry.get("bytes_in", 0) + bytes_in
                svc_entry["bytes_out"] = svc_entry.get("bytes_out", 0) + bytes_out
                svc_entry["last_seen"] = now
                svc_entry["active_seconds"] = svc_entry.get("active_seconds", 0.0) + max(0.0, duration_s)
                svc_entry.setdefault("first_seen", now)

                # Write address book entry (append for durability)
                try:
                    with open(ADDRESS_BOOK_FILE, "a") as f:
                        f.write(json.dumps(entry) + "\n")
                except Exception:
                    pass

            # Update egress stats
            if bytes_out > 0:
                if service not in self.egress_stats:
                    self.egress_stats[service] = {
                        "service": service,
                        "bytes_sent": 0,
                        "request_count": 0,
                        "users": {}
                    }
                entry = self.egress_stats[service]
                entry["bytes_sent"] += bytes_out
                entry["request_count"] += 1
                if nkn_addr and nkn_addr != "—":
                    entry["users"].setdefault(nkn_addr, 0)
                    entry["users"][nkn_addr] += bytes_out

                # Write egress stats periodically (every 10 requests)
                if entry["request_count"] % 10 == 0:
                    try:
                        with open(EGRESS_STATS_FILE, "a") as f:
                            f.write(json.dumps(entry) + "\n")
                    except Exception:
                        pass

            # Write service stats
            try:
                with open(SERVICE_STATS_FILE, "a") as f:
                    f.write(json.dumps({"ts": now, "service": service, "count": 1}) + "\n")
            except Exception:
                pass

    def get_service_timeline(self, hours: int = 24) -> Dict[str, List[Tuple[float, int]]]:
        """Get service utilization timeline for the last N hours."""
        with self.lock:
            cutoff = time.time() - (hours * 3600)
            result = {}
            for svc, history in self.service_history.items():
                result[svc] = [(ts, cnt) for ts, cnt in history if ts > cutoff]
            return result

    def get_address_book(self) -> List[Dict[str, Any]]:
        """Get all addresses sorted by last seen."""
        with self.lock:
            return sorted(self.address_book.values(), key=lambda x: x.get("last_seen", 0), reverse=True)

    def get_egress_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get egress bandwidth stats."""
        with self.lock:
            return dict(self.egress_stats)


# ──────────────────────────────────────────────────────────────
# Animated Hydra System
# ──────────────────────────────────────────────────────────────
class HydraPolyp:
    """Single polyp of the hydra with animated tentacles."""
    def __init__(self, x: int, y: int, polyp_id: int = 0):
        self.x = x
        self.y = y
        self.polyp_id = polyp_id
        self.tentacle_count = 7
        self.phase = (polyp_id * math.pi / 3) + (time.time() * 0.5)  # Offset phases
        self.activity = 0.0  # 0.0 to 1.0
        self.size = 1.0

    def update(self, activity_level: float):
        """Update animation state based on activity."""
        self.activity = min(1.0, self.activity * 0.9 + activity_level * 0.1)  # Smooth decay
        self.phase = (time.time() * 0.5) + (self.polyp_id * math.pi / 3)

    def get_tentacle_positions(self) -> List[Tuple[int, int]]:
        """Calculate animated tentacle tip positions."""
        positions = []
        wiggle = math.sin(self.phase) * (1 + self.activity * 1.5)
        for i in range(self.tentacle_count):
            angle = (i / self.tentacle_count) * 2 * math.pi
            segs = 2 + int(self.activity * 3)
            for seg in range(1, segs + 1):
                length = seg + 1 + self.activity
                tx = int(self.x + math.cos(angle + wiggle * 0.4) * length)
                ty = int(self.y + math.sin(angle + wiggle * 0.3) * length * 0.6)  # Squash Y
                positions.append((tx, ty))
        return positions

class Hydra:
    """Multi-polyp hydra organism that reacts to router activity."""
    def __init__(self, base_x: int = 8, base_y: int = 20):
        self.base_x = base_x
        self.base_y = base_y
        self.polyps = [HydraPolyp(base_x, base_y - 10, 0)]
        self.stalk_segments = 14
        self.activity_history = deque(maxlen=100)
        self.last_activity_time = time.time()
        self.division_threshold = 0.8  # Activity level to trigger polyp division
        self.max_polyps = 5

    def feed_activity(self, kind: str, intensity: float = 0.5):
        """Feed router activity to the hydra."""
        self.activity_history.append((time.time(), kind, intensity))
        self.last_activity_time = time.time()

        # Calculate current activity level
        now = time.time()
        recent = [i for t, k, i in self.activity_history if now - t < 2.0]
        activity_level = sum(recent) / max(1, len(recent))

        # Update all polyps
        for polyp in self.polyps:
            polyp.update(activity_level)

        # Division: add polyp if sustained high activity
        if activity_level > self.division_threshold and len(self.polyps) < self.max_polyps:
            if len([i for t, k, i in self.activity_history if now - t < 5.0]) > 20:
                self._divide_polyp()
        else:
            if now - self.last_activity_time > 30 and len(self.polyps) > 1:
                self.polyps.pop()

    def current_activity(self) -> float:
        now = time.time()
        recent = [i for t, _, i in self.activity_history if now - t < 3.0]
        return min(1.0, sum(recent) / max(1, len(recent))) if recent else 0.0

    def _divide_polyp(self):
        """Bud a new polyp (biological division)."""
        if len(self.polyps) >= self.max_polyps:
            return
        # Place new polyp offset from base
        offset = len(self.polyps) * 2
        new_polyp = HydraPolyp(self.base_x + offset, self.base_y - 10 + offset, len(self.polyps))
        self.polyps.append(new_polyp)

    def render(self, stdscr, y_offset: int = 5):
        """Render the hydra to curses screen."""
        if not curses:
            return

        try:
            # Draw stalk from bottom up
            for i in range(self.stalk_segments):
                y = y_offset + self.stalk_segments - i
                x = self.base_x + int(math.sin(time.time() + i * 0.3) * 1.5)
                if 0 <= y < curses.LINES - 1 and 0 <= x < curses.COLS - 1:
                    stdscr.addstr(y, x, "│", curses.color_pair(6))

            # Draw each polyp
            for polyp in self.polyps:
                py = y_offset + (self.base_y - polyp.y)
                px = polyp.x

                # Draw body (pulsing size based on activity)
                body_char = "●" if polyp.activity > 0.3 else "○"
                if 0 <= py < curses.LINES - 1 and 0 <= px < curses.COLS - 1:
                    color = curses.color_pair(7) if polyp.activity > 0.5 else curses.color_pair(6)
                    stdscr.addstr(py, px, body_char, color | curses.A_BOLD)

                # Draw tentacles
                for tx, ty in polyp.get_tentacle_positions():
                    ty_screen = y_offset + (self.base_y - ty)
                    if 0 <= ty_screen < curses.LINES - 1 and 0 <= tx < curses.COLS - 1:
                        stdscr.addstr(ty_screen, tx, "~", curses.color_pair(6))

        except curses.error:
            pass  # Ignore boundary errors


# ──────────────────────────────────────────────────────────────
# Enhanced Nested Menu UI
# ──────────────────────────────────────────────────────────────
class EnhancedUI:
    """Enhanced nested menu interface with Config, Statistics, Address Book, Ingress, and Egress views."""

    MENU_ITEMS = ["Config", "Statistics", "Address Book", "Ingress", "Egress", "Debug"]

    def __init__(self, enabled: bool, config_path: Path):
        self.enabled = enabled and curses is not None and sys.stdout.isatty()
        self.config_path = config_path
        self.events: "queue.Queue[tuple[str, str, str, str]]" = queue.Queue()
        self.nodes: Dict[str, dict] = {}
        self.services: Dict[str, dict] = {}
        self.daemon_info: Optional[dict] = None
        self.stop = threading.Event()
        self.action_handler: Optional[Callable[[dict], None]] = None

        # Menu state
        self.current_view = "main"  # main, config, stats, addressbook, ingress, egress
        self.main_menu_index = 0
        self.scroll_offset = 0
        self.selected_service = None
        self.selected_address = None
        self.show_qr = False
        self.qr_data = ""
        self.qr_label = ""

        # Activity tracking
        self.activity: Deque[Tuple[str, str, str, str]] = deque(maxlen=500)
        self.flow_logs: Deque[Dict[str, str]] = deque(maxlen=800)
        self.debug_tab_index: int = 0
        self.debug_scroll_offsets: Dict[str, int] = {}

        # Stats tracker
        self.stats = StatsTracker()

        # Service configuration (enabled/disabled)
        self.service_config: Dict[str, bool] = {}  # service_name -> enabled
        self._load_service_config()

        # Security settings
        self.port_isolation_enabled = True  # Default ON for security
        self._load_security_config()

        # Animated Hydra
        self.hydra = Hydra(base_x=8, base_y=20)
        self.brutalist_mode = True  # Use brutalist UI layout
        self.network_state: str = "online"  # online, offline, hard_offline
        self.offline_since: float = 0.0

    def _load_service_config(self):
        """Load service enabled/disabled state from config."""
        try:
            if self.config_path.exists():
                cfg = json.loads(self.config_path.read_text())
                # Normalize services entries that may be raw counts
                svc_states = cfg.get("service_states", {})
                if isinstance(svc_states, dict):
                    for svc_name, enabled in svc_states.items():
                        self.service_config[svc_name] = bool(enabled)
                for svc_name, enabled in cfg.get("service_states", {}).items():
                    self.service_config[svc_name] = bool(enabled)
                for node_cfg in cfg.get("nodes", []):
                    for svc in node_cfg.get("services", []):
                        svc_name = svc if isinstance(svc, str) else svc.get("name")
                        if svc_name:
                            self.service_config.setdefault(svc_name, True)
            elif CONFIG_PATH.exists():
                cfg = json.loads(CONFIG_PATH.read_text())
                for svc_name, enabled in cfg.get("service_states", {}).items():
                    self.service_config[svc_name] = bool(enabled)
        except Exception:
            pass

    def _save_service_config(self):
        """Save service configuration to config file."""
        try:
            if self.config_path.exists():
                cfg = json.loads(self.config_path.read_text())
            else:
                cfg = {"nodes": []}

            # Update service enabled states in config
            # Note: This is a simplified approach; actual implementation would need
            # to properly map services to nodes and update the config structure
            cfg["service_states"] = self.service_config
            cfg["security"] = {
                "port_isolation_enabled": self.port_isolation_enabled
            }

            self.config_path.write_text(json.dumps(cfg, indent=2))
            # Mirror to main CONFIG_PATH for compatibility (best-effort)
            try:
                if CONFIG_PATH.exists():
                    base_cfg = json.loads(CONFIG_PATH.read_text())
                else:
                    base_cfg = {"nodes": []}
                base_cfg["service_states"] = self.service_config
                base_cfg.setdefault("security", {})
                base_cfg["security"]["port_isolation_enabled"] = self.port_isolation_enabled
                CONFIG_PATH.write_text(json.dumps(base_cfg, indent=2))
            except Exception:
                pass

            if self.action_handler:
                self.action_handler({"type": "config_saved"})
        except Exception as e:
            pass

    def _load_security_config(self):
        """Load security settings from config."""
        try:
            if self.config_path.exists():
                cfg = json.loads(self.config_path.read_text())
                security = cfg.get("security", {})
                self.port_isolation_enabled = security.get("port_isolation_enabled", True)
            elif CONFIG_PATH.exists():
                cfg = json.loads(CONFIG_PATH.read_text())
                security = cfg.get("security", {})
                self.port_isolation_enabled = security.get("port_isolation_enabled", True)
        except Exception:
            pass

    def _recompute_network_state(self):
        """Derive network state from node statuses for hydra animation."""
        any_online = any(info.get("state") == "online" for info in self.nodes.values())
        now = time.time()
        if any_online:
            self.network_state = "online"
            self.offline_since = 0.0
            return
        if self.offline_since == 0.0:
            self.offline_since = now
        offline_dur = now - self.offline_since
        if offline_dur > 20:
            self.network_state = "hard_offline"
        else:
            self.network_state = "offline"

    # Public API (compatible with UnifiedUI)
    def add_node(self, node_id: str, name: str):
        self.nodes.setdefault(node_id, {
            "name": name,
            "addr": "—",
            "state": "booting",
            "last": "",
            "in": 0,
            "out": 0,
            "err": 0,
            "queue": 0,
            "services": [],
            "started": time.time(),
        })

    def set_action_handler(self, handler: Callable[[dict], None]):
        self.action_handler = handler

    def set_addr(self, node_id: str, addr: Optional[str]):
        if node_id in self.nodes:
            self.nodes[node_id]["addr"] = addr or "—"
            self.nodes[node_id]["state"] = "online" if addr else "waiting"
        self._recompute_network_state()

    def set_state(self, node_id: str, state: str):
        if node_id in self.nodes:
            self.nodes[node_id]["state"] = state
        self._recompute_network_state()

    def set_queue(self, node_id: str, size: int):
        if node_id in self.nodes:
            self.nodes[node_id]["queue"] = max(0, size)

    def set_node_services(self, node_id: str, services: List[str]):
        if node_id in self.nodes:
            self.nodes[node_id]["services"] = services

    def update_service_info(self, name: str, info: dict):
        cur = self.services.get(name, {})
        cur.update(info)
        self.services[name] = cur
        self.service_config.setdefault(name, True)

    def set_daemon_info(self, info: Optional[dict]):
        self.daemon_info = info

    def bump(self, node_id: str, kind: str, msg: str, nkn_addr: str = "", bytes_sent: int = 0,
             service: Optional[str] = None, bytes_in: int = 0, duration_s: float = 0.0):
        target = self.nodes.get(node_id)
        if target:
            target["last"] = msg
            if kind == "IN":
                target["in"] += 1
            elif kind == "OUT":
                target["out"] += 1
            elif kind == "ERR":
                target["err"] += 1

        ts = time.strftime("%H:%M:%S")
        source = target.get("name") if target else node_id
        self.activity.append((ts, source or node_id, kind, msg))

        # Feed activity to hydra (intensity based on kind)
        intensity = {"IN": 0.6, "OUT": 0.4, "ERR": 0.9}.get(kind, 0.3)
        self.hydra.feed_activity(kind, intensity)

        # Ensure address is captured in address book
        if nkn_addr:
            self.stats.touch_address(nkn_addr, service)

        # Track stats for requests
        if service:
            addr = nkn_addr if nkn_addr else self._extract_nkn_addr(msg)
            self.stats.record_request(service, addr, bytes_out=bytes_sent, bytes_in=bytes_in, duration_s=duration_s)
        elif kind in ("IN", "OUT"):
            # Extract service from message if provided (best-effort)
            for svc in self.services.keys():
                if svc in msg or svc in str(node_id):
                    addr = nkn_addr if nkn_addr else self._extract_nkn_addr(msg)
                    self.stats.record_request(svc, addr, bytes_out=bytes_sent, bytes_in=bytes_in, duration_s=duration_s)
                    break

        if self.enabled:
            self.events.put((node_id, kind, msg, ts))
        else:
            print(f"[{ts}] {source:<8} {kind:<3} {msg}")

    def record_flow(
        self,
        source: str,
        target: str,
        payload: str,
        direction: str = "→",
        service: Optional[str] = None,
        channel: Optional[str] = None,
        blocked: bool = False,
    ) -> None:
        """Record a directional flow between a source and target for the Debug view."""
        ts = time.strftime("%H:%M:%S")
        entry = {
            "ts": ts,
            "source": source or "unknown",
            "target": target or "unknown",
            "payload": payload,
            "dir": direction,
            "service": service or "All",
            "channel": channel or "",
            "blocked": bool(blocked),
        }
        self.flow_logs.append(entry)
        if channel:
            self.stats.touch_address(channel, service)
        # Nudge hydra based on flow density
        self.hydra.feed_activity("IN", 0.5 if direction == "→" else 0.4)

    def _extract_nkn_addr(self, msg: str) -> str:
        """Extract NKN address from message string."""
        # Look for NKN address patterns in the message
        import re
        # NKN addresses typically look like: identifier.pubkey_hash
        match = re.search(r'([a-zA-Z0-9_-]+\.[a-f0-9]{40,})', msg)
        if match:
            return match.group(1)
        # Or just hex patterns
        match = re.search(r'([a-f0-9]{40,})', msg)
        if match:
            return match.group(1)
        return "unknown"

    def run(self):
        if not self.enabled:
            try:
                while not self.stop.is_set():
                    time.sleep(0.25)
            except KeyboardInterrupt:
                pass
            return
        curses.wrapper(self._main)

    def shutdown(self):
        self.stop.set()

    def set_chunk_upload_kb(self, kb: int):
        # Placeholder for compatibility
        pass

    # ──────────────────────────────────────────────────────────────
    # Curses UI Implementation
    # ──────────────────────────────────────────────────────────────

    def _main(self, stdscr):
        """Main curses loop with nested menu system."""
        curses.curs_set(0)
        stdscr.nodelay(True)
        stdscr.timeout(120)

        # Initialize colors
        if curses.has_colors():
            curses.start_color()
            curses.use_default_colors()
            curses.init_pair(1, curses.COLOR_CYAN, -1)  # Header
            curses.init_pair(2, curses.COLOR_GREEN, -1)  # Active/OK
            curses.init_pair(3, curses.COLOR_YELLOW, -1)  # Warning
            curses.init_pair(4, curses.COLOR_RED, -1)  # Error
            curses.init_pair(5, curses.COLOR_MAGENTA, -1)  # Section
            curses.init_pair(6, curses.COLOR_CYAN, -1)  # Hydra (idle)
            curses.init_pair(7, curses.COLOR_YELLOW, -1)  # Hydra (active)

        while not self.stop.is_set():
            # Clear events queue
            try:
                while True:
                    _ = self.events.get_nowait()
            except queue.Empty:
                pass

            stdscr.erase()
            h, w = stdscr.getmaxyx()
            hydra_w = max(28, w // 3)
            hydra_w = min(hydra_w, w - 24)  # leave room for content
            hydra_win = stdscr.derwin(h, hydra_w, 0, 0)
            content_win = stdscr.derwin(h, w - hydra_w, 0, hydra_w)
            content_h, content_w = content_win.getmaxyx()

            hydra_win.erase()
            content_win.erase()

            self._render_hydra_panel(hydra_win, h, hydra_w)
            self._render_frame_chrome(stdscr, hydra_w, h, w)

            if self.current_view == "main":
                self._render_main_menu(content_win, content_h, content_w)
            elif self.current_view == "config":
                self._render_config_view(content_win, content_h, content_w)
            elif self.current_view == "stats":
                self._render_stats_view(content_win, content_h, content_w)
            elif self.current_view == "addressbook":
                self._render_address_book_view(content_win, content_h, content_w)
            elif self.current_view == "ingress":
                self._render_ingress_view(content_win, content_h, content_w)
            elif self.current_view == "egress":
                self._render_egress_view(content_win, content_h, content_w)
            elif self.current_view == "debug":
                self._render_debug_view(content_win, content_h, content_w)

            hydra_win.noutrefresh()
            content_win.noutrefresh()
            stdscr.noutrefresh()
            curses.doupdate()

            # Handle input
            try:
                ch = stdscr.getch()
                self._handle_input(ch)
            except Exception:
                pass

    def _safe_addstr(self, stdscr, y, x, text, attr=curses.A_NORMAL):
        """Safely add string to stdscr, handling errors."""
        try:
            h, w = stdscr.getmaxyx()
            if 0 <= y < h and 0 <= x < w:
                max_len = w - x - 1
                if len(text) > max_len:
                    text = text[:max_len]
                stdscr.addstr(y, x, text, attr)
        except curses.error:
            pass

    def _draw_box(self, stdscr, y, x, h, w):
        """Draw a box border."""
        try:
            stdscr.attron(curses.A_DIM)
            stdscr.addstr(y, x, "╔" + "═" * (w - 2) + "╗")
            stdscr.addstr(y + h - 1, x, "╚" + "═" * (w - 2) + "╝")
            for row in range(y + 1, y + h - 1):
                stdscr.addstr(row, x, "║")
                stdscr.addstr(row, x + w - 1, "║")
            stdscr.attroff(curses.A_DIM)
        except curses.error:
            pass

    def _render_frame_chrome(self, stdscr, divider_x: int, h: int, w: int):
        """Brutalist chrome: heavy top/bottom bars and a vertical divider."""
        try:
            bar = "┏" + "━" * (w - 2) + "┓"
            self._safe_addstr(stdscr, 0, 0, bar, curses.A_BOLD)
            bottom = "┗" + "━" * (w - 2) + "┛"
            self._safe_addstr(stdscr, h - 1, 0, bottom, curses.A_DIM)
            for row in range(1, h - 1):
                self._safe_addstr(stdscr, row, divider_x, "┃", curses.A_DIM)
        except Exception:
            pass

    def _render_hydra_panel(self, stdscr, h: int, w: int):
        """Render the animated hydra on the left side."""
        try:
            stdscr.attrset(curses.A_DIM)
            for row in range(h):
                if row % 2 == 0:
                    self._safe_addstr(stdscr, row, 1, "▌" + " " * max(0, w - 4) + "▐")
            stdscr.attrset(curses.A_NORMAL)
        except Exception:
            pass

        base_x = max(4, w // 2)
        base_y = h - 3
        self.hydra.base_x = base_x
        self.hydra.base_y = base_y
        activity_lvl = self.hydra.current_activity()

        net_state = self.network_state
        if net_state == "online":
            base_color = curses.color_pair(6)
        elif net_state == "offline":
            base_color = curses.color_pair(3)  # orange/yellow
        else:
            base_color = curses.color_pair(4)  # red

        # Make offline hydra more frantic
        if net_state == "offline":
            activity_lvl = max(activity_lvl, 0.4)
        elif net_state == "hard_offline":
            activity_lvl = max(activity_lvl, 0.8)

        # Draw stalk with varied thickness
        stalk_color = base_color | (curses.A_BOLD if activity_lvl > 0.6 else curses.A_DIM)
        for seg in range(self.hydra.stalk_segments):
            y = base_y - seg
            ch = "┃" if seg % 2 == 0 else "│"
            self._safe_addstr(stdscr, y, base_x, ch, stalk_color)
            if activity_lvl > 0.5 and seg % 3 == 0:
                self._safe_addstr(stdscr, y, base_x - 1, "╱", base_color)
                self._safe_addstr(stdscr, y, base_x + 1, "╲", base_color)

        # Draw root offshoots reacting to connections
        root_count = min(6, 2 + int(activity_lvl * 6))
        for r in range(root_count):
            ry = base_y - (r * 2 + 1)
            rx = base_x - (2 + (r % 3))
            self._safe_addstr(stdscr, ry, rx, "╱", base_color)
            self._safe_addstr(stdscr, ry + 1, rx + 1, "╱", base_color)
            rx2 = base_x + (2 + (r % 2))
            self._safe_addstr(stdscr, ry, rx2, "╲", base_color)
            self._safe_addstr(stdscr, ry + 1, rx2 - 1, "╲", base_color)

        # Position polyps with sway
        sway = int(max(1, int(activity_lvl * 3))) + (2 if net_state == "hard_offline" else 0)
        for idx, polyp in enumerate(self.hydra.polyps):
            offset = int(math.sin(time.time() * 0.8 + idx) * sway)
            polyp.x = base_x + offset
            polyp.y = base_y - (idx * 4 + 2)
            polyp.update(max(activity_lvl, polyp.activity))
            # Draw head
            head_char = "◉" if polyp.activity > 0.4 else "○"
            head_attr = (curses.color_pair(7) | curses.A_BOLD) if net_state == "online" and polyp.activity > 0.5 else base_color | curses.A_BOLD
            self._safe_addstr(stdscr, polyp.y, polyp.x, head_char, head_attr)
            # Draw tentacles and buds
            for tx, ty in polyp.get_tentacle_positions():
                char = "~" if (tx + ty) % 3 else "⌇"
                self._safe_addstr(stdscr, ty, tx, char, base_color)
            bud_char = "✶" if polyp.activity > 0.6 else "·"
            bud_color = curses.color_pair(4) if net_state == "hard_offline" else curses.color_pair(7)
            self._safe_addstr(stdscr, polyp.y - 1, polyp.x + 1, bud_char, bud_color)

        # Label
        label = "[ hydra ]"
        self._safe_addstr(stdscr, 1, max(1, w - len(label) - 2), label, curses.color_pair(7) | curses.A_BOLD)

    def _render_main_menu(self, stdscr, h, w):
        """Render the main menu."""
        self._draw_box(stdscr, 0, 0, h, w)
        title = "[HYDRA ROUTER]"
        self._safe_addstr(stdscr, 0, 2, title, curses.color_pair(1) | curses.A_BOLD)
        help_text = "↑/↓ choose • Enter select • Q quit"
        self._safe_addstr(stdscr, 1, 2, help_text, curses.A_DIM)

        split = (len(self.MENU_ITEMS) + 1) // 2
        top_items = self.MENU_ITEMS[:split]
        bottom_items = self.MENU_ITEMS[split:]

        row = 3
        for i, item in enumerate(top_items):
            selected = (i == self.main_menu_index)
            bullet = "►" if selected else " "
            attr = curses.color_pair(6) | curses.A_BOLD if selected else curses.A_NORMAL
            pad = "━" if selected else "─"
            line = f"{bullet} [{item.upper():<12}] {pad*max(4, w - 24)}"
            self._safe_addstr(stdscr, row, 2, line[: max(0, w - 4)], attr)
            row += 2

        row = h - (len(bottom_items) * 2 + 2)
        for j, item in enumerate(bottom_items):
            idx = split + j
            selected = (idx == self.main_menu_index)
            bullet = "►" if selected else " "
            attr = curses.color_pair(6) | curses.A_BOLD if selected else curses.A_DIM
            pad = "▏" if selected else ":"
            line = f"{bullet} {item:<14} {pad*4}"
            self._safe_addstr(stdscr, row, w - len(line) - 3, line[: max(0, w - 6)], attr)
            row += 2

        status = f"{len(self.services)} svc / {len(self.nodes)} nodes"
        self._safe_addstr(stdscr, h - 2, 2, status, curses.A_DIM)

    def _render_config_view(self, stdscr, h, w):
        """Render the Config view with service enable/disable."""
        self._draw_box(stdscr, 0, 0, h, w)
        title = "═══ Configuration ═══"
        self._safe_addstr(stdscr, 0, (w - len(title)) // 2, title, curses.color_pair(1) | curses.A_BOLD)

        help_text = "↑/↓: Navigate | Space: Toggle | S: Save | ESC: Back"
        self._safe_addstr(stdscr, 1, 2, help_text, curses.A_DIM)

        start_row = 3
        services = sorted(self.services.keys())

        if not services:
            self._safe_addstr(stdscr, start_row, 2, "(No services configured)", curses.A_DIM)
            return

        self._safe_addstr(stdscr, start_row, 2, "Services:", curses.color_pair(5) | curses.A_BOLD)
        start_row += 2

        visible_rows = h - start_row - 2
        end_idx = min(len(services), self.scroll_offset + visible_rows)

        for i in range(self.scroll_offset, end_idx):
            svc = services[i]
            enabled = self.service_config.get(svc, True)
            status = "[✓]" if enabled else "[ ]"

            row = start_row + (i - self.scroll_offset)
            if i == self.main_menu_index:
                text = f"▶ {status} {svc}"
                attr = curses.color_pair(6)
            else:
                text = f"  {status} {svc}"
                attr = curses.color_pair(2) if enabled else curses.A_DIM

            self._safe_addstr(stdscr, row, 4, text, attr)

            info = self.services.get(svc, {})
            addr = info.get("assigned_addr") or "—"
            if len(addr) > 20:
                addr = addr[:17] + "..."
            state = info.get("status", "unknown")

            # Get endpoint info from SERVICE_TARGETS (module-level constant)
            endpoint = "—"
            for svc_key, target_info in SERVICE_TARGETS.items():
                if svc in target_info.get("aliases", []) or svc == svc_key:
                    endpoint = target_info.get("endpoint", "—")
                    break
            if len(endpoint) > 25:
                endpoint = endpoint[:22] + "..."

            detail = f"{state} | {endpoint}"
            self._safe_addstr(stdscr, row, 40, detail, curses.A_DIM)

        # Add security settings section
        security_row = start_row + (end_idx - self.scroll_offset) + 2
        if security_row < h - 3:
            self._safe_addstr(stdscr, security_row, 2, "Security:", curses.color_pair(5) | curses.A_BOLD)
            security_row += 2

            # Port Isolation toggle (index == len(services))
            is_selected = (self.main_menu_index == len(services))
            isolation_status = "[✓]" if self.port_isolation_enabled else "[ ]"
            isolation_text = f"{'▶ ' if is_selected else '  '}{isolation_status} Port Isolation (restrict to known endpoints)"
            isolation_icon = " 🔒" if self.port_isolation_enabled else " 🔓"
            isolation_attr = curses.color_pair(6) if is_selected else (curses.color_pair(2) if self.port_isolation_enabled else curses.A_DIM)
            self._safe_addstr(stdscr, security_row, 4, isolation_text + isolation_icon, isolation_attr)

    def _render_stats_view(self, stdscr, h, w):
        """Render Statistics view with ASCII bar graphs."""
        self._draw_box(stdscr, 0, 0, h, w)
        title = "═══ Service Statistics (24h) ═══"
        self._safe_addstr(stdscr, 0, (w - len(title)) // 2, title, curses.color_pair(1) | curses.A_BOLD)

        help_text = "ESC: Back"
        self._safe_addstr(stdscr, 1, 2, help_text, curses.A_DIM)

        timeline = self.stats.get_service_timeline(24)

        if not timeline:
            self._safe_addstr(stdscr, 3, 2, "(No activity in last 24 hours)", curses.A_DIM)
            return

        start_row = 3
        row = start_row

        for svc in sorted(timeline.keys()):
            if row >= h - 2:
                break

            history = timeline[svc]
            if not history:
                continue

            buckets = [0] * 24
            now = time.time()
            for ts, count in history:
                hours_ago = int((now - ts) / 3600)
                if 0 <= hours_ago < 24:
                    buckets[23 - hours_ago] += count

            svc_label = (svc[:15] + "...") if len(svc) > 18 else svc
            self._safe_addstr(stdscr, row, 2, f"{svc_label:18}", curses.color_pair(5))

            max_val = max(buckets) if max(buckets) > 0 else 1
            bar_width = min(40, w - 25)

            for i, count in enumerate(buckets[-bar_width:]):
                if count > 0:
                    height = int((count / max_val) * 5) + 1
                    bar_char = "▁▂▃▄▅▆▇█"[min(height - 1, 7)]
                    color = curses.color_pair(2) if count > 0 else curses.A_DIM
                    self._safe_addstr(stdscr, row, 22 + i, bar_char, color)

            total = sum(buckets)
            self._safe_addstr(stdscr, row, w - 12, f"({total:>5})", curses.A_DIM)
            row += 1

        if row < h - 1:
            self._safe_addstr(stdscr, row + 1, 22, "←24h", curses.A_DIM)
            self._safe_addstr(stdscr, row + 1, w - 8, "now→", curses.A_DIM)

    def _fmt_bytes(self, n: int) -> str:
        if n >= 1024 * 1024:
            return f"{n / (1024*1024):.1f} MB"
        if n >= 1024:
            return f"{n / 1024:.1f} KB"
        return f"{n} B"

    def _fmt_minutes(self, seconds: float) -> str:
        return f"{seconds/60:.1f}m"

    def _render_address_detail(self, stdscr, entry: Dict[str, Any], y: int, x: int, h: int, w: int):
        """Render detail panel for a selected address."""
        self._draw_box(stdscr, y, x, h, w)
        inner_y = y + 1
        inner_x = x + 2
        addr = entry.get("addr", "—")
        total = entry.get("total_requests", 0)
        last = entry.get("last_seen", 0)
        first = entry.get("first_seen", last)
        span_s = max(0.0, last - first)
        span_label = self._fmt_minutes(span_s) if span_s else "—"
        bytes_in = entry.get("bytes_in", 0)
        bytes_out = entry.get("bytes_out", 0)
        active_s = entry.get("active_seconds", 0.0)
        active_label = self._fmt_minutes(active_s)

        self._safe_addstr(stdscr, inner_y, inner_x, "addr:", curses.color_pair(5) | curses.A_BOLD)
        self._safe_addstr(stdscr, inner_y, inner_x + 6, addr[: max(8, w - 10)], curses.A_NORMAL)
        inner_y += 1
        self._safe_addstr(stdscr, inner_y, inner_x, f"first: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(first))}", curses.A_DIM)
        inner_y += 1
        self._safe_addstr(stdscr, inner_y, inner_x, f"last : {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(last))}", curses.A_DIM)
        inner_y += 2
        self._safe_addstr(stdscr, inner_y, inner_x, f"requests: {total}   span: {span_label}   active: {active_label}", curses.color_pair(2))
        inner_y += 1
        self._safe_addstr(stdscr, inner_y, inner_x, f"in : {self._fmt_bytes(bytes_in)}   out: {self._fmt_bytes(bytes_out)}", curses.A_NORMAL)
        inner_y += 2

        services_raw = entry.get("services", {})
        services: Dict[str, Dict[str, Any]] = {}
        for svc, info in services_raw.items():
            if isinstance(info, dict):
                services[svc] = info
            else:
                # Legacy int count
                services[svc] = {"count": int(info or 0), "bytes_in": 0, "bytes_out": 0, "active_seconds": 0.0, "first_seen": entry.get("first_seen", first), "last_seen": last}
        if not services:
            self._safe_addstr(stdscr, inner_y, inner_x, "(no service usage)", curses.A_DIM)
            return

        self._safe_addstr(stdscr, inner_y, inner_x, "services:", curses.color_pair(5) | curses.A_BOLD)
        inner_y += 1
        for svc, info in sorted(services.items(), key=lambda kv: kv[1].get("count", 0), reverse=True):
            if inner_y >= y + h - 1:
                break
            cnt = info.get("count", 0)
            bin_val = info.get("bytes_in", 0)
            bout_val = info.get("bytes_out", 0)
            active = info.get("active_seconds", 0.0)
            line = f" - {svc:<14} {cnt:>4}x  in {self._fmt_bytes(bin_val):>8}  out {self._fmt_bytes(bout_val):>8}  act {self._fmt_minutes(active):>6}"
            self._safe_addstr(stdscr, inner_y, inner_x, line[: max(10, w - 4)], curses.A_NORMAL)
            inner_y += 1

    def _render_address_book_view(self, stdscr, h, w):
        """Render Address Book with NKN addresses and usage stats."""
        title = "⌈ Address Book ⌋"
        self._safe_addstr(stdscr, 0, 2, title, curses.color_pair(1) | curses.A_BOLD)
        help_text = "↑/↓ select • detail pane on the right • ESC back"
        self._safe_addstr(stdscr, 1, 2, help_text, curses.A_DIM)

        addresses = self.stats.get_address_book()
        if not addresses:
            self._safe_addstr(stdscr, 3, 2, "(No visitors yet)", curses.A_DIM)
            return

        list_w = max(32, w // 2)
        detail_w = w - list_w - 3
        list_start_row = 3
        visible_rows = h - list_start_row - 2

        # Clamp selection
        self.main_menu_index = min(self.main_menu_index, max(0, len(addresses) - 1))
        if self.main_menu_index < self.scroll_offset:
            self.scroll_offset = self.main_menu_index
        if self.main_menu_index >= self.scroll_offset + visible_rows:
            self.scroll_offset = max(0, self.main_menu_index - visible_rows + 1)
        end_idx = min(len(addresses), self.scroll_offset + visible_rows)

        # List header
        header = f"{'addr':<28} {'reqs':>6} {'last':>16}"
        self._safe_addstr(stdscr, list_start_row, 1, header, curses.color_pair(5) | curses.A_BOLD)
        self._safe_addstr(stdscr, list_start_row + 1, 1, "─" * (list_w - 2), curses.A_DIM)

        for i in range(self.scroll_offset, end_idx):
            addr_info = addresses[i]
            addr = addr_info.get("addr", "—")
            total = addr_info.get("total_requests", 0)
            last_seen = addr_info.get("last_seen", 0)
            last_str = time.strftime("%m-%d %H:%M", time.localtime(last_seen))

            addr_display = (addr[:24] + "...") if len(addr) > 27 else addr

            row = list_start_row + 2 + (i - self.scroll_offset)
            if row >= h - 1:
                break

            if i == self.main_menu_index:
                attr = curses.color_pair(6) | curses.A_BOLD
                prefix = "►"
            else:
                attr = curses.A_NORMAL
                prefix = " "

            line = f"{prefix} {addr_display:<27} {total:>6} {last_str:>16}"
            self._safe_addstr(stdscr, row, 1, line[: list_w - 2], attr)

        # Detail panel
        selected = addresses[self.main_menu_index] if addresses else {}
        detail_h = h - 4
        detail_x = list_w + 2
        self._render_address_detail(stdscr, selected, 2, detail_x, detail_h, detail_w)

    def _render_ingress_view(self, stdscr, h, w):
        """Render Ingress view with QR codes for service addresses."""
        self._draw_box(stdscr, 0, 0, h, w)
        title = "═══ Ingress Addresses ═══"
        self._safe_addstr(stdscr, 0, (w - len(title)) // 2, title, curses.color_pair(1) | curses.A_BOLD)

        help_text = "↑/↓: Navigate | Enter: Show QR | ESC: Back"
        self._safe_addstr(stdscr, 1, 2, help_text, curses.A_DIM)

        if self.show_qr and self.qr_data:
            self._render_qr_code(stdscr, 3, 2, h - 4, w - 4)
            return

        start_row = 3
        services = sorted(self.services.keys())

        if not services:
            self._safe_addstr(stdscr, start_row, 2, "(No services available)", curses.A_DIM)
            return

        visible_rows = h - start_row - 2
        end_idx = min(len(services), self.scroll_offset + visible_rows)

        for i in range(self.scroll_offset, end_idx):
            svc = services[i]
            info = self.services.get(svc, {})
            addr = info.get("assigned_addr", "—")
            state = info.get("status", "unknown")

            row = start_row + (i - self.scroll_offset)
            if i == self.main_menu_index:
                attr = curses.color_pair(6)
                prefix = "▶ "
            else:
                attr = curses.color_pair(2) if state == "ready" else curses.A_DIM
                prefix = "  "

            svc_display = (svc[:20] + "...") if len(svc) > 23 else svc
            # Don't truncate address - show full string
            addr_display = addr

            line = f"{prefix}{svc_display:<23} [{state:<8}] {addr_display}"
            self._safe_addstr(stdscr, row, 2, line, attr)

    def _render_egress_view(self, stdscr, h, w):
        """Render Egress view with bandwidth and user leaderboard."""
        self._draw_box(stdscr, 0, 0, h, w)
        title = "═══ Egress Statistics ═══"
        self._safe_addstr(stdscr, 0, (w - len(title)) // 2, title, curses.color_pair(1) | curses.A_BOLD)

        help_text = "ESC: Back"
        self._safe_addstr(stdscr, 1, 2, help_text, curses.A_DIM)

        egress = self.stats.get_egress_stats()

        if not egress:
            self._safe_addstr(stdscr, 3, 2, "(No egress data)", curses.A_DIM)
            return

        start_row = 3
        self._safe_addstr(stdscr, start_row, 2, "Service Summary:", curses.color_pair(5) | curses.A_BOLD)
        start_row += 2

        header = f"{'Service':<20} {'Requests':>10} {'Bandwidth':>15} {'Users':>8}"
        self._safe_addstr(stdscr, start_row, 2, header, curses.A_BOLD)
        start_row += 1

        for svc in sorted(egress.keys()):
            if start_row >= h // 2:
                break

            stats = egress[svc]
            req_count = stats.get("request_count", 0)
            bytes_sent = stats.get("bytes_sent", 0)
            users = len(stats.get("users", {}))

            if bytes_sent > 1024 * 1024 * 1024:
                bw = f"{bytes_sent / (1024**3):.2f} GB"
            elif bytes_sent > 1024 * 1024:
                bw = f"{bytes_sent / (1024**2):.2f} MB"
            elif bytes_sent > 1024:
                bw = f"{bytes_sent / 1024:.2f} KB"
            else:
                bw = f"{bytes_sent} B"

            svc_display = (svc[:17] + "...") if len(svc) > 20 else svc
            line = f"  {svc_display:<20} {req_count:>10} {bw:>15} {users:>8}"
            self._safe_addstr(stdscr, start_row, 2, line, curses.color_pair(2))
            start_row += 1

        start_row += 2
        if start_row < h - 5:
            self._safe_addstr(stdscr, start_row, 2, "Top Users (by bandwidth):", curses.color_pair(5) | curses.A_BOLD)
            start_row += 2

            user_totals = {}
            for stats in egress.values():
                for user, bytes_sent in stats.get("users", {}).items():
                    user_totals[user] = user_totals.get(user, 0) + bytes_sent

            top_users = sorted(user_totals.items(), key=lambda x: x[1], reverse=True)[:10]

            for i, (user, bytes_sent) in enumerate(top_users):
                if start_row >= h - 2:
                    break

                if bytes_sent > 1024 * 1024 * 1024:
                    bw = f"{bytes_sent / (1024**3):.2f} GB"
                elif bytes_sent > 1024 * 1024:
                    bw = f"{bytes_sent / (1024**2):.2f} MB"
                else:
                    bw = f"{bytes_sent / 1024:.2f} KB"

                user_display = (user[:40] + "...") if len(user) > 43 else user
                line = f"  {i+1:2}. {user_display:<43} {bw:>15}"
                self._safe_addstr(stdscr, start_row, 2, line, curses.A_NORMAL)
                start_row += 1

    def _debug_tabs(self) -> List[str]:
        """Tabs for the Debug view: All + known services (from config or seen flows)."""
        svc_names = set(self.services.keys())
        for entry in self.flow_logs:
            svc = entry.get("service")
            if svc and svc != "All":
                svc_names.add(svc)
        tabs = ["All"] + sorted(svc_names)
        if not tabs:
            tabs = ["All"]
        if self.debug_tab_index >= len(tabs):
            self.debug_tab_index = max(0, len(tabs) - 1)
        return tabs

    def _debug_scroll_for_tab(self, tab: str) -> int:
        return self.debug_scroll_offsets.get(tab, 0)

    def _set_debug_scroll(self, tab: str, value: int) -> None:
        self.debug_scroll_offsets[tab] = max(0, value)
    
    def _short_label(self, label: str, max_label: int) -> str:
        """
        Compact long labels for the debug view.

        - For NKN-style addresses like 'hydra.<40+ hex>', show 'hydra.xx…yy'
          (first 2 and last 2 hex chars).
        - For everything else, fall back to simple width-based truncation.
        """
        if len(label) <= max_label:
            return label

        try:
            import re
            m = re.match(r"^([a-zA-Z0-9_-]+)\.([0-9a-f]{8,})$", label)
            if m:
                prefix, hexpart = m.groups()
                return f"{prefix}.{hexpart[:2]}…{hexpart[-2:]}"
        except Exception:
            pass

        # Generic fallback: keep as much as fits
        return label[: max_label - 1] + "…"

    def _render_debug_view(self, stdscr, h, w):
        """Render Debug/Activity Log view with directional flows and tabs."""
        self._draw_box(stdscr, 0, 0, h, w)
        title = "═══ Debug / Activity Log ═══"
        self._safe_addstr(stdscr, 0, (w - len(title)) // 2, title, curses.color_pair(1) | curses.A_BOLD)

        tabs = self._debug_tabs()
        tab_line = "  ".join(
            f"[{t}]" if i == self.debug_tab_index else t
            for i, t in enumerate(tabs)
        )
        self._safe_addstr(stdscr, 1, 2, tab_line[: max(0, w - 4)], curses.color_pair(6) | curses.A_BOLD)

        help_text = "←/→ tabs | ↑/↓ scroll | ESC: Back"
        self._safe_addstr(stdscr, 2, 2, help_text, curses.A_DIM)

        tab = tabs[self.debug_tab_index]

        start_row = 4
        visible_rows = h - start_row - 1

        flows = [
            entry for entry in reversed(self.flow_logs)
            if tab == "All" or entry.get("service") == tab
        ]

        if not flows:
            # Fall back to coarse activity if no flow entries yet
            if self.activity:
                self._safe_addstr(stdscr, start_row, 2, "(No flow entries yet; showing recent activity)", curses.A_DIM)
                start_row += 2
                visible_rows = h - start_row - 1
                activity_list = list(reversed(self.activity))
                total_entries = len(activity_list)
                max_scroll = max(0, total_entries - visible_rows)
                alt_scroll = min(self._debug_scroll_for_tab(tab), max_scroll)
                end_idx = min(total_entries, alt_scroll + visible_rows)
                for i in range(alt_scroll, end_idx):
                    ts, source, kind, message = activity_list[i]
                    row = start_row + (i - alt_scroll)
                    attr = curses.color_pair(2) if kind in ("IN", "OUT") else (curses.color_pair(4) if kind == "ERR" else curses.A_NORMAL)
                    line = f"[{ts}] {source:<12} {kind:<3} {message}"
                    self._safe_addstr(stdscr, row, 2, line[: max(0, w - 4)], attr)
                if total_entries > visible_rows:
                    scroll_info = f"Showing {alt_scroll + 1}-{end_idx} of {total_entries}"
                    self._safe_addstr(stdscr, h - 1, w - len(scroll_info) - 2, scroll_info, curses.A_DIM)
            else:
                self._safe_addstr(stdscr, start_row, 2, "(No recent flows)", curses.A_DIM)
            return

        total_entries = len(flows)
        max_scroll = max(0, total_entries - visible_rows)
        scroll = min(self._debug_scroll_for_tab(tab), max_scroll)
        self._set_debug_scroll(tab, scroll)

        end_idx = min(total_entries, scroll + visible_rows)
        for i in range(scroll, end_idx):
            entry = flows[i]
            row = start_row + (i - scroll)
            ts = entry.get("ts", "--:--:--")
            src = entry.get("source", "unknown")
            tgt = entry.get("target", "unknown")
            payload = entry.get("payload", "")
            svc = entry.get("service", "")
            channel = entry.get("channel", "")
            arrow = entry.get("dir", "→")

            # Compact source/target to fit on screen
            max_label = max(10, (w // 4))
            src_short = self._short_label(src, max_label)
            tgt_short = self._short_label(tgt, max_label)

            payload_space = max(10, w - len(ts) - len(src_short) - len(tgt_short) - 12)
            payload_short = payload if len(payload) <= payload_space else payload[: payload_space - 1] + "…"

            dir_left = arrow if len(arrow) == 1 else "→"
            dir_right = dir_left
            line = f"[{ts}] {src_short} {dir_left} {payload_short} {dir_right} {tgt_short}"
            if svc and svc != "All":
                line += f" [{svc}]"
            if channel:
                line += f" @{channel}"
            attr = curses.color_pair(2)
            if entry.get("blocked"):
                attr = curses.color_pair(3) | curses.A_BOLD  # orange/yellow for blocked
            elif "err" in payload.lower():
                attr = curses.color_pair(4)
            self._safe_addstr(stdscr, row, 2, line[: max(0, w - 4)], attr)

        if total_entries > visible_rows:
            scroll_info = f"Showing {scroll + 1}-{end_idx} of {total_entries}"
            self._safe_addstr(stdscr, h - 1, w - len(scroll_info) - 2, scroll_info, curses.A_DIM)

    def _render_qr_code(self, stdscr, y, x, max_h, max_w):
        """Render QR code for selected service."""
        if not self.qr_data:
            return

        qr_text = render_qr_ascii(self.qr_data, scale=1, invert=False)
        lines = qr_text.splitlines()

        qr_h = len(lines)
        qr_w = max(len(line) for line in lines) if lines else 0

        start_y = y + (max_h - qr_h) // 2
        start_x = x + (max_w - qr_w) // 2

        if self.qr_label:
            label_y = max(0, start_y - 2)
            self._safe_addstr(stdscr, label_y, start_x, self.qr_label, curses.color_pair(1) | curses.A_BOLD)

        for i, line in enumerate(lines):
            row = start_y + i
            if row >= y + max_h:
                break
            self._safe_addstr(stdscr, row, start_x, line, curses.A_NORMAL)

        inst_y = start_y + qr_h + 2
        if inst_y < y + max_h:
            self._safe_addstr(stdscr, inst_y, start_x, "Press ESC to close", curses.A_DIM)

    def _handle_input(self, ch):
        """Handle keyboard input for navigation."""
        if ch == ord('q') or ch == ord('Q'):
            if self.current_view == "main":
                self.stop.set()
            else:
                self.current_view = "main"
                self.main_menu_index = 0
                self.scroll_offset = 0
                self.show_qr = False

        elif ch == 27:  # ESC
            if self.show_qr:
                self.show_qr = False
            elif self.current_view != "main":
                self.current_view = "main"
                self.main_menu_index = 0
                self.scroll_offset = 0
            else:
                self.stop.set()

        elif ch in (curses.KEY_UP, ord('k')):
            if self.current_view == "main":
                self.main_menu_index = (self.main_menu_index - 1) % len(self.MENU_ITEMS)
            elif self.current_view == "debug":
                # Scroll debug view
                tab = self._debug_tabs()[self.debug_tab_index]
                cur = self._debug_scroll_for_tab(tab)
                self._set_debug_scroll(tab, cur - 1)
            else:
                self.main_menu_index = max(0, self.main_menu_index - 1)
                if self.main_menu_index < self.scroll_offset:
                    self.scroll_offset = self.main_menu_index

        elif ch in (curses.KEY_DOWN, ord('j')):
            if self.current_view == "main":
                self.main_menu_index = (self.main_menu_index + 1) % len(self.MENU_ITEMS)
            elif self.current_view == "debug":
                # Scroll debug view
                tab = self._debug_tabs()[self.debug_tab_index]
                cur = self._debug_scroll_for_tab(tab)
                self._set_debug_scroll(tab, cur + 1)
            else:
                max_idx = 0
                if self.current_view == "config":
                    # Services + 1 for security section (port isolation)
                    max_idx = max(0, len(self.services))
                elif self.current_view == "addressbook":
                    max_idx = max(0, len(self.stats.get_address_book()) - 1)
                elif self.current_view == "ingress":
                    max_idx = max(0, len(self.services) - 1)

                self.main_menu_index = min(max_idx, self.main_menu_index + 1)

        elif ch in (curses.KEY_ENTER, 10, 13):
            self._handle_enter()

        elif ch == ord(' '):
            if self.current_view == "config":
                services = sorted(self.services.keys())
                if 0 <= self.main_menu_index < len(services):
                    # Toggle service enabled/disabled
                    svc = services[self.main_menu_index]
                    self.service_config[svc] = not self.service_config.get(svc, True)
                    if self.action_handler:
                        self.action_handler({
                            "type": "service_toggle",
                            "service": svc,
                            "enabled": self.service_config[svc],
                        })
                elif self.main_menu_index == len(services):
                    # Toggle port isolation (security section)
                    self.port_isolation_enabled = not self.port_isolation_enabled
                    if self.action_handler:
                        self.action_handler({
                            "type": "port_isolation",
                            "enabled": self.port_isolation_enabled,
                        })

        elif ch in (ord('s'), ord('S')):
            if self.current_view == "config":
                self._save_service_config()
        elif ch in (curses.KEY_RIGHT, ord('l')):
            if self.current_view == "debug":
                tabs = self._debug_tabs()
                if tabs:
                    self.debug_tab_index = (self.debug_tab_index + 1) % len(tabs)
        elif ch in (curses.KEY_LEFT, ord('h')):
            if self.current_view == "debug":
                tabs = self._debug_tabs()
                if tabs:
                    self.debug_tab_index = (self.debug_tab_index - 1) % len(tabs)

    def _handle_enter(self):
        """Handle Enter key based on current view."""
        if self.current_view == "main":
            selected = self.MENU_ITEMS[self.main_menu_index]
            if selected == "Config":
                self.current_view = "config"
            elif selected == "Statistics":
                self.current_view = "stats"
            elif selected == "Address Book":
                self.current_view = "addressbook"
            elif selected == "Ingress":
                self.current_view = "ingress"
            elif selected == "Egress":
                self.current_view = "egress"
            elif selected == "Debug":
                self.current_view = "debug"

            self.main_menu_index = 0
            self.scroll_offset = 0
            # Reset debug scroll when entering debug view
            if self.current_view == "debug":
                tabs = self._debug_tabs()
                if tabs:
                    self.debug_tab_index = 0
                    self._set_debug_scroll(tabs[0], 0)

        elif self.current_view == "ingress":
            services = sorted(self.services.keys())
            if 0 <= self.main_menu_index < len(services):
                svc = services[self.main_menu_index]
                info = self.services.get(svc, {})
                addr = info.get("assigned_addr", "")
                if addr and addr != "—":
                    self.qr_data = addr
                    self.qr_label = f"Service: {svc}"
                    self.show_qr = True


# ──────────────────────────────────────────────────────────────
# Unified curses UI (Legacy - kept for backwards compatibility)
# ──────────────────────────────────────────────────────────────
class UnifiedUI:
    def __init__(self, enabled: bool):
        self.enabled = enabled and curses is not None and sys.stdout.isatty()
        self.events: "queue.Queue[tuple[str, str, str, str]]" = queue.Queue()
        self.nodes: Dict[str, dict] = {}
        self.services: Dict[str, dict] = {}
        self.daemon_info: Optional[dict] = None
        self.stop = threading.Event()
        self.action_handler: Optional[Callable[[dict], None]] = None
        self.selected_index = 0
        self._interactive_rows: List[dict] = []
        self.service_index: int = 0
        self.service_names: List[str] = []
        self.show_activity: bool = False
        self.qr_candidates: List[dict] = []
        self.qr_cycle_index: int = 0
        self.qr_cycle_label: str = ""
        self.qr_cycle_lines: List[str] = []
        self.qr_next_ts: float = 0.0
        self.qr_locked: bool = False
        self.qr_row_ref: Optional[dict] = None
        self._last_dims: Tuple[int, int] = (0, 0)
        self.activity: Deque[Tuple[str, str, str, str]] = deque(maxlen=500)
        self.chunk_upload_kb: int = 600

    def add_node(self, node_id: str, name: str):
        self.nodes.setdefault(node_id, {
            "name": name,
            "addr": "—",
            "state": "booting",
            "last": "",
            "in": 0,
            "out": 0,
            "err": 0,
            "queue": 0,
            "services": [],
            "started": time.time(),
        })

    def set_action_handler(self, handler: Callable[[dict], None]):
        self.action_handler = handler

    def set_chunk_upload_kb(self, kb: int):
        try:
            self.chunk_upload_kb = max(4, int(kb))
        except Exception:
            pass

    def set_addr(self, node_id: str, addr: Optional[str]):
        if node_id in self.nodes:
            self.nodes[node_id]["addr"] = addr or "—"
            self.nodes[node_id]["state"] = "online" if addr else "waiting"

    def set_state(self, node_id: str, state: str):
        if node_id in self.nodes:
            self.nodes[node_id]["state"] = state

    def set_queue(self, node_id: str, size: int):
        if node_id in self.nodes:
            self.nodes[node_id]["queue"] = max(0, size)

    def set_node_services(self, node_id: str, services: List[str]):
        if node_id in self.nodes:
            self.nodes[node_id]["services"] = services

    def update_service_info(self, name: str, info: dict):
        cur = self.services.get(name, {})
        cur.update(info)
        self.services[name] = cur
        self.service_names = sorted(self.services.keys())
        if self.service_names:
            self.service_index %= len(self.service_names)
        else:
            self.service_index = 0

    def set_daemon_info(self, info: Optional[dict]):
        self.daemon_info = info

    def bump(self, node_id: str, kind: str, msg: str):
        target = self.nodes.get(node_id)
        if target:
            target["last"] = msg
            if kind == "IN":
                target["in"] += 1
            elif kind == "OUT":
                target["out"] += 1
            elif kind == "ERR":
                target["err"] += 1
        ts = time.strftime("%H:%M:%S")
        source = target.get("name") if target else node_id
        self.activity.append((ts, source or node_id, kind, msg))
        if self.enabled:
            self.events.put((node_id, kind, msg, ts))
        else:
            print(f"[{ts}] {source:<8} {kind:<3} {msg}")

    def run(self):
        if not self.enabled:
            try:
                while not self.stop.is_set():
                    time.sleep(0.25)
            except KeyboardInterrupt:
                pass
            return
        curses.wrapper(self._main)

    def shutdown(self):
        self.stop.set()

    # ──────────────────────────────────────────────
    # curses helpers
    # ──────────────────────────────────────────────
    def _main(self, stdscr):
        curses.curs_set(0)
        stdscr.nodelay(True)
        stdscr.timeout(120)
        color_enabled = False
        header_attr = curses.A_BOLD
        node_attr = curses.A_NORMAL
        section_attr = curses.A_DIM
        if curses.has_colors():
            curses.start_color()
            curses.use_default_colors()
            curses.init_pair(1, curses.COLOR_CYAN, -1)
            curses.init_pair(2, curses.COLOR_GREEN, -1)
            curses.init_pair(3, curses.COLOR_MAGENTA, -1)
            header_attr = curses.color_pair(1) | curses.A_BOLD
            node_attr = curses.color_pair(2)
            section_attr = curses.color_pair(3) | curses.A_BOLD
            color_enabled = True
        while not self.stop.is_set():
            try:
                while True:
                    _ = self.events.get_nowait()
            except queue.Empty:
                pass

            stdscr.erase()
            stdscr.addnstr(0, 0, "Unified NKN Router — arrows: cycle services, e: pin QR, s: activity, c: config, q: quit", max(0, curses.COLS - 1), header_attr)
            if self.daemon_info:
                daemon_line = f"Daemon: enabled at {self.daemon_info.get('path','?')}"
            else:
                daemon_line = "Daemon: disabled"
            stdscr.addnstr(1, 0, daemon_line[: curses.COLS - 1], curses.A_DIM)

            rows = self._build_rows()
            self._interactive_rows = [row for row in rows if row.get("selectable")]
            selected_row = self._interactive_rows[0] if self._interactive_rows else None

            now = time.time()
            qr_candidates = [] if self.show_activity else [row for row in rows if row.get("type") in ("node", "service")]
            if not self.show_activity:
                if qr_candidates:
                    if qr_candidates != self.qr_candidates:
                        self.qr_candidates = qr_candidates
                        self.qr_cycle_index = self.qr_cycle_index % len(self.qr_candidates)
                    if not self.qr_locked and (now >= self.qr_next_ts or not self.qr_cycle_lines):
                        self._advance_qr_cycle()
                else:
                    self.qr_candidates = []
                    if not self.qr_locked:
                        self.qr_cycle_lines = []

                dims = (curses.LINES, curses.COLS)
                if self.qr_row_ref and dims != self._last_dims:
                    label, lines = self._qr_text_for_row(self.qr_row_ref, include_detail=False)
                    self._set_cycle_display(label, lines, lock=self.qr_locked, remember_row=self.qr_row_ref)
                self._last_dims = dims

            screen_row = 3
            width = max(0, curses.COLS - 1)
            if self.show_activity:
                # Full-screen activity log, no QR
                for row in rows:
                    if row.get("type") == "activity_header":
                        try:
                            stdscr.addnstr(screen_row, 0, row.get("text", "")[:width], node_attr | curses.A_BOLD)
                        except curses.error:
                            pass
                        screen_row += 1
                        continue
                    if row.get("type") != "activity":
                        continue
                    if screen_row >= curses.LINES - 1:
                        break
                    try:
                        stdscr.addnstr(screen_row, 0, row.get("text", "")[:width], node_attr)
                    except curses.error:
                        pass
                    screen_row += 1
            else:
                for row in rows:
                    if screen_row >= curses.LINES - 1:
                        break
                    rtype = row.get("type")
                    if rtype == "separator":
                        screen_row += 1
                        continue
                    attr = node_attr if rtype in ("node", "service") else curses.A_NORMAL
                    if rtype == "header":
                        attr = header_attr
                    if rtype == "section":
                        attr = section_attr
                    if rtype in ("activity", "activity_header"):
                        attr = node_attr
                        if rtype == "activity_header":
                            attr |= curses.A_BOLD
                    prefix = ""
                    if row.get("selectable"):
                        prefix = "• " if (selected_row and row is selected_row) else "  "
                    text = prefix + row.get("text", "")
                    if selected_row and row is selected_row and row.get("selectable"):
                        attr |= curses.A_REVERSE
                    try:
                        stdscr.addnstr(screen_row, 0, text[:width], attr)
                    except curses.error:
                        pass
                    screen_row += 1

            if self.qr_cycle_lines and not self.show_activity:
                mode = "locked" if self.qr_locked else "auto"
                label_line = f"QR ({mode} every 10s): {self.qr_cycle_label}" if self.qr_cycle_label else f"QR ({mode})"
                if screen_row < curses.LINES - 1:
                    try:
                        stdscr.addnstr(screen_row, 0, label_line[:width], curses.A_DIM | curses.A_BOLD)
                    except curses.error:
                        pass
                    screen_row += 1
                for ln in self.qr_cycle_lines:
                    if screen_row >= curses.LINES - 1:
                        break
                    try:
                        stdscr.addnstr(screen_row, 0, ln[:width], curses.A_DIM)
                    except curses.error:
                        pass
                    screen_row += 1

            stdscr.refresh()

            try:
                ch = stdscr.getch()
                if ch in (ord('q'), ord('Q')):
                    self.stop.set()
                elif ch in (curses.KEY_UP, ord('k'), curses.KEY_LEFT):
                    self._cycle_service(-1)
                elif ch in (curses.KEY_DOWN, ord('j'), curses.KEY_RIGHT):
                    self._cycle_service(1)
                elif ch in (ord('e'), ord('E')):
                    if selected_row:
                        label, lines = self._qr_text_for_row(selected_row, include_detail=False)
                        self._set_cycle_display(label, lines, lock=True, remember_row=selected_row)
                elif ch in (curses.KEY_ENTER, 10, 13):
                    if selected_row:
                        self._handle_enter(stdscr, selected_row)
                elif ch in (ord('s'), ord('S')):
                    self.show_activity = not self.show_activity
                elif ch in (ord('c'), ord('C')):
                    self._handle_config_prompt(stdscr)
            except Exception:
                pass

    def _cycle_service(self, delta: int) -> None:
        if not self.service_names:
            return
        self.service_index = (self.service_index + delta) % len(self.service_names)
        self.qr_locked = False
        self._advance_qr_cycle(force_row=True)

    def _handle_config_prompt(self, stdscr) -> None:
        kb = self._prompt_number(stdscr, "Chunk upload size (KB)", self.chunk_upload_kb or 600)
        if kb is not None and self.action_handler:
            self.action_handler({"type": "config", "key": "chunk_upload_kb", "value": kb})

    def _build_rows(self) -> List[dict]:
        rows: List[dict] = []
        if self.show_activity:
            rows.append({"type": "activity_header", "text": "Service Activity (s to toggle, ↑/↓ to scroll)", "selectable": False})
            if self.activity:
                # Show most recent first; allow the list to fill the screen
                for ts, source, kind, message in reversed(self.activity):
                    line = f"[{ts}] {source} {kind}: {message}"
                    rows.append({"type": "activity", "text": line, "selectable": False})
            else:
                rows.append({"type": "activity", "text": "(no recent activity)", "selectable": False})
            return rows

        rows.append({"type": "section", "text": "Services", "selectable": False})
        if self.service_names:
            name = self.service_names[self.service_index % len(self.service_names)]
            info = self.services.get(name, {})
            addr = info.get("assigned_addr") or "—"
            rows.append({"type": "service", "id": name, "text": f"{name} {addr}", "selectable": True})
        else:
            rows.append({"type": "service", "id": "none", "text": "(no services yet)", "selectable": False})
        return rows

    @staticmethod
    def _extract_identifier_segment(value: str) -> str:
        if not value:
            return ""
        segment = value.strip()
        if "relay-" in segment:
            segment = segment.split("relay-")[-1]
        for sep in (":", "@"):
            if sep in segment:
                segment = segment.split(sep)[-1]
        return segment

    def _service_identifier(self, addr: str, assigned: str) -> str:
        for candidate in (addr, assigned):
            segment = self._extract_identifier_segment(candidate)
            if segment:
                return segment
        return "—"

    def _handle_enter(self, stdscr, row: dict) -> None:
        row_type = row.get("type")
        if row_type == "node":
            self._show_qr_for_row(stdscr, row, include_detail=True)
            return
        elif row_type == "service":
            self._show_qr_for_row(stdscr, row, include_detail=True)
            return
        elif row_type == "daemon":
            enabled = bool(self.daemon_info)
            if enabled:
                options = ["Disable daemon", "Show daemon info", "Cancel"]
            else:
                options = ["Enable daemon", "Cancel"]
            choice = self._prompt_menu(stdscr, "Daemon Controls", options)
            if self.action_handler:
                if not enabled and choice == 0:
                    self.action_handler({"type": "daemon", "op": "enable"})
                elif enabled and choice == 0:
                    self.action_handler({"type": "daemon", "op": "disable"})
                elif enabled and choice == 1:
                    info = json.dumps(self.daemon_info or {}, indent=2)
                    self._show_message(stdscr, info)

    def _show_log_tail(self, stdscr, path: Optional[str]) -> None:
        if not path:
            self._show_message(stdscr, "No log file path available")
            return
        try:
            log_path = Path(path)
            if not log_path.exists():
                raise FileNotFoundError(path)
            lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()[-20:]
            text = "\n".join(lines) if lines else "(log empty)"
            self._show_message(stdscr, text)
        except Exception as exc:
            self._show_message(stdscr, f"Log read failed: {exc}")

    def _show_node_logs(self, stdscr, node_name: str) -> None:
        logs = []
        for svc, info in self.services.items():
            if info.get("assigned_node") == node_name and info.get("log"):
                logs.append((svc, info.get("log")))
        if not logs:
            self._show_message(stdscr, "No logs associated with this node yet.")
            return
        output_lines: List[str] = []
        for svc, path in logs:
            try:
                log_path = Path(path)
                if not log_path.exists():
                    raise FileNotFoundError(path)
                lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()[-10:]
                output_lines.append(f"[{svc}] {path}")
                output_lines.extend(lines if lines else ["(log empty)"])
            except Exception as exc:
                output_lines.append(f"[{svc}] log read failed: {exc}")
            output_lines.append("")
        self._show_message(stdscr, "\n".join(output_lines))

    def _qr_text_for_row(self, row: dict, include_detail: bool = False) -> Tuple[str, List[str]]:
        rtype = row.get("type")
        label = ""
        detail_lines: List[str] = []
        addr = ""
        max_width = max(10, curses.COLS - 2) if curses else 80
        if rtype == "node":
            node = self.nodes.get(row.get("id"))
            if not node:
                return ("", ["(Node not found)"])
            addr = (node.get("addr") or "").strip()
            label = f"Node {node.get('name','')} ({addr or '—'})"
            if include_detail:
                services = ", ".join(node.get("services", [])) or "—"
                detail = (
                    f"Node: {node.get('name','')}\n"
                    f"Address: {node.get('addr','')}\n"
                    f"State: {node.get('state','')}\n"
                    f"Queue: {node.get('queue',0)}\n"
                    f"Counts: IN={node.get('in',0)} OUT={node.get('out',0)} ERR={node.get('err',0)}\n"
                    f"Services: {services}"
                )
                detail_lines = detail.splitlines()
        elif rtype == "service":
            service_id = row.get("id")
            info = self.services.get(service_id, {})
            addr = (info.get("assigned_addr") or "").strip()
            label = f"Service {service_id} ({addr or '—'})"
            if include_detail:
                detail = (
                    f"Service: {service_id}\n"
                    f"Assigned node: {info.get('assigned_node','—')}\n"
                    f"Address: {addr or '—'}\n"
                    f"Status: {info.get('status','?')}"
                )
                detail_lines = detail.splitlines()
        else:
            return ("", ["(Unsupported item)"])

        lines: List[str] = []
        if include_detail and detail_lines:
            lines.extend(detail_lines)
            lines.append("")

        if addr:
            ascii_lines = render_qr_ascii(addr).splitlines()
            ascii_lines = [ln[:max_width] for ln in ascii_lines]
            lines.extend(ascii_lines)
        else:
            lines.append("(No NKN address yet)")
        return (label, lines)

    def _set_cycle_display(self, label: str, lines: List[str], delay: float = 10.0, lock: bool = False, remember_row: Optional[dict] = None) -> None:
        if not lines:
            lines = ["(No data)"]
        self.qr_cycle_label = label
        self.qr_cycle_lines = lines
        self.qr_next_ts = time.time() + delay
        if remember_row is not None:
            self.qr_row_ref = remember_row
        if lock:
            self.qr_locked = True

    def _format_service_line(self, name: str, addr: str) -> str:
        base = self._clean_identifier(name)
        addr_hex = addr
        if isinstance(addr, str) and "." in addr:
            addr_hex = addr.split(".")[-1]
        return f"{base}.{addr_hex}" if addr_hex else base

    @staticmethod
    def _clean_identifier(name: str) -> str:
        try:
            import re
            m = re.match(r"^(.*?-relay)(?:-[^.]+)?$", name)
            if m:
                return m.group(1)
        except Exception:
            pass
        return name

    def _advance_qr_cycle(self, force_row: bool = False) -> None:
        if not self.qr_candidates:
            self.qr_cycle_lines = []
            return
        if force_row and self.qr_row_ref:
            row = self.qr_row_ref
        else:
            row = self.qr_candidates[self.qr_cycle_index % len(self.qr_candidates)]
            self.qr_cycle_index = (self.qr_cycle_index + 1) % max(1, len(self.qr_candidates))
        label, lines = self._qr_text_for_row(row, include_detail=False)
        self._set_cycle_display(label, lines, remember_row=row)

    def _show_qr_for_row(self, stdscr, row: dict, include_detail: bool = False) -> None:
        if not row:
            return
        label_detail, lines_detail = self._qr_text_for_row(row, include_detail=True)
        label_inline, lines_inline = self._qr_text_for_row(row, include_detail=False)
        if include_detail and lines_detail:
            self._show_message(stdscr, "\n".join(lines_detail))
        self._set_cycle_display(label_inline, lines_inline)
    def _prompt_menu(self, stdscr, title: str, options: List[str]) -> Optional[int]:
        if not options:
            return None
        height = len(options) + 4
        width = max(len(title), *(len(opt) for opt in options)) + 6
        y = max(2, (curses.LINES - height) // 2)
        x = max(2, (curses.COLS - width) // 2)
        win = curses.newwin(height, width, y, x)
        win.box()
        win.addnstr(1, 2, title, width - 4, curses.A_BOLD)
        idx = 0
        while True:
            for i, opt in enumerate(options):
                attr = curses.A_REVERSE if i == idx else curses.A_NORMAL
                win.addnstr(3 + i, 2, opt[: width - 4], attr)
            win.refresh()
            ch = win.getch()
            if ch in (curses.KEY_UP, ord('k')):
                idx = (idx - 1) % len(options)
            elif ch in (curses.KEY_DOWN, ord('j')):
                idx = (idx + 1) % len(options)
            elif ch in (curses.KEY_ENTER, 10, 13):
                win.clear(); win.refresh(); return idx
            elif ch in (27, ord('q')):
                win.clear(); win.refresh(); return None

    def _show_message(self, stdscr, message: str) -> None:
        lines = message.splitlines() or [message]
        height = min(len(lines) + 4, curses.LINES - 2)
        width = min(max(len(line) for line in lines) + 4, curses.COLS - 2)
        y = max(1, (curses.LINES - height) // 2)
        x = max(1, (curses.COLS - width) // 2)
        win = curses.newwin(height, width, y, x)
        win.box()
        for i, line in enumerate(lines[: height - 4]):
            win.addnstr(2 + i, 2, line[: width - 4], curses.A_NORMAL)
        win.addnstr(height - 2, 2, "Press Enter", curses.A_DIM)
        win.refresh()
        while True:
            ch = win.getch()
            if ch in (curses.KEY_ENTER, 10, 13, 27, ord('q')):
                break
        win.clear()
        win.refresh()

    def _prompt_number(self, stdscr, title: str, default_val: int) -> Optional[int]:
        prompt = f"{title} (current: {default_val}): "
        width = min(max(len(prompt) + 12, 30), curses.COLS - 2)
        height = 5
        y = max(1, (curses.LINES - height) // 2)
        x = max(1, (curses.COLS - width) // 2)
        win = curses.newwin(height, width, y, x)
        win.box()
        win.addnstr(1, 2, prompt[: width - 4], curses.A_BOLD)
        curses.echo()
        try:
            win.move(2, 2)
            s = win.getstr(2, 2, width - 4)
            try:
                val = int(s.decode('utf-8', errors='ignore') or default_val)
            except Exception:
                val = default_val
            return val
        finally:
            curses.noecho()
            win.clear()
            win.refresh()


# ──────────────────────────────────────────────────────────────
# BridgeManager supervising the Node child
# ──────────────────────────────────────────────────────────────
DM_OPTS_STREAM = {"noReply": False, "maxHoldingSeconds": 120}
DM_OPTS_SINGLE = {"noReply": True}
BRIDGE_MIN_S = 0.5
BRIDGE_MAX_S = 30.0
SEND_QUEUE_MAX = 2000


class BridgeManager:
    def __init__(self, node_id: str, env: dict, ui: UnifiedUI, on_dm: Callable[[str, dict], None], on_ready: Optional[Callable[[Optional[str]], None]] = None):
        self.node_id = node_id
        self.env = env
        self.ui = ui
        self.on_dm = on_dm
        self.on_ready = on_ready
        self.proc: Optional[subprocess.Popen[str]] = None
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.addr = ""
        self.backoff = BRIDGE_MIN_S
        self.stdout_thread: Optional[threading.Thread] = None
        self.stderr_thread: Optional[threading.Thread] = None
        self.sender_thread: Optional[threading.Thread] = None
        self.send_q: "queue.Queue[tuple[str, dict, dict]]" = queue.Queue(maxsize=SEND_QUEUE_MAX)

    def start(self):
        with self.lock:
            if self.proc and self.proc.poll() is None:
                return
            try:
                self.proc = subprocess.Popen(
                    ["node", str(BRIDGE_JS)],
                    cwd=BRIDGE_DIR,
                    env=self.env,
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    bufsize=1,
                )
                self.addr = ""
                self.backoff = BRIDGE_MIN_S
                if self.on_ready:
                    self.on_ready(None)
            except Exception as e:  # pragma: no cover
                self.ui.bump(self.node_id, "ERR", f"bridge spawn failed: {e}")
                return
        self.stdout_thread = threading.Thread(target=self._stdout_pump, daemon=True)
        self.stdout_thread.start()
        self.stderr_thread = threading.Thread(target=self._stderr_pump, daemon=True)
        self.stderr_thread.start()
        if not self.sender_thread:
            self.sender_thread = threading.Thread(target=self._sender_loop, daemon=True)
            self.sender_thread.start()

    def dm(self, to: str, data: dict, opts: Optional[dict] = None):
        payload = (to, data, opts or {})
        try:
            self.send_q.put_nowait(payload)
        except queue.Full:
            with contextlib.suppress(Exception):
                _ = self.send_q.get_nowait()
            with contextlib.suppress(Exception):
                self.send_q.put_nowait(payload)

    def shutdown(self):
        self.stop.set()
        with self.lock:
            proc = self.proc
        if proc and proc.poll() is None:
            with contextlib.suppress(Exception):
                if proc.stdin:
                    proc.stdin.close()
            with contextlib.suppress(Exception):
                proc.terminate()
        if self.on_ready:
            self.on_ready(None)

    # internal --------------------------------------------------
    def _stdout_pump(self):
        p = self.proc
        if not p or not p.stdout:
            return
        while not self.stop.is_set():
            line = p.stdout.readline()
            if not line:
                if p.poll() is not None:
                    break
                time.sleep(0.05)
                continue
            try:
                msg = json.loads(line.strip())
            except Exception:
                continue
            typ = msg.get("type")
            if typ == "ready":
                self.addr = msg.get("address") or ""
                self.ui.set_addr(self.node_id, self.addr)
                self.ui.bump(self.node_id, "SYS", f"ready {self.addr}")
                if self.on_ready:
                    self.on_ready(self.addr)
            elif typ == "status":
                state = msg.get("state", "")
                if state in ("probe_fail", "probe_exit", "error", "close"):
                    self.ui.set_state(self.node_id, state)
                self.ui.bump(self.node_id, "SYS", f"bridge {state}")
            elif typ == "nkn-dm":
                src = msg.get("src") or ""
                body = msg.get("msg") or {}
                if isinstance(body, dict) and body.get("event") == "relay.selfprobe":
                    continue
                self.on_dm(src, body)
            elif typ == "err":
                self.ui.bump(self.node_id, "ERR", msg.get("msg", "bridge error"))
        self._restart_later()

    def _stderr_pump(self):
        p = self.proc
        if not p or not p.stderr:
            return
        while not self.stop.is_set():
            line = p.stderr.readline()
            if not line:
                if p.poll() is not None:
                    break
                time.sleep(0.05)
                continue
            self.ui.bump(self.node_id, "ERR", line.strip())

    def _sender_loop(self):
        while not self.stop.is_set():
            try:
                to, data, opts = self.send_q.get(timeout=0.2)
            except queue.Empty:
                continue
            wrote = False
            while not wrote and not self.stop.is_set():
                with self.lock:
                    proc = self.proc
                    stdin = proc.stdin if proc else None
                if proc and proc.poll() is None and stdin:
                    try:
                        payload = {"type": "dm", "to": to, "data": data}
                        if opts:
                            payload["opts"] = opts
                        stdin.write(json.dumps(payload) + "\n")
                        stdin.flush()
                        wrote = True
                        break
                    except Exception:
                        time.sleep(0.1)
                else:
                    time.sleep(0.2)
            self.send_q.task_done()

    def _restart_later(self):
        if self.stop.is_set():
            return
        delay = self.backoff
        self.backoff = min(self.backoff * 2.0, BRIDGE_MAX_S)
        self.ui.set_state(self.node_id, f"restart {delay:.1f}s")
        if self.on_ready:
            self.on_ready(None)
        def _kick():
            time.sleep(delay)
            if not self.stop.is_set():
                self.start()
        threading.Thread(target=_kick, daemon=True).start()


# ──────────────────────────────────────────────────────────────
# Helpers for ASR-specific control messages
# ──────────────────────────────────────────────────────────────
def req_from_asr_start(msg: dict) -> dict:
    opts = msg.get("opts") or {}
    service = (opts.get("service") or "asr").strip()
    return {
        "service": service,
        "path": "/recognize/stream/start",
        "method": "POST",
        "headers": opts.get("headers") or {},
        "timeout_ms": opts.get("timeout_ms") or 45000,
        "verify": opts.get("verify") if isinstance(opts.get("verify"), bool) else None,
        "insecure_tls": opts.get("insecure_tls") in (True, "1", "true", "on"),
    }


def req_from_asr_audio(msg: dict) -> dict:
    sid = (msg.get("sid") or "").strip()
    if not sid:
        raise ValueError("asr.audio missing sid")
    fmt = (msg.get("format") or "pcm16").strip()
    sr = int(msg.get("sr") or 16000)
    body_b64 = msg.get("body_b64") or ""
    if not body_b64:
        raise ValueError("asr.audio missing body_b64")
    opts = msg.get("opts") or {}
    service = (opts.get("service") or "asr").strip()
    headers = {"Content-Type": "application/octet-stream"}
    headers.update(opts.get("headers") or {})
    return {
        "service": service,
        "path": f"/recognize/stream/{sid}/audio?format={fmt}&sr={sr}",
        "method": "POST",
        "headers": headers,
        "body_b64": body_b64,
        "timeout_ms": opts.get("timeout_ms") or 45000,
        "verify": opts.get("verify") if isinstance(opts.get("verify"), bool) else None,
        "insecure_tls": opts.get("insecure_tls") in (True, "1", "true", "on"),
        "stream": False,
    }


def req_from_asr_end(msg: dict) -> dict:
    sid = (msg.get("sid") or "").strip()
    if not sid:
        raise ValueError("asr.end missing sid")
    opts = msg.get("opts") or {}
    service = (opts.get("service") or "asr").strip()
    return {
        "service": service,
        "path": f"/recognize/stream/{sid}/end",
        "method": "POST",
        "headers": opts.get("headers") or {},
        "timeout_ms": opts.get("timeout_ms") or 45000,
        "verify": opts.get("verify") if isinstance(opts.get("verify"), bool) else None,
        "insecure_tls": opts.get("insecure_tls") in (True, "1", "true", "on"),
    }


def req_from_asr_events(msg: dict) -> dict:
    sid = (msg.get("sid") or "").strip()
    if not sid:
        raise ValueError("asr.events missing sid")
    opts = msg.get("opts") or {}
    service = (opts.get("service") or "asr").strip()
    headers = {"Accept": "text/event-stream", "X-Relay-Stream": "chunks"}
    headers.update(opts.get("headers") or {})
    return {
        "service": service,
        "path": f"/recognize/stream/{sid}/events",
        "method": "GET",
        "headers": headers,
        "timeout_ms": opts.get("timeout_ms") or 300000,
        "verify": opts.get("verify") if isinstance(opts.get("verify"), bool) else None,
        "insecure_tls": opts.get("insecure_tls") in (True, "1", "true", "on"),
        "stream": "chunks",
    }


# ──────────────────────────────────────────────────────────────
# Helpers for browser automation requests
# ──────────────────────────────────────────────────────────────
def _browser_opts(msg: dict) -> dict:
    raw = msg.get("opts")
    return raw if isinstance(raw, dict) else {}


def _browser_service(opts: dict) -> str:
    return (opts.get("service") or "web_scrape").strip() or "web_scrape"


def _browser_headers(opts: dict, base: Optional[dict] = None) -> dict:
    headers = {}
    if base:
        headers.update(base)
    extra = opts.get("headers")
    if isinstance(extra, dict):
        headers.update(extra)
    return headers


def _browser_timeout(opts: dict, default_ms: int) -> int:
    raw = opts.get("timeout_ms")
    if raw is None:
        return default_ms
    try:
        return int(raw)
    except Exception:
        return default_ms


def _browser_sid(msg: dict) -> str:
    sid = (msg.get("sid") or "").strip()
    if sid:
        return sid
    opts = _browser_opts(msg)
    sid_opt = opts.get("sid")
    return (sid_opt or "").strip()


def _browser_path_with_sid(base_path: str, sid: str) -> str:
    if not sid:
        return base_path
    return f"{base_path}?sid={urllib.parse.quote_plus(sid)}"


def _browser_request(msg: dict, path: str, *, method: str = "GET", json_body: Optional[dict] = None,
                     headers: Optional[dict] = None, stream: Optional[str] = None, default_timeout_ms: int = 45000) -> dict:
    opts = _browser_opts(msg)
    req = {
        "service": _browser_service(opts),
        "path": path,
        "method": method,
        "headers": _browser_headers(opts, headers or {}),
        "timeout_ms": _browser_timeout(opts, default_timeout_ms),
    }
    if json_body is not None:
        req["json"] = json_body
    if stream:
        req["stream"] = stream
    if isinstance(opts.get("verify"), bool):
        req["verify"] = bool(opts["verify"])
    if opts.get("insecure_tls") in (True, "1", "true", "on"):
        req["insecure_tls"] = True
    return req


def _with_sid_json(msg: dict, payload: dict) -> dict:
    sid = _browser_sid(msg)
    if sid:
        payload.setdefault("sid", sid)
    return payload


def req_from_browser_open(msg: dict) -> dict:
    opts = _browser_opts(msg)
    headless = msg.get("headless")
    if headless is None:
        headless = opts.get("headless")
    if headless is None:
        headless = True
    return _browser_request(
        msg,
        "/session/start",
        method="POST",
        json_body={"headless": bool(headless)},
        default_timeout_ms=_browser_timeout(opts, 60000),
    )


def req_from_browser_close(msg: dict) -> dict:
    return _browser_request(msg, "/session/close", method="POST", json_body={})


def req_from_browser_nav(msg: dict) -> dict:
    url = (msg.get("url") or "").strip()
    if not url:
        raise ValueError("browser.nav missing url")
    body = _with_sid_json(msg, {"url": url})
    return _browser_request(msg, "/navigate", method="POST", json_body=body)


def req_from_browser_click(msg: dict) -> dict:
    selector = (msg.get("selector") or "").strip()
    if not selector:
        raise ValueError("browser.click missing selector")
    body = _with_sid_json(msg, {"selector": selector})
    return _browser_request(msg, "/click", method="POST", json_body=body)


def req_from_browser_type(msg: dict) -> dict:
    selector = (msg.get("selector") or "").strip()
    if not selector:
        raise ValueError("browser.type missing selector")
    if "text" not in msg:
        raise ValueError("browser.type missing text")
    body = _with_sid_json(msg, {"selector": selector, "text": msg.get("text")})
    return _browser_request(msg, "/type", method="POST", json_body=body)


def req_from_browser_scroll(msg: dict) -> dict:
    amount = msg.get("amount", 600)
    try:
        amount = int(amount)
    except Exception:
        raise ValueError("browser.scroll amount must be int") from None
    body = _with_sid_json(msg, {"amount": amount})
    return _browser_request(msg, "/scroll", method="POST", json_body=body)


def req_from_browser_click_xy(msg: dict) -> dict:
    for key in ("x", "y", "viewportW", "viewportH"):
        if key not in msg:
            raise ValueError(f"browser.click_xy missing {key}")
    body = _with_sid_json(
        msg,
        {
            "x": msg.get("x"),
            "y": msg.get("y"),
            "viewportW": msg.get("viewportW"),
            "viewportH": msg.get("viewportH"),
            "naturalW": msg.get("naturalW") or msg.get("naturalWidth"),
            "naturalH": msg.get("naturalH") or msg.get("naturalHeight"),
        },
    )
    return _browser_request(msg, "/click_xy", method="POST", json_body=body)


def req_from_browser_dom(msg: dict) -> dict:
    sid = _browser_sid(msg)
    path = _browser_path_with_sid("/dom", sid)
    return _browser_request(msg, path, method="GET", headers={"Accept": "application/json"}, default_timeout_ms=60000)


def req_from_browser_screenshot(msg: dict) -> dict:
    sid = _browser_sid(msg)
    path = _browser_path_with_sid("/screenshot", sid)
    return _browser_request(msg, path, method="GET", default_timeout_ms=90000)


def req_from_browser_events(msg: dict) -> dict:
    sid = _browser_sid(msg)
    if not sid:
        raise ValueError("browser.events missing sid")
    path = _browser_path_with_sid("/events", sid)
    headers = {"Accept": "text/event-stream", "Cache-Control": "no-cache", "X-Relay-Stream": "lines"}
    return _browser_request(msg, path, method="GET", headers=headers, stream="lines", default_timeout_ms=300000)


def req_from_browser_back(msg: dict) -> dict:
    payload = _with_sid_json(msg, {})
    return _browser_request(msg, "/history/back", method="POST", json_body=payload)


def req_from_browser_forward(msg: dict) -> dict:
    payload = _with_sid_json(msg, {})
    return _browser_request(msg, "/history/forward", method="POST", json_body=payload)


def req_from_browser_scroll_up(msg: dict) -> dict:
    amount = msg.get("amount")
    payload: dict = {}
    if amount is not None:
        try:
            payload["amount"] = abs(int(amount))
        except Exception as exc:
            raise ValueError("browser.scroll_up amount must be int") from exc
    payload = _with_sid_json(msg, payload)
    return _browser_request(msg, "/scroll/up", method="POST", json_body=payload)


def req_from_browser_scroll_down(msg: dict) -> dict:
    amount = msg.get("amount")
    payload: dict = {}
    if amount is not None:
        try:
            payload["amount"] = abs(int(amount))
        except Exception as exc:
            raise ValueError("browser.scroll_down amount must be int") from exc
    payload = _with_sid_json(msg, payload)
    return _browser_request(msg, "/scroll/down", method="POST", json_body=payload)


def req_from_browser_drag(msg: dict) -> dict:
    payload = {}
    for key in ("startX", "startY", "endX", "endY"):
        value = msg.get(key) or msg.get(key.lower()) or msg.get(key.replace("X", "x")) or msg.get(key.replace("Y", "y"))
        if value is None:
            raise ValueError(f"browser.drag missing {key}")
        payload[key] = value
    payload["viewportW"] = msg.get("viewportW") or msg.get("viewport_w") or msg.get("width")
    payload["viewportH"] = msg.get("viewportH") or msg.get("viewport_h") or msg.get("height")
    payload["naturalW"] = msg.get("naturalW") or msg.get("natural_w") or msg.get("naturalWidth") or msg.get("width")
    payload["naturalH"] = msg.get("naturalH") or msg.get("natural_h") or msg.get("naturalHeight") or msg.get("height")
    payload = _with_sid_json(msg, payload)
    return _browser_request(msg, "/drag", method="POST", json_body=payload)


def req_from_browser_scroll_point(msg: dict) -> dict:
    payload = {}
    for key in ("x", "y"):
        if key not in msg and key.upper() not in msg:
            raise ValueError(f"browser.scroll_point missing {key}")
        payload[key] = msg.get(key) or msg.get(key.upper())
    payload["deltaX"] = msg.get("deltaX") or msg.get("delta_x") or 0
    payload["deltaY"] = msg.get("deltaY") or msg.get("delta_y") or 0
    payload["viewportW"] = msg.get("viewportW") or msg.get("viewport_w") or msg.get("width")
    payload["viewportH"] = msg.get("viewportH") or msg.get("viewport_h") or msg.get("height")
    payload["naturalW"] = msg.get("naturalW") or msg.get("natural_w") or msg.get("naturalWidth") or msg.get("width")
    payload["naturalH"] = msg.get("naturalH") or msg.get("natural_h") or msg.get("naturalHeight") or msg.get("height")
    payload = _with_sid_json(msg, payload)
    return _browser_request(msg, "/scroll/point", method="POST", json_body=payload)


# ──────────────────────────────────────────────────────────────
# RelayNode combining bridge + HTTP workers
# ──────────────────────────────────────────────────────────────
class RelayNode:
    def __init__(self, node_cfg: dict, global_cfg: dict, ui: UnifiedUI,
                 assignment_lookup: Optional[Callable[[str], Tuple[Optional[str], Optional[str]]]],
                 address_callback: Optional[Callable[[str, Optional[str]], None]],
                 rate_limit_callback: Optional[Callable[[str, str], bool]] = None,
                 router_event_handler: Optional[Callable[[str, str, dict, "RelayNode"], bool]] = None,
                 nkn_traffic_callback: Optional[Callable[[str, str, dict, str], None]] = None,
                 endpoint_usage_callback: Optional[Callable[[str, List[str]], None]] = None):
        self.cfg = node_cfg
        self.global_cfg = global_cfg
        self.ui = ui
        self.node_id = node_cfg.get("name") or node_cfg.get("id") or secrets.token_hex(4)
        self.ui.add_node(self.node_id, self.node_id)
        # Always share the global targets map so detection updates propagate
        global_targets = global_cfg.setdefault("targets", {})

        explicit_targets = node_cfg.get("targets") or {}
        if explicit_targets:
            # Seed/override global targets with explicit per-node config
            for k, v in explicit_targets.items():
                if v:
                    global_targets[k] = v

        self.targets = global_targets

        http_cfg = global_cfg.get("http", {})
        self.workers_count = int(node_cfg.get("workers") or http_cfg.get("workers") or 4)
        self.max_body = int(node_cfg.get("max_body_b") or http_cfg.get("max_body_b") or (2 * 1024 * 1024))
        self.verify_default = bool(node_cfg.get("verify_default") if node_cfg.get("verify_default") is not None else http_cfg.get("verify_default", True))
        self.chunk_raw_b = int(http_cfg.get("chunk_raw_b", 12 * 1024))
        self.chunk_upload_b = int(http_cfg.get("chunk_upload_b", 600 * 1024))
        self.heartbeat_s = float(http_cfg.get("heartbeat_s", 10))
        self.batch_lines = int(http_cfg.get("batch_lines", 24))
        self.batch_latency = float(http_cfg.get("batch_latency", 0.08))
        self.retry_attempts = int(http_cfg.get("retries", 4))
        self.retry_backoff = float(http_cfg.get("retry_backoff", 0.5))
        self.retry_cap = float(http_cfg.get("retry_cap", 4.0))
        self.jobs: "queue.Queue[dict]" = queue.Queue()
        self.assignment_lookup = assignment_lookup or self._default_assignment_lookup
        self.address_callback = address_callback or (lambda _node, _addr: None)
        self.rate_limit_callback = rate_limit_callback
        self.router_event_handler = router_event_handler
        self.nkn_traffic_callback = nkn_traffic_callback
        self.endpoint_usage_callback = endpoint_usage_callback
        self.primary_service = node_cfg.get("primary_service") or self.node_id
        aliases = node_cfg.get("aliases") or []
        alias_map = {alias.lower(): self.primary_service for alias in aliases}
        alias_map[self.primary_service] = self.primary_service
        if not alias_map:
            alias_map = {
                "asr": "whisper_asr",
                "whisper": "whisper_asr",
                "whisper_asr": "whisper_asr",
                "tts": "piper_tts",
                "piper": "piper_tts",
                "piper_tts": "piper_tts",
                "ollama": "ollama_farm",
                "llm": "ollama_farm",
                "ollama_farm": "ollama_farm",
                "mcp": "mcp_server",
                "context": "mcp_server",
                "mcp_server": "mcp_server",
                "web_scrape": "web_scrape",
                "browser": "web_scrape",
                "chrome": "web_scrape",
                "scrape": "web_scrape",
            }
        self.alias_map = alias_map
        self.current_address: Optional[str] = None
        self.bridge = self._build_bridge()
        self.workers: list[threading.Thread] = []
        self.rate_limit_since: Optional[float] = None
        self.rate_limit_hits: Deque[float] = deque(maxlen=64)
        self.last_rate_limit: Optional[float] = None
        self.upload_sessions: Dict[str, dict] = {}
        self.response_cache: Dict[str, dict] = {}
        self.upload_cleanup_stop = threading.Event()
        self.upload_cleanup_thread: Optional[threading.Thread] = None

    # lifecycle -------------------------------------------------
    def start(self):
        for _ in range(max(1, self.workers_count)):
            t = threading.Thread(target=self._http_worker, daemon=True)
            t.start()
            self.workers.append(t)
        if not self.upload_cleanup_thread:
            self.upload_cleanup_thread = threading.Thread(target=self._upload_cleanup_loop, daemon=True)
            self.upload_cleanup_thread.start()
        self.bridge.start()

    def stop(self):
        for _ in self.workers:
            self.jobs.put(None)  # type: ignore
        self.bridge.shutdown()
        self.upload_cleanup_stop.set()
        # Clear response cache
        self.response_cache.clear()

    # bridge callbacks -----------------------------------------
    def _build_bridge(self) -> BridgeManager:
        bridge_cfg = self.global_cfg.get("bridge", {})
        env = os.environ.copy()
        env["NKN_SEED_HEX"] = (self.cfg.get("seed_hex") or "").lower().replace("0x", "")
        env["NKN_IDENTIFIER"] = self.node_id
        env["NKN_NUM_SUBCLIENTS"] = str(self.cfg.get("num_subclients") or bridge_cfg.get("num_subclients") or 2)
        env["NKN_BRIDGE_SEED_WS"] = str(self.cfg.get("seed_ws") or bridge_cfg.get("seed_ws") or "")
        env["NKN_SELF_PROBE_MS"] = str(self.cfg.get("self_probe_ms") or bridge_cfg.get("self_probe_ms") or 12000)
        env["NKN_SELF_PROBE_FAILS"] = str(self.cfg.get("self_probe_fails") or bridge_cfg.get("self_probe_fails") or 3)
        return BridgeManager(self.node_id, env, self.ui, self._handle_dm, self._on_ready)

    def _on_ready(self, addr: Optional[str]) -> None:
        self.current_address = addr
        self.address_callback(self.node_id, addr)

    def _notify_nkn_traffic(self, direction: str, peer: str, payload: dict, event_name: str = "") -> None:
        cb = self.nkn_traffic_callback
        if not callable(cb):
            return
        try:
            cb(direction, peer, payload if isinstance(payload, dict) else {}, event_name or "")
        except Exception:
            pass

    def _notify_endpoint_usage(self, peer: str, labels: List[str]) -> None:
        cb = self.endpoint_usage_callback
        if not callable(cb) or not labels:
            return
        try:
            cb(peer, labels)
        except Exception:
            pass

    def _dm(self, target: str, payload: dict, opts: Optional[dict] = None) -> None:
        self._notify_nkn_traffic("out", target, payload if isinstance(payload, dict) else {}, str((payload or {}).get("event") or ""))
        if opts is None:
            self.bridge.dm(target, payload)
            return
        self.bridge.dm(target, payload, opts)

    def _handle_dm(self, src: str, body: dict):
        if not isinstance(body, dict):
            return
        event = (body.get("event") or "").lower()
        rid = body.get("id") or ""  # echoed back later
        coarse_service = self._canonical_service(body.get("service") or body.get("target"))
        if not coarse_service:
            if event.startswith("asr."):
                coarse_service = "whisper_asr"
            elif event.startswith("browser."):
                coarse_service = "web_scrape"
            elif event.startswith("relay.") or event.startswith("http."):
                coarse_service = self._canonical_service(body.get("service") or body.get("target"))
        router_events = {
            "resolve_tunnels",
            "resolve_tunnels_result",
            "service_rpc_request",
            "service_rpc_result",
        }
        if event not in router_events:
            self._notify_nkn_traffic("in", src, body, event)
        self.ui.bump(self.node_id, "IN", f"{event or '<unknown>'} {rid}")
        self._record_flow(src, self.node_id, f"{event or '<unknown>'} {rid}", service=coarse_service, channel=src)
        if self.router_event_handler and event in (
            "resolve_tunnels",
            "resolve_tunnels_result",
            "service_rpc_request",
            "service_rpc_result",
        ):
            try:
                if self.router_event_handler(event, src, body, self):
                    return
            except Exception as exc:
                self.ui.bump(self.node_id, "ERR", f"router_event_handler: {exc}")
        if event in ("relay.ping", "ping"):
            self._dm(src, {"event": "relay.pong", "ts": int(time.time() * 1000), "addr": self.bridge.addr})
            return
        if event in ("relay.info", "info"):
            assign_map = self.assignment_lookup("__map__") if self.assignment_lookup else {}
            info = {
                "event": "relay.info",
                "ts": int(time.time() * 1000),
                "addr": self.bridge.addr,
                "services": sorted(self.targets.keys()),
                "workers": self.workers_count,
                "max_body_b": self.max_body,
                "verify_default": self.verify_default,
                "assignments": assign_map,
            }
            self._dm(src, info)
            return
        if event in ("relay.health", "health"):
            assign_map = self.assignment_lookup("__map__") if self.assignment_lookup else {}
            targets_cfg = self.global_cfg.get("targets", {}) if isinstance(self.global_cfg, dict) else {}

            def _assignment(entry):
                if isinstance(entry, dict):
                    return entry.get("node"), entry.get("addr")
                if isinstance(entry, (list, tuple)) and entry:
                    return entry[0], entry[1] if len(entry) > 1 else None
                return None, None

            def _port_of(url: str) -> Optional[int]:
                try:
                    parsed = urllib.parse.urlparse(url)
                    port_val = parsed.port
                    if port_val:
                        return int(port_val)
                    if parsed.scheme == "https":
                        return 443
                    if parsed.scheme == "http":
                        return 80
                except Exception:
                    return None
                return None

            services_payload = []
            for svc_name, info in SERVICE_TARGETS.items():
                target_key = info.get("target") or svc_name
                endpoint = (
                    self.targets.get(target_key)
                    or targets_cfg.get(target_key)
                    or info.get("endpoint")
                    or DEFAULT_TARGETS.get(target_key, "")
                )
                port = _port_of(endpoint) if endpoint else None
                assigned = assign_map.get(svc_name) if isinstance(assign_map, dict) else None
                assigned_node, assigned_addr = _assignment(assigned)
                services_payload.append({
                    "name": svc_name,
                    "target": target_key,
                    "aliases": info.get("aliases", []),
                    "endpoint": endpoint,
                    "port": port,
                    "ports": sorted(dict.fromkeys(info.get("ports", []))),
                    "assigned_node": assigned_node,
                    "assigned_addr": assigned_addr,
                    "this_node": self.node_id if svc_name == self.primary_service else None,
                })

            health = {
                "event": "relay.health",
                "ts": int(time.time() * 1000),
                "node": self.node_id,
                "addr": self.bridge.addr,
                "port_isolation": self.ui.port_isolation_enabled,
                "services": services_payload,
            }
            self._dm(src, health, DM_OPTS_SINGLE)
            return
        try:
            if event == "asr.start":
                req = req_from_asr_start(body)
                if self._check_assignment("whisper_asr", src, rid):
                    self._enqueue_request(src, rid, req)
                return
            if event == "http.upload.begin":
                self._handle_upload_begin(src, rid, body)
                return
            if event == "http.upload.chunk":
                self._handle_upload_chunk(src, rid, body)
                return
            if event == "http.upload.end":
                self._handle_upload_end(src, rid, body)
                return
            if event == "asr.audio":
                req = req_from_asr_audio(body)
                if self._check_assignment("whisper_asr", src, rid):
                    self._enqueue_request(src, rid, req)
                return
            if event == "asr.end":
                req = req_from_asr_end(body)
                if self._check_assignment("whisper_asr", src, rid):
                    self._enqueue_request(src, rid, req)
                return
            if event == "asr.events":
                req = req_from_asr_events(body)
                if self._check_assignment("whisper_asr", src, rid):
                    self._enqueue_request(src, rid, req)
                return
            if event.startswith("browser."):
                if event == "browser.open":
                    req = req_from_browser_open(body)
                elif event == "browser.close":
                    req = req_from_browser_close(body)
                elif event == "browser.nav":
                    req = req_from_browser_nav(body)
                elif event == "browser.click":
                    req = req_from_browser_click(body)
                elif event == "browser.type":
                    req = req_from_browser_type(body)
                elif event == "browser.scroll":
                    req = req_from_browser_scroll(body)
                elif event == "browser.click_xy":
                    req = req_from_browser_click_xy(body)
                elif event == "browser.dom":
                    req = req_from_browser_dom(body)
                elif event == "browser.screenshot":
                    req = req_from_browser_screenshot(body)
                elif event == "browser.events":
                    req = req_from_browser_events(body)
                elif event == "browser.back":
                    req = req_from_browser_back(body)
                elif event == "browser.forward":
                    req = req_from_browser_forward(body)
                elif event == "browser.scroll_up":
                    req = req_from_browser_scroll_up(body)
                elif event == "browser.scroll_down":
                    req = req_from_browser_scroll_down(body)
                elif event == "browser.drag":
                    req = req_from_browser_drag(body)
                elif event == "browser.scroll_point":
                    req = req_from_browser_scroll_point(body)
                else:
                    return
                if self._check_assignment("web_scrape", src, rid):
                    self._enqueue_request(src, rid, req)
                return
        except Exception as e:
            self._dm(src, {
                "event": "relay.response",
                "id": rid,
                "ok": False,
                "status": 0,
                "headers": {},
                "json": None,
                "body_b64": None,
                "truncated": False,
                "error": f"{type(e).__name__}: {e}",
            }, DM_OPTS_SINGLE)
            self.ui.bump(self.node_id, "ERR", f"{event} {e}")
            return
        if event in ("relay.http", "http.request", "relay.fetch"):
            req = body.get("req") or {}
            service_hint = req.get("service") or req.get("target")
            canonical = self._canonical_service(service_hint)
            if self._check_assignment(canonical, src, rid):
                self._enqueue_request(src, rid, req)
            return
        if event == "relay.response.missing":
            uid = body.get("id") or body.get("upload_id") or ""
            missing = body.get("missing") or []
            self._handle_response_missing(src, uid, missing)
            return
        # ignore unknown

    def _canonical_service(self, hint: Optional[str]) -> Optional[str]:
        if not hint:
            return None
        hint = str(hint).lower()
        return self.alias_map.get(hint, hint)

    def _record_flow(self, source: str, target: str, payload: str, service: Optional[str] = None,
                     channel: Optional[str] = None, direction: str = "→", blocked: bool = False) -> None:
        """Route flow events to the UI without breaking headless mode."""
        try:
            if hasattr(self.ui, "record_flow"):
                self.ui.record_flow(source, target, payload, direction=direction, service=service, channel=channel, blocked=blocked)
        except Exception:
            pass

    def _flow_target_label(self, service: Optional[str], url: str) -> str:
        try:
            parsed = urllib.parse.urlparse(url)
            host = parsed.netloc or parsed.path
        except Exception:
            host = url
        if service and host:
            return f"{service}@{host}"
        if service:
            return service
        return host or "service"

    def _record_usage_stats(self, service: Optional[str], addr: str, bytes_in: int = 0, bytes_out: int = 0,
                            start_ts: Optional[float] = None) -> None:
        if not service:
            return
        duration_s = 0.0
        if start_ts is not None:
            duration_s = max(0.0, time.time() - start_ts)
        try:
            self.ui.stats.record_request(service, addr or "unknown", bytes_out=bytes_out, bytes_in=bytes_in, duration_s=duration_s)
        except Exception:
            pass

    def _estimate_body_bytes(self, req: dict) -> int:
        """Best-effort estimate of request body size for stats."""
        try:
            if "body_b64" in req and req["body_b64"] is not None:
                return len(base64.b64decode(str(req["body_b64"]), validate=False))
            if "data" in req and req["data"] is not None:
                val = req["data"]
                return len(val) if isinstance(val, (bytes, bytearray)) else len(str(val).encode("utf-8"))
            if "json" in req and req["json"] is not None:
                return len(json.dumps(req["json"]).encode("utf-8"))
            if req.get("body_chunks_b64"):
                return sum(len(base64.b64decode(str(c), validate=False)) for c in req.get("body_chunks_b64") if c is not None)
            if req.get("json_chunks_b64"):
                return sum(len(base64.b64decode(str(c), validate=False)) for c in req.get("json_chunks_b64") if c is not None)
        except Exception:
            return 0
        return 0

    def _check_assignment(self, service_name: Optional[str], src: str, rid: str) -> bool:
        if not service_name:
            return True
        result = self.assignment_lookup(service_name) if self.assignment_lookup else (None, None)
        if isinstance(result, dict):
            node_id, addr = result.get("node"), result.get("addr")
        else:
            node_id, addr = result
        if node_id and node_id != self.node_id:
            payload = {
                "event": "relay.redirect",
                "service": service_name,
                "id": rid,
                "node": node_id,
                "addr": addr,
                "ts": int(time.time() * 1000),
            }
            if not addr:
                payload["error"] = "service currently offline"
            self._dm(src, payload, DM_OPTS_SINGLE)
            self.ui.bump(self.node_id, "OUT", f"redirect {service_name} -> {node_id}")
            self._record_flow(src, node_id, f"redirect {service_name} {rid}", service=service_name, channel=src)
            return False
        return True

    def _default_assignment_lookup(self, service: str):
        if service == "__map__":
            return {}
        return (None, None)

    def _enqueue_request(self, src: str, rid: str, req: dict):
        self.jobs.put({"src": src, "id": rid, "req": req})
        try:
            self.ui.set_queue(self.node_id, self.jobs.qsize())
        except Exception:
            pass

    # HTTP workers ---------------------------------------------
    def _validate_url_port(self, url: str, service: str = "") -> bool:
        """Validate URL port against known service endpoints (port isolation security)."""
        if not self.ui.port_isolation_enabled:
            return True  # Port isolation disabled, allow all requests

        try:
            from urllib.parse import urlparse

            parsed = urlparse(url)
            port = parsed.port
            if port is None:
                port = 443 if parsed.scheme == "https" else 80

            # Canonicalize service so aliases map correctly
            svc = self._canonical_service(service) if service else ""

            # Start with statically whitelisted ports
            allowed_ports: set[int] = set()
            for svc_key, target_info in SERVICE_TARGETS.items():
                if svc and svc != svc_key and svc not in target_info.get("aliases", []):
                    continue
                for p in target_info.get("ports", []):
                    allowed_ports.add(int(p))

            # Add ports from configured targets (so custom endpoints are honored while isolation is on)
            for target_name, base_url in (self.targets or {}).items():
                if svc and svc != target_name and svc != self._canonical_service(target_name):
                    continue
                try:
                    t_parsed = urlparse(base_url)
                    t_port = t_parsed.port
                    if t_port is None:
                        t_port = 443 if t_parsed.scheme == "https" else 80
                    if t_port:
                        allowed_ports.add(int(t_port))
                except Exception:
                    continue

            return port in allowed_ports
        except Exception:
            # If we can't parse the URL, reject it for security
            return False
        
    def _realign_service_target(self, service_name: Optional[str], failed_url: str) -> bool:
        """
        After a connection failure, try to detect the service's actual port from its log,
        validate it's listening, and update self.targets / SERVICE_TARGETS.

        Returns True if we updated the target and callers should re-resolve the URL.
        """
        if not service_name:
            return False

        svc = self._canonical_service(service_name)
        if not svc or svc not in SERVICE_TARGETS:
            return False

        try:
            parsed = urllib.parse.urlparse(failed_url)
        except Exception:
            parsed = None

        # Prefer configured host hint, then the host from the failed URL, then loopback.
        host_hint = self._service_host_hint(svc)
        requested_host = parsed.hostname if parsed else None
        host = host_hint or requested_host or "127.0.0.1"
        scheme = (parsed.scheme if parsed and parsed.scheme else "http")

        # Detect port from logs
        detected_port = self._detect_service_port_from_log(svc)
        if not detected_port:
            return False

        # This will probe host:port and, if successful, update SERVICE_TARGETS and self.targets
        aligned = self._whitelist_service_port(svc, detected_port, host, scheme,
                                               reason="egress-retry", require_probe=True)
        if aligned:
            LOGGER.info("Realigned %s to detected port %s after connection failure", svc, detected_port)
        return aligned

    def _detect_service_port_from_log(self, service_name: str) -> Optional[int]:
        """Best-effort detection of a service port by tailing its log."""
        log_file = LOGS_ROOT / f"{service_name}.log"
        if not log_file.exists():
            return None
        try:
            import re

            with open(log_file, "r") as f:
                lines = f.readlines()[-100:]
            patterns = [
                r"Running on .*:(\d+)",
                r"listening on .*:(\d+)",
                r"Listening on port (\d+)",
                r"Server.*port (\d+)",
                r"Started.*:(\d+)",
                r"http://[^:]+:(\d+)",
            ]
            for line in reversed(lines):
                for pattern in patterns:
                    match = re.search(pattern, line, re.IGNORECASE)
                    if match:
                        port = int(match.group(1))
                        if 1024 <= port <= 65535:
                            return port
        except Exception as exc:  # pragma: no cover
            LOGGER.debug("On-demand port detection failed for %s: %s", service_name, exc)
        return None

    def _service_host_hint(self, service: str) -> Optional[str]:
        """Extract a host hint from targets or endpoint config."""
        info = SERVICE_TARGETS.get(service) or {}
        target_key = info.get("target") or service
        base = self.targets.get(target_key) or info.get("endpoint") or ""
        try:
            parsed = urllib.parse.urlparse(base)
            return parsed.hostname
        except Exception:
            return None

    def _hosts_equivalent(self, left: Optional[str], right: Optional[str]) -> bool:
        """Treat loopback hostnames as equivalent when comparing."""
        if not left or not right:
            return False
        loopbacks = {"127.0.0.1", "localhost"}
        if left in loopbacks and right in loopbacks:
            return True
        return left == right

    def _probe_service_port(self, host: str, port: int, timeout: float = 0.35) -> bool:
        """Lightweight TCP probe to confirm the port is listening."""
        try:
            import socket

            with socket.create_connection((host, port), timeout=timeout):
                return True
        except Exception:
            return False

    def _whitelist_service_port(self, service: str, port: int, host: Optional[str], scheme: Optional[str], reason: str, require_probe: bool = False) -> bool:
        """Whitelist + align a service port and target endpoint immediately."""
        svc = self._canonical_service(service) or service
        if not svc or port <= 0:
            return False
        info = SERVICE_TARGETS.get(svc)
        if not info:
            return False

        if require_probe and host:
            if not self._probe_service_port(host, port):
                LOGGER.debug("Port isolation: skip %s port %s (%s) probe failed", svc, port, reason)
                return False

        ports = info.setdefault("ports", [])
        changed = False
        if port not in ports:
            ports.append(port)
            LOGGER.info("Port isolation: added port %s for %s (%s)", port, svc, reason)
            changed = True

        target_key = info.get("target") or svc
        base_hint = self.targets.get(target_key) or info.get("endpoint") or ""
        base_host = None
        base_scheme = None
        try:
            if base_hint:
                parsed = urllib.parse.urlparse(base_hint)
                base_host = parsed.hostname
                base_scheme = parsed.scheme
        except Exception:
            pass
        if not base_host and host:
            base_host = host
        if not base_scheme:
            base_scheme = scheme
        if not base_host:
            return changed
        new_base = f"{base_scheme or 'http'}://{base_host}:{port}"
        if new_base != base_hint:
            info["endpoint"] = new_base
            self.targets[target_key] = new_base
            self.global_cfg.setdefault("targets", {})[target_key] = new_base
            try:
                self.ui.update_service_info(svc, {"endpoint": new_base})
            except Exception:
                pass
            LOGGER.info("Port isolation: aligned %s -> %s", svc, new_base)
            changed = True
        return changed

    def _ensure_port_whitelisted(self, url: str, service: str) -> bool:
        """On-demand detection/probe to avoid blocking first requests on new ports."""
        if not self.ui.port_isolation_enabled:
            return True
        svc = self._canonical_service(service) if service else ""
        if not svc:
            return False
        try:
            parsed = urllib.parse.urlparse(url)
        except Exception:
            return False
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        cfg_host = self._service_host_hint(svc)
        requested_host = parsed.hostname
        host_hint = cfg_host or requested_host or "127.0.0.1"
        host_mismatch = bool(cfg_host and requested_host and not self._hosts_equivalent(cfg_host, requested_host))
        scheme = parsed.scheme or "http"

        detected_port = self._detect_service_port_from_log(svc)
        if detected_port:
            aligned = self._whitelist_service_port(svc, detected_port, host_hint, scheme, reason="log-detect", require_probe=True)
            if aligned and not host_mismatch and self._validate_url_port(url, svc):
                return True

        if host_mismatch:
            return False

        if port and host_hint and self._probe_service_port(host_hint, port):
            self._whitelist_service_port(svc, port, host_hint, scheme, reason="probe")
            return self._validate_url_port(url, svc)
        return False

    def _resolve_url(self, req: dict) -> str:
        url = (req.get("url") or "").strip()
        svc_raw = (req.get("service") or "").strip()
        svc = self._canonical_service(svc_raw) if svc_raw else ""
        target_key = svc
        if svc in SERVICE_TARGETS:
            target_key = SERVICE_TARGETS[svc].get("target") or svc
        elif svc_raw:
            target_key = svc_raw

        if url:
            # Validate URL port against known service endpoints
            if not self._validate_url_port(url, svc):
                if not self._ensure_port_whitelisted(url, svc):
                    raise ValueError(f"port isolation: URL '{url}' port not in whitelist for service '{svc}'")
            return url

        base = self.targets.get(target_key) or self.targets.get(svc)
        if not base:
            raise ValueError(f"unknown service '{svc or svc_raw or 'default'}'")
        path = req.get("path") or "/"
        if not path.startswith("/"):
            path = "/" + path
        resolved_url = base.rstrip("/") + path

        # Validate resolved URL port
        if not self._validate_url_port(resolved_url, svc):
            if not self._ensure_port_whitelisted(resolved_url, svc):
                raise ValueError(f"port isolation: resolved URL '{resolved_url}' port not in whitelist for service '{svc}'")

        return resolved_url

    def _http_request_with_retry(self, session: requests.Session, method: str, url: str, **kwargs):
        last_exc = None
        for attempt in range(self.retry_attempts):
            try:
                return session.request(method, url, **kwargs)
            except requests.RequestException as exc:
                last_exc = exc
                time.sleep(min(self.retry_backoff * (2 ** attempt), self.retry_cap))
        if last_exc is not None:
            raise last_exc
        raise RuntimeError("request failed")

    def _http_worker(self):
        session = requests.Session()
        while True:
            job = self.jobs.get()
            if job is None:
                break
            src = job.get("src")
            rid = job.get("id")
            req = job.get("req") or {}
            service_name = self._canonical_service(req.get("service") or req.get("target")) or self.primary_service
            body_bytes = self._estimate_body_bytes(req)
            start_ts = time.time()
            try:
                self._process_request(session, src, rid, req)
            except Exception as e:
                blocked = isinstance(e, ValueError) and "port isolation" in str(e).lower()

                # Derive a useful target label & payload with host:port if possible
                url = req.get("url") or ""
                try:
                    if not url:
                        # Recompute resolved URL just for logging; ignore validation
                        tmp_req = dict(req)
                        tmp_req.setdefault("path", req.get("path") or "/")
                        url = self._resolve_url(tmp_req)
                except Exception:
                    pass

                target_label = self._flow_target_label(service_name, url or (req.get("path") or service_name or ""))

                method = (req.get("method") or "GET").upper()
                path_snippet = req.get("path") or "/"
                try:
                    parsed = urllib.parse.urlparse(url) if url else None
                    hostport = parsed.netloc if parsed else ""
                except Exception:
                    hostport = ""

                if hostport:
                    loc = f"{hostport}{path_snippet}"
                else:
                    loc = path_snippet

                if blocked:
                    payload = f"BLOCKED {method} {loc}: {e}"
                else:
                    payload = f"ERROR {type(e).__name__} {method} {loc}: {e}"

                self._record_flow(
                    src or "client",
                    target_label,
                    payload,
                    service=service_name,
                    channel=src,
                    blocked=blocked,
                )

                self._dm(src, {
                    "event": "relay.response",
                    "id": rid,
                    "ok": False,
                    "status": 0,
                    "headers": {},
                    "json": None,
                    "body_b64": None,
                    "truncated": False,
                    "error": f"{type(e).__name__}: {e}",
                }, DM_OPTS_SINGLE)
                self.ui.bump(self.node_id, "ERR", f"http {type(e).__name__}: {e}")
                self._record_usage_stats(service_name, src, bytes_in=body_bytes, bytes_out=0, start_ts=start_ts)

            finally:
                self.jobs.task_done()
                try:
                    self.ui.set_queue(self.node_id, self.jobs.qsize())
                except Exception:
                    pass

    def _process_request(self, session: requests.Session, src: str, rid: str, req: dict):
        start_ts = time.time()
        url = self._resolve_url(req)
        method = (req.get("method") or "GET").upper()
        headers = req.get("headers") or {}
        timeout_s = float(req.get("timeout_ms") or 30000) / 1000.0

        verify = self.verify_default
        if isinstance(req.get("verify"), bool):
            verify = bool(req.get("verify"))
        if req.get("insecure_tls") in (True, "1", "true", "on"):
            verify = False

        service_name = self._canonical_service(req.get("service") or req.get("target")) or self.primary_service
        endpoint_label = ""
        with contextlib.suppress(Exception):
            parsed_endpoint = urllib.parse.urlparse(url)
            endpoint_base = f"{parsed_endpoint.scheme}://{parsed_endpoint.netloc}" if parsed_endpoint.netloc else url
            endpoint_label = f"{service_name}:{endpoint_base}"
        if endpoint_label:
            self._notify_endpoint_usage(src or "unknown", [endpoint_label])

        # Look up service definition from the watchdog, not from RelayNode
        svc_def = None
        if service_name:
            try:
                from router import ServiceWatchdog  # if this file is named differently, drop the import and just use ServiceWatchdog directly
            except ImportError:
                # Same module, name already in globals – safe to ignore
                pass
            try:
                svc_def = next(
                    (d for d in ServiceWatchdog.DEFINITIONS if d.name == service_name),
                    None,
                )
            except Exception:
                svc_def = None

        # Derive target label + host:port + path for logging
        target_label = self._flow_target_label(service_name, url)
        try:
            parsed = urllib.parse.urlparse(url)
            path_snippet = parsed.path or "/"
            port = parsed.port
            if port is None:
                port = 443 if parsed.scheme == "https" else 80
            host_port = ""
            if parsed.hostname:
                host_port = f"{parsed.hostname}:{port}" if port else parsed.hostname
            elif parsed.netloc:
                host_port = parsed.netloc
        except Exception:
            path_snippet = "/"
            host_port = ""

        want_stream = False
        stream_mode = str(req.get("stream") or headers.get("X-Relay-Stream") or "").strip().lower()
        if stream_mode in ("1", "true", "yes", "on", "chunks", "dm", "lines", "ndjson", "sse", "events"):
            want_stream = True
        if svc_def and getattr(svc_def, "default_stream", False):
            want_stream = True
            if "X-Relay-Stream" not in headers and "x-relay-stream" not in headers:
                headers["X-Relay-Stream"] = "chunks"

        params: Dict[str, Any] = {"headers": headers, "timeout": timeout_s, "verify": verify}
        body_bytes = 0
        body_chunks = req.get("body_chunks_b64") or []
        json_chunks = req.get("json_chunks_b64") or []

        if body_chunks:
            try:
                combined = b"".join(
                    base64.b64decode(str(c), validate=False) for c in body_chunks if c is not None
                )
            except Exception:
                combined = b""
            params["data"] = combined
            body_bytes = len(combined)
        elif json_chunks:
            try:
                combined = b"".join(
                    base64.b64decode(str(c), validate=False) for c in json_chunks if c is not None
                )
                params["data"] = combined
                headers.setdefault("Content-Type", "application/json")
            except Exception:
                params["data"] = b""
            body_bytes = len(params.get("data") or b"")
        elif "json" in req and req["json"] is not None:
            params["json"] = req["json"]
            try:
                body_bytes = len(json.dumps(req["json"]).encode("utf-8"))
            except Exception:
                body_bytes = 0
        elif "body_b64" in req and req["body_b64"] is not None:
            try:
                params["data"] = base64.b64decode(str(req["body_b64"]), validate=False)
            except Exception:
                params["data"] = b""
            body_bytes = len(params.get("data") or b"")
        elif "data" in req and req["data"] is not None:
            params["data"] = req["data"]
            try:
                body_bytes = (
                    len(req["data"])
                    if isinstance(req["data"], (bytes, bytearray))
                    else len(str(req["data"]).encode("utf-8"))
                )
            except Exception:
                body_bytes = 0

        realigned = False  # Ensure we only realign/retry once per request

        def _retry_after_realign(exc: Exception, stream: bool = False) -> requests.Response:
            nonlocal url, target_label, path_snippet, host_port, realigned

            # Only trigger on connection-type issues, once
            if realigned:
                raise exc
            if not isinstance(exc, (requests.ConnectionError, requests.Timeout)):
                raise exc
            if not service_name:
                raise exc

            if not self._realign_service_target(service_name, url):
                raise exc

            realigned = True
            # Re-resolve against updated self.targets (includes detection updates)
            url = self._resolve_url(req)
            target_label = self._flow_target_label(service_name, url)
            try:
                parsed2 = urllib.parse.urlparse(url)
                path_snippet = parsed2.path or "/"
                port2 = parsed2.port
                if port2 is None:
                    port2 = 443 if parsed2.scheme == "https" else 80
                host_port = ""
                if parsed2.hostname:
                    host_port = f"{parsed2.hostname}:{port2}" if port2 else parsed2.hostname
                elif parsed2.netloc:
                    host_port = parsed2.netloc
            except Exception:
                path_snippet = "/"
                host_port = ""

            if stream:
                return self._http_request_with_retry(session, method, url, stream=True, **params)
            return self._http_request_with_retry(session, method, url, **params)

        # --- streaming path ---
        if want_stream:
            try:
                resp = self._http_request_with_retry(session, method, url, stream=True, **params)
            except requests.RequestException as exc:
                # Try one fast realign + retry on connection failure
                resp = _retry_after_realign(exc, stream=True)

            if resp.status_code == 429:
                self._register_rate_limit_hit()
                self._send_simple_response(src, rid, resp, req)
                self._record_usage_stats(
                    service_name,
                    src,
                    bytes_in=body_bytes,
                    bytes_out=len(resp.content or b""),
                    start_ts=start_ts,
                )
                return

            self._reset_rate_limit()
            stream_mode_resolved = self._infer_stream_mode(stream_mode, resp)
            bytes_out = self._handle_stream(
                src, rid, resp, stream_mode_resolved, service_name, body_bytes, start_ts
            )
            stream_loc = f"{host_port}{path_snippet}" if host_port else path_snippet
            self._record_flow(
                target_label,
                src or "client",
                f"STREAM {resp.status_code} {method} {stream_loc} {rid}",
                service=service_name,
                channel=src,
                direction="←",
            )
            self._record_usage_stats(
                service_name, src, bytes_in=body_bytes, bytes_out=bytes_out, start_ts=start_ts
            )
            return

        # --- non-streaming path ---
        try:
            resp = self._http_request_with_retry(session, method, url, **params)
        except requests.RequestException as exc:
            resp = _retry_after_realign(exc, stream=False)

        self._handle_response_status(resp.status_code)
        bytes_out = self._send_simple_response(src, rid, resp, req)
        resp_loc = f"{host_port}{path_snippet}" if host_port else path_snippet
        self._record_flow(
            target_label,
            src or "client",
            f"RESP {resp.status_code} {method} {resp_loc} {rid}",
            service=service_name,
            channel=src,
            direction="←",
        )
        self._record_usage_stats(
            service_name, src, bytes_in=body_bytes, bytes_out=bytes_out, start_ts=start_ts
        )



    def _handle_response_status(self, status_code: int) -> None:
        if status_code == 429:
            self._register_rate_limit_hit()
        else:
            self._reset_rate_limit()

    def _send_upload_error(self, src: str, rid: str, message: str, status: int = 400) -> None:
        payload = {
            "event": "relay.response",
            "id": rid,
            "ok": False,
            "status": status,
            "headers": {},
            "json": None,
            "body_b64": None,
            "truncated": False,
            "error": message,
        }
        self._dm(src, payload, DM_OPTS_SINGLE)
        self.ui.bump(self.node_id, "ERR", f"upload {rid}: {message}")

    def _log_upload(self, rid: str, text: str) -> None:
        self.ui.bump(self.node_id, "IN", f"upload {rid}: {text}")

    def _request_missing(self, uid: str, entry: dict, missing: list[int]) -> None:
        if not missing:
            return
        src = entry.get("src") or ""
        rid = entry.get("rid") or uid
        payload = {
            "event": "http.upload.missing",
            "id": rid,
            "upload_id": uid,
            "missing": missing,
            "total": entry.get("total") or len(entry.get("chunks") or []),
            "got": entry.get("got") or 0,
        }
        self._dm(src, payload, DM_OPTS_SINGLE)
        self._log_upload(rid, f"request missing {len(missing)}")

    def _handle_upload_begin(self, src: str, rid: str, body: dict) -> None:
        uid = body.get("upload_id") or rid
        if not uid:
            return
        req = body.get("req") or {}
        total = int(body.get("total") or body.get("total_chunks") or 0)
        ctype = body.get("content_type") or (req.get("headers") or {}).get("Content-Type") or ""
        entry = self.upload_sessions.get(uid)
        if entry:
            # Merge begin into an existing session started implicitly from a chunk
            if not entry.get("req"):
                entry["req"] = req
            if not entry.get("ctype"):
                entry["ctype"] = ctype
            if not entry.get("total") and total:
                entry["total"] = total
                entry["chunks"] = [None] * total
            self._log_upload(rid, f"begin merge total={entry.get('total')}")
        else:
            entry = {
                "src": src,
                "rid": rid,
                "req": req,
                "chunks": [None] * total if total > 0 else [],
                "total": total,
                "got": 0,
                "ended": False,
                "ctype": ctype,
                "created": time.time(),
            }
            self.upload_sessions[uid] = entry
            self._log_upload(rid, f"begin total={total}")

    def _handle_upload_chunk(self, src: str, rid: str, body: dict) -> None:
        uid = body.get("upload_id") or rid
        entry = self.upload_sessions.get(uid)
        if not entry:
            # If the chunk includes req (first chunk) recover by creating a session on the fly
            req = body.get("req")
            if not req:
                # Late or duplicate chunk after completion; ignore quietly (common after retry)
                # Only log at debug level to avoid spam from expected retry duplicates
                seq = int(body.get("seq") or 0)
                if seq > 0:
                    # This is normal - chunk arrived after upload completed (from retry mechanism)
                    pass  # Silent ignore
                else:
                    self._log_upload(rid, "chunk received with no active upload; ignoring")
                return
            total_chunks = int(body.get("total") or body.get("total_chunks") or 0)
            ctype = body.get("content_type") or (req.get("headers") or {}).get("Content-Type") or ""
            entry = {
                "src": src,
                "rid": rid,
                "req": req,
                "chunks": [None] * total_chunks if total_chunks > 0 else [],
                "total": total_chunks,
                "got": 0,
                "ended": False,
                "ctype": ctype,
                "created": time.time(),
            }
            self.upload_sessions[uid] = entry
            self._log_upload(rid, f"implicit begin from chunk total={total_chunks}")
        b64 = body.get("b64") or ""
        try:
            raw = base64.b64decode(str(b64), validate=False)
        except Exception:
            self._send_upload_error(src, rid, "invalid chunk b64", 400)
            return
        if len(raw) > self.chunk_upload_b:
            self._send_upload_error(src, rid, f"chunk too large ({len(raw)} > {self.chunk_upload_b})", 413)
            self.upload_sessions.pop(uid, None)
            return
        seq = int(body.get("seq") or 0)
        total = entry.get("total") or 0
        if total > 0 and (seq < 1 or seq > total):
            self._send_upload_error(src, rid, "chunk seq out of range", 400)
            return
        if total > 0:
            if entry["chunks"][seq - 1] is None:
                entry["got"] += 1
            entry["chunks"][seq - 1] = raw
        else:
            entry["chunks"].append(raw)
            entry["got"] += 1
        entry["last"] = time.time()
        self._log_upload(rid, f"chunk {seq}/{total or '?'} got={entry['got']}")
        if entry.get("ended") and ((total > 0 and entry["got"] >= total) or total == 0):
            self._finalize_upload(uid, entry)

    def _handle_upload_end(self, src: str, rid: str, body: dict) -> None:
        uid = body.get("upload_id") or rid
        entry = self.upload_sessions.get(uid)
        if not entry:
            self._send_upload_error(src, rid, "unknown upload id", 404)
            return
        entry["ended"] = True
        entry["end_received_time"] = time.time()  # Track when end was received
        total = entry.get("total") or 0
        self._log_upload(rid, f"end received got={entry['got']} total={total}")
        if total == 0 or entry["got"] >= total:
            self._finalize_upload(uid, entry)
        else:
            self._finalize_upload(uid, entry, allow_partial=False)

    def _finalize_upload(self, uid: str, entry: dict, allow_partial: bool = False) -> None:
        chunks = entry.get("chunks") or []
        missing = [i + 1 for i, ch in enumerate(chunks) if ch is None]
        if missing and not allow_partial:
            # Add grace period: don't request missing chunks if we just received 'end'
            # Give in-flight packets 2-3 seconds to arrive before requesting resend
            now = time.time()
            missing_req_time = entry.get("missing_requested") or 0
            end_received_time = entry.get("end_received_time") or now
            time_since_end = now - end_received_time

            # If we haven't requested missing yet and end was recent, wait a bit
            if not missing_req_time and time_since_end < 2.0:
                # Don't request yet - let cleanup loop handle it after grace period
                return

            # If we already requested and not enough time passed, wait
            if missing_req_time and (now - missing_req_time) < 1.0:
                return

            entry["missing_requested"] = now
            self._request_missing(uid, entry, missing)
            return
        data = b"".join(ch for ch in chunks if ch is not None)
        req = dict(entry.get("req") or {})
        ctype = entry.get("ctype") or (req.get("headers") or {}).get("Content-Type") or ""
        if "application/json" in ctype:
            try:
                text = data.decode("utf-8", errors="ignore")
                req["json"] = json.loads(text)
            except Exception:
                req["body_b64"] = base64.b64encode(data).decode("ascii")
        else:
            req["body_b64"] = base64.b64encode(data).decode("ascii")
        self._enqueue_request(entry.get("src") or "", entry.get("rid") or "", req)
        missing_note = f" missing={len(missing)}" if missing else ""
        self._log_upload(entry.get("rid") or uid, f"complete bytes={len(data)}{missing_note}")
        # Only drop the session once we’ve enqueued the HTTP request (or finalized partial)
        self.upload_sessions.pop(uid, None)

    def _handle_response_missing(self, src: str, rid: str, missing: List[int]) -> None:
        if not missing:
            return
        cache = self.response_cache.get(rid)
        if not cache:
            return
        chunks = cache.get("chunks") or {}
        for seq in missing:
            b64 = chunks.get(seq)
            if not b64:
                continue
            payload = {
                "event": "relay.response.chunk",
                "id": rid,
                "seq": seq,
                "b64": b64,
            }
            self._dm(src, payload, DM_OPTS_STREAM)

    def _upload_cleanup_loop(self) -> None:
        """Sweep unfinished uploads so they don't stall forever."""
        while not self.upload_cleanup_stop.is_set():
            try:
                now = time.time()
                to_finish: List[Tuple[str, dict]] = []
                to_retry: List[Tuple[str, dict]] = []  # For grace period retry
                to_error: List[Tuple[str, dict]] = []
                for uid, entry in list(self.upload_sessions.items()):
                    created = float(entry.get("created") or now)
                    age = now - created
                    got = int(entry.get("got") or 0)
                    ended = bool(entry.get("ended"))
                    total = int(entry.get("total") or 0)
                    missing_requested = float(entry.get("missing_requested") or 0)
                    end_received_time = float(entry.get("end_received_time") or 0)
                    time_since_end = now - end_received_time if end_received_time else age

                    # If we got something but never saw end within 20s, finalize partial
                    if got > 0 and age >= 20 and not ended:
                        to_finish.append((uid, entry))
                    # If we saw end but missing chunks, handle grace period logic
                    elif ended and (total == 0 or got < total):
                        # If missing not yet requested and grace period (2s) elapsed, trigger request
                        if not missing_requested and time_since_end >= 2.0:
                            to_retry.append((uid, entry))
                        # If missing requested and 10s elapsed since request, give up with partial
                        elif missing_requested and (now - missing_requested) >= 10:
                            to_finish.append((uid, entry))
                        # Legacy: if no end timestamp and age >= 10s, finalize
                        elif not missing_requested and not end_received_time and age >= 10:
                            to_finish.append((uid, entry))
                    # If nothing arrived within 20s, give up with an error
                    elif got == 0 and age >= 20:
                        to_error.append((uid, entry))
                # Retry finalization for sessions past grace period
                for uid, entry in to_retry:
                    self._log_upload(entry.get("rid") or uid, f"grace period elapsed, retry finalize")
                    self._finalize_upload(uid, entry, allow_partial=False)
                # Finalize partial uploads that timed out
                for uid, entry in to_finish:
                    self._log_upload(entry.get("rid") or uid, f"cleanup finalize got={entry.get('got')} total={entry.get('total')}")
                    self._finalize_upload(uid, entry, allow_partial=True)
                # Error out uploads that never received any chunks
                for uid, entry in to_error:
                    rid = entry.get("rid") or uid
                    src = entry.get("src") or ""
                    self._log_upload(rid, "cleanup timeout (no chunks)")
                    self._send_upload_error(src, rid, "upload timed out before chunks arrived", 408)
                    self.upload_sessions.pop(uid, None)
            except Exception:
                pass
            self.upload_cleanup_stop.wait(2.0)

    def _register_rate_limit_hit(self) -> None:
        now = time.time()
        if self.rate_limit_since is None:
            self.rate_limit_since = now
        self.last_rate_limit = now
        self.rate_limit_hits.append(now)
        while self.rate_limit_hits and now - self.rate_limit_hits[0] > 60.0:
            self.rate_limit_hits.popleft()
        if self.rate_limit_since and (now - self.rate_limit_since) >= 60.0:
            if self.rate_limit_callback and self.rate_limit_callback(self.primary_service, self.node_id):
                self.rate_limit_since = None
                self.rate_limit_hits.clear()

    def _reset_rate_limit(self) -> None:
        if self.rate_limit_since is not None:
            self.rate_limit_since = None
            self.rate_limit_hits.clear()
            self.last_rate_limit = None

    def _send_simple_response(self, src: str, rid: str, resp: requests.Response, req: Optional[dict] = None) -> int:
        raw = resp.content or b""
        truncated = False
        if len(raw) > self.max_body:
            raw = raw[: self.max_body]
            truncated = True
        payload = {
            "event": "relay.response",
            "id": rid,
            "ok": True,
            "status": int(resp.status_code),
            "headers": {k.lower(): v for k, v in resp.headers.items()},
            "json": None,
            "body_b64": None,
            "truncated": truncated,
            "error": None,
        }
        content_type = (resp.headers.get("Content-Type") or "").lower()
        if "application/json" in content_type:
            try:
                parsed_json = resp.json()
                payload["json"] = self._sanitize_response_json(req, parsed_json)
            except Exception:
                payload["body_b64"] = base64.b64encode(raw).decode("ascii")
        elif len(raw) <= self.max_body:
            payload["body_b64"] = base64.b64encode(raw).decode("ascii")
        else:
            payload["body_b64"] = base64.b64encode(raw).decode("ascii")
        self._dm(src, payload, DM_OPTS_SINGLE)
        # Track stats with bytes sent and NKN address
        bytes_sent = len(raw)
        self.ui.bump(self.node_id, "OUT", f"{payload['status']} {rid}", nkn_addr=src, bytes_sent=bytes_sent)
        service_name = self._canonical_service((req or {}).get("service") or (req or {}).get("target"))
        origin = service_name or "service"
        self._record_flow(origin, src, f"{payload['status']} {rid}", service=service_name, channel=src, direction="←")
        return bytes_sent

    def _sanitize_response_json(self, req: Optional[dict], data: Any) -> Any:
        try:
            if not isinstance(data, (dict, list)):
                return data

            target = str((req or {}).get("service") or (req or {}).get("target") or "").lower()
            path = str((req or {}).get("path") or "").lower()
            url = str((req or {}).get("url") or "").lower()

            def looks_like_show_payload(obj: Any, depth: int = 0) -> bool:
                if depth > 3:
                    return False
                if isinstance(obj, dict):
                    if any(isinstance(k, str) and k.lower() == "license" for k in obj.keys()):
                        return True
                    if "modelfile" in obj or "modelfile_sha" in obj:
                        return True
                    return any(looks_like_show_payload(v, depth + 1) for v in obj.values())
                if isinstance(obj, list):
                    return any(looks_like_show_payload(v, depth + 1) for v in obj)
                return False

            maybe_show = False
            if any(seg in target for seg in ("ollama", "llm")) and any(seg in path or seg in url for seg in ("/show", "/api/show")):
                maybe_show = True
            elif looks_like_show_payload(data):
                maybe_show = True

            if not maybe_show:
                return data

            def strip_license(obj: Any) -> Any:
                if isinstance(obj, dict):
                    new_obj = {}
                    removed = False
                    for key, value in obj.items():
                        if isinstance(key, str) and key.lower() == "license":
                            removed = True
                            continue
                        new_obj[key] = strip_license(value)
                    if removed:
                        new_obj["license"] = "[omitted]"
                    return new_obj
                if isinstance(obj, list):
                    return [strip_license(item) for item in obj]
                return obj

            return strip_license(data)
        except Exception:
            return data

    def _infer_stream_mode(self, mode: str, resp: requests.Response) -> str:
        if mode in ("lines", "ndjson", "line"):
            return "lines"
        if mode in ("sse", "events"):
            return "lines"
        ctype = (resp.headers.get("Content-Type") or "").lower()
        if "text/event-stream" in ctype or "application/x-ndjson" in ctype:
            return "lines"
        if "json" in ctype and "stream" in ctype:
            return "lines"
        return "chunks"

    def _handle_stream(self, src: str, rid: str, resp: requests.Response, mode: str,
                      service_name: Optional[str], bytes_in: int, start_ts: float) -> int:
        headers = {k.lower(): v for k, v in resp.headers.items()}
        filename = None
        cd = resp.headers.get("Content-Disposition") or resp.headers.get("content-disposition") or ""
        if cd:
            import re
            import urllib.parse

            m = re.search(r"filename\*=UTF-8''([^;]+)|filename=\"?([^\";]+)\"?", cd, re.I)
            if m:
                filename = urllib.parse.unquote(m.group(1) or m.group(2))
        cl_raw = resp.headers.get("Content-Length") or resp.headers.get("content-length")
        try:
            cl_num = int(cl_raw) if cl_raw is not None else None
        except Exception:
            cl_num = None
        begin_payload = {
            "event": "relay.response.begin",
            "id": rid,
            "ok": resp.status_code < 400,
            "status": int(resp.status_code),
            "headers": headers,
            "content_length": cl_num,
            "filename": filename,
            "ts": int(time.time() * 1000),
        }
        self._dm(src, begin_payload, DM_OPTS_STREAM)
        self.ui.bump(self.node_id, "OUT", f"stream begin {rid}")

        if mode == "lines":
            total_bytes = self._stream_lines(src, rid, resp)
        else:
            total_bytes = self._stream_chunks(src, rid, resp)

        return total_bytes

    def _stream_lines(self, src: str, rid: str, resp: requests.Response) -> int:
        decoder = codecs.getincrementaldecoder("utf-8")()
        text_buf = ""
        batch = []
        seq = 0
        total_bytes = 0
        total_lines = 0
        last_flush = time.time()
        hb_deadline = time.time() + self.heartbeat_s
        done_seen = False
        ok = True
        error_msg = None

        def flush_batch():
            nonlocal batch, last_flush
            if not batch:
                return
            payload = {
                "event": "relay.response.lines",
                "id": rid,
                "lines": batch,
            }
            self._dm(src, payload, DM_OPTS_STREAM)
            batch = []
            last_flush = time.time()

        try:
            for chunk in resp.iter_content(chunk_size=self.chunk_raw_b):
                if chunk:
                    total_bytes += len(chunk)
                    text_buf += decoder.decode(chunk)
                    while True:
                        idx = text_buf.find("\n")
                        if idx < 0:
                            break
                        line = text_buf[:idx]
                        text_buf = text_buf[idx + 1 :]
                        if not line.strip():
                            continue
                        seq += 1
                        total_lines += 1
                        try:
                            maybe = json.loads(line)
                            if isinstance(maybe, dict) and maybe.get("done") is True:
                                done_seen = True
                        except Exception:
                            pass
                        batch.append({
                            "seq": seq,
                            "ts": int(time.time() * 1000),
                            "line": line,
                        })
                        if (
                            len(batch) >= self.batch_lines
                            or (time.time() - last_flush) >= self.batch_latency
                        ):
                            flush_batch()
                if time.time() >= hb_deadline:
                    self._dm(
                        src,
                        {
                            "event": "relay.response.keepalive",
                            "id": rid,
                            "ts": int(time.time() * 1000),
                        },
                        DM_OPTS_STREAM,
                    )
                    hb_deadline = time.time() + self.heartbeat_s

            # flush any remaining text
            tail = decoder.decode(b"", final=True)
            if tail.strip():
                seq += 1
                total_lines += 1
                batch.append({
                    "seq": seq,
                    "ts": int(time.time() * 1000),
                    "line": tail,
                })
            flush_batch()
        except Exception as e:
            ok = False
            error_msg = f"{type(e).__name__}: {e}"
            try:
                flush_batch()
            except Exception:
                pass
            self.ui.bump(self.node_id, "ERR", f"stream lines {e}")

        end_payload = {
            "event": "relay.response.end",
            "id": rid,
            "ok": ok,
            "bytes": total_bytes,
            "last_seq": seq,
            "lines": total_lines,
            "done_seen": done_seen,
        }
        if error_msg:
            end_payload["error"] = error_msg
        self._dm(src, end_payload, DM_OPTS_STREAM)
        if ok:
            self.ui.bump(self.node_id, "OUT", f"stream end {rid}")
        else:
            self.ui.bump(self.node_id, "ERR", f"stream lines {error_msg}")
        return total_bytes


    def _stream_chunks(self, src: str, rid: str, resp: requests.Response) -> int:
        total = 0
        seq = 0
        last_send = time.time()
        cache_entry = {"chunks": {}, "created": time.time()}
        self.response_cache[rid] = cache_entry
        try:
            for chunk in resp.iter_content(chunk_size=self.chunk_raw_b):
                if not chunk:
                    if time.time() - last_send >= self.heartbeat_s:
                        self._dm(src, {"event": "relay.response.keepalive", "id": rid, "ts": int(time.time() * 1000)}, DM_OPTS_STREAM)
                        last_send = time.time()
                    continue
                total += len(chunk)
                seq += 1
                b64 = base64.b64encode(chunk).decode("ascii")
                payload = {
                    "event": "relay.response.chunk",
                    "id": rid,
                    "seq": seq,
                    "b64": b64,
                }
                cache_entry["chunks"][seq] = b64
                self._dm(src, payload, DM_OPTS_STREAM)
                last_send = time.time()
        except Exception as e:
            self._dm(src, {
                "event": "relay.response.end",
                "id": rid,
                "ok": False,
                "bytes": total,
                "last_seq": seq,
                "truncated": False,
                "error": f"{type(e).__name__}: {e}",
            }, DM_OPTS_STREAM)
            self.ui.bump(self.node_id, "ERR", f"stream chunks {e}")
            self.response_cache.pop(rid, None)
            return
        self._dm(src, {
            "event": "relay.response.end",
            "id": rid,
            "ok": True,
            "bytes": total,
            "last_seq": seq,
            "truncated": False,
            "error": None,
        }, DM_OPTS_STREAM)
        # keep cache briefly for resend handling; cleanup after short delay
        threading.Timer(5.0, lambda: self.response_cache.pop(rid, None)).start()
        self.ui.bump(self.node_id, "OUT", f"stream end {rid}")
        return total


# ──────────────────────────────────────────────────────────────
# Router supervisor
# ──────────────────────────────────────────────────────────────
class Router:
    @staticmethod
    def _service_slug_static(service: str) -> str:
        slug = ''.join(ch if ch.isalnum() else '-' for ch in service.lower())
        slug = '-'.join(filter(None, slug.split('-')))
        return slug or 'relay'

    @classmethod
    def _relay_name_static(cls, service: str, seed_hex: str) -> str:
        slug = cls._service_slug_static(service)
        ident = (seed_hex or '').lower().replace('0x', '')
        if ident:
            ident = ident[:8]
        return f"{slug}-relay-{ident}" if ident else f"{slug}-relay"

    def __init__(self, cfg: dict, use_ui: bool):
        self.cfg: Dict[str, Any] = {}
        self.targets: Dict[str, str] = {}
        self.feature_flags: Dict[str, bool] = {
            "router_control_plane_api": True,
            "resolver_auto_apply": True,
            "cloudflared_manager": False,
        }
        self.api_enabled = True
        self.api_host = "127.0.0.1"
        self.api_port = 9071
        self.nkn_settings: Dict[str, Any] = {}
        self.cloudflared_enabled = False
        self.cloudflared_cfg: Dict[str, Any] = {}
        self.auth_cfg: Dict[str, Any] = {}
        self.ticket_keys: Dict[str, str] = {}
        self.ticket_accepted_kids: List[str] = []
        self.ticket_replay_cache: Dict[str, float] = {}
        self.ticket_replay_lock = threading.Lock()
        self.marketplace_cfg: Dict[str, Any] = {}
        self.marketplace_sync_cfg: Dict[str, Any] = {}
        self.marketplace_sync_worker: Optional[threading.Thread] = None
        self.marketplace_sync_state_lock = threading.Lock()
        self.marketplace_sync_state: Dict[str, Any] = {
            "in_flight": False,
            "last_trigger": "",
            "last_attempt_ts_ms": 0,
            "last_success_ts_ms": 0,
            "last_failure_ts_ms": 0,
            "last_error": "",
            "last_status_code": 0,
            "last_result": {},
            "results": deque(maxlen=24),
            "target_urls": [],
            "next_due_ts_ms": 0,
            "success_count": 0,
            "failure_count": 0,
            "consecutive_failures": 0,
        }
        self.service_publication_cfg: Dict[str, Dict[str, Any]] = {}
        self.catalog_runtime_overrides: Dict[str, Dict[str, Any]] = {}
        self.config_dirty = False

        self.use_ui = use_ui
        self.startup_time = time.time()
        self.ui = EnhancedUI(use_ui, STATE_CONFIG_PATH)
        self.ui.set_action_handler(self.handle_ui_action)
        ensure_bridge()
        self._apply_runtime_config(cfg)

        self.watchdog = ServiceWatchdog(BASE_DIR, watchdog_config=self.cfg.get("watchdog", {}))
        self.watchdog.ensure_sources(service_config=self.ui.service_config)
        for svc, enabled in self.watchdog.desired_state().items():
            self.ui.service_config[svc] = bool(enabled)
        self.latest_service_status: Dict[str, dict] = {
            snap["name"]: snap for snap in self.watchdog.get_snapshot()
        }
        self.request_counter = {"value": 0}
        self.snapshot_lock = threading.Lock()
        self.snapshot_cache: Dict[str, Any] = {"ts_ms": 0, "services": {}, "resolved": {}, "stale": False}
        self.last_good_service_payloads: Dict[str, dict] = {}
        self.snapshot_failures: Dict[str, int] = {}
        self.pending_resolves: Dict[str, dict] = {}
        self.pending_resolves_lock = threading.Lock()
        self.resolve_sweeper_stop = threading.Event()
        self.resolve_sweeper_thread: Optional[threading.Thread] = None
        self.telemetry_lock = threading.Lock()
        self.telemetry_state: Dict[str, Any] = {
            "inbound_messages": 0,
            "outbound_messages": 0,
            "inbound_bytes": 0,
            "outbound_bytes": 0,
            "resolve_requests_in": 0,
            "resolve_requests_out": 0,
            "resolve_success_out": 0,
            "resolve_fail_out": 0,
            "rpc_requests_in": 0,
            "rpc_requests_out": 0,
            "rpc_success_out": 0,
            "rpc_fail_out": 0,
            "peer_usage": {},
            "endpoint_hits": {},
            "history": deque(maxlen=240),
            "rpc_metering": {
                "requests": 0,
                "success": 0,
                "failure": 0,
                "estimated_units_total": 0.0,
                "estimated_micros_total": 0,
                "settled_micros_total": 0,
                "by_service": {},
                "events": deque(maxlen=300),
            },
            "rpc_enforcement": {
                "checks": 0,
                "allow": 0,
                "deny": 0,
                "denied_codes": {},
                "by_service": {},
                "last_event": {},
            },
        }
        self.activity_log: Deque[dict] = deque(maxlen=240)
        self.api_server = None
        self.api_thread: Optional[threading.Thread] = None
        self.cloudflared_manager = (
            CloudflaredManager(WATCHDOG_RUNTIME_ROOT / "cloudflared", self.cloudflared_cfg, logger=LOGGER.info)
            if self.cloudflared_enabled
            else None
        )

        self.assignment_lock = threading.Lock()
        self.config_dirty = bool(self.config_dirty)
        self.rate_limit_state: Dict[str, dict] = {}
        self.service_relays = self._ensure_service_relays()
        self.service_assignments = self._init_assignments()

        self.nodes: List[RelayNode] = []
        self.service_nodes: Dict[str, RelayNode] = {}
        self.node_map: Dict[str, RelayNode] = {}
        self.node_addresses: Dict[str, Optional[str]] = {}

        self.stop = threading.Event()
        self.status_thread: Optional[threading.Thread] = None

        self.daemon_mgr = DaemonManager()
        self.daemon_info = self.daemon_mgr.check()
        self.ui.set_daemon_info(self.daemon_info)

        for service_name, relay_cfg in self.service_relays.items():
            node = self._create_relay_node(service_name, relay_cfg)
            self.nodes.append(node)
            self.node_map[node.node_id] = node
            self.service_nodes[service_name] = node
            self.node_addresses[node.node_id] = None

        self._refresh_node_assignments()

    # Control plane + snapshot -------------------------------------------------
    @staticmethod
    def _as_bool(value: Any, default: bool = False) -> bool:
        if isinstance(value, bool):
            return value
        if value is None:
            return default
        text = str(value).strip().lower()
        if text in ("1", "true", "yes", "on"):
            return True
        if text in ("0", "false", "no", "off"):
            return False
        return default

    @staticmethod
    def _as_int(value: Any, default: int, minimum: Optional[int] = None, maximum: Optional[int] = None) -> int:
        try:
            out = int(value)
        except Exception:
            out = int(default)
        if minimum is not None:
            out = max(minimum, out)
        if maximum is not None:
            out = min(maximum, out)
        return out

    @staticmethod
    def _as_float(value: Any, default: float, minimum: Optional[float] = None, maximum: Optional[float] = None) -> float:
        try:
            out = float(value)
        except Exception:
            out = float(default)
        if minimum is not None:
            out = max(float(minimum), out)
        if maximum is not None:
            out = min(float(maximum), out)
        return out

    def _apply_runtime_config(self, cfg: dict) -> None:
        normalized, changed, warnings, errors = _normalize_router_config(cfg)
        for warning in warnings:
            LOGGER.warning("Runtime config apply: %s", warning)
        if errors:
            details = "; ".join(errors)
            raise ValueError(f"Runtime config apply failed: {details}")

        next_cfg = normalized
        next_targets = dict(next_cfg.get("targets", {}))
        for key, value in DEFAULT_TARGETS.items():
            next_targets.setdefault(key, value)
        next_cfg["targets"] = next_targets

        api_cfg = dict(next_cfg.get("api", {}))
        next_api_enabled = self._as_bool(api_cfg.get("enable"), True)
        next_api_host = str(api_cfg.get("host") or "127.0.0.1").strip() or "127.0.0.1"
        next_api_port = self._as_int(api_cfg.get("port"), 9071, minimum=1, maximum=65535)

        nkn_cfg = dict(next_cfg.get("nkn", {}))
        next_nkn_settings = {
            "enable": self._as_bool(nkn_cfg.get("enable"), True),
            "dm_retries": self._as_int(nkn_cfg.get("dm_retries"), 2, minimum=1, maximum=20),
            "resolve_timeout_seconds": self._as_int(nkn_cfg.get("resolve_timeout_seconds"), 20, minimum=2, maximum=120),
            "rpc_max_request_b": self._as_int(nkn_cfg.get("rpc_max_request_b"), 512 * 1024, minimum=1024),
            "rpc_max_response_b": self._as_int(nkn_cfg.get("rpc_max_response_b"), 2 * 1024 * 1024, minimum=1024),
        }

        cloudflared_cfg = dict(next_cfg.get("cloudflared", {}))
        next_cloudflared_enabled = self._as_bool(cloudflared_cfg.get("enable"), False)
        feature_flags_cfg = dict(next_cfg.get("feature_flags", {}))
        next_feature_flags = {
            "router_control_plane_api": self._as_bool(feature_flags_cfg.get("router_control_plane_api"), True),
            "resolver_auto_apply": self._as_bool(feature_flags_cfg.get("resolver_auto_apply"), True),
            "cloudflared_manager": self._as_bool(feature_flags_cfg.get("cloudflared_manager"), False),
        }
        next_cfg["feature_flags"] = next_feature_flags
        next_api_enabled = bool(next_api_enabled and next_feature_flags["router_control_plane_api"])
        next_cloudflared_enabled = bool(next_cloudflared_enabled and next_feature_flags["cloudflared_manager"])

        marketplace_cfg = dict(next_cfg.get("marketplace", {}))
        next_marketplace_cfg = {
            "enable_catalog": self._as_bool(marketplace_cfg.get("enable_catalog"), True),
            "provider_id": str(marketplace_cfg.get("provider_id") or "hydra-router").strip() or "hydra-router",
            "provider_label": str(marketplace_cfg.get("provider_label") or "Hydra Router").strip() or "Hydra Router",
            "provider_network": str(marketplace_cfg.get("provider_network") or "hydra").strip().lower() or "hydra",
            "provider_contact": str(marketplace_cfg.get("provider_contact") or "").strip(),
            "default_currency": str(marketplace_cfg.get("default_currency") or "USDC").strip().upper() or "USDC",
            "default_unit": str(marketplace_cfg.get("default_unit") or "request").strip().lower() or "request",
            "default_price_per_unit": self._as_float(
                marketplace_cfg.get("default_price_per_unit"),
                0.0,
                minimum=0.0,
                maximum=1_000_000.0,
            ),
            "include_unhealthy": self._as_bool(marketplace_cfg.get("include_unhealthy"), True),
            "catalog_ttl_seconds": self._as_int(marketplace_cfg.get("catalog_ttl_seconds"), 20, minimum=2, maximum=600),
        }
        next_cfg["marketplace"] = next_marketplace_cfg
        marketplace_sync_cfg = dict(next_cfg.get("marketplace_sync", {}))
        next_marketplace_sync_cfg = {
            "enable_auto_publish": self._as_bool(marketplace_sync_cfg.get("enable_auto_publish"), False),
            "target_urls": str(marketplace_sync_cfg.get("target_urls") or "").strip(),
            "auth_token": str(marketplace_sync_cfg.get("auth_token") or "").strip(),
            "auth_token_env": str(
                marketplace_sync_cfg.get("auth_token_env") or "HYDRA_MARKETPLACE_SYNC_TOKEN"
            ).strip() or "HYDRA_MARKETPLACE_SYNC_TOKEN",
            "auth_header": str(marketplace_sync_cfg.get("auth_header") or "Authorization").strip() or "Authorization",
            "auth_scheme": str(marketplace_sync_cfg.get("auth_scheme") or "Bearer").strip(),
            "publish_interval_seconds": self._as_int(
                marketplace_sync_cfg.get("publish_interval_seconds"),
                45,
                minimum=5,
                maximum=3600,
            ),
            "publish_timeout_seconds": self._as_float(
                marketplace_sync_cfg.get("publish_timeout_seconds"),
                6.0,
                minimum=1.0,
                maximum=120.0,
            ),
            "max_backoff_seconds": self._as_int(
                marketplace_sync_cfg.get("max_backoff_seconds"),
                300,
                minimum=5,
                maximum=7200,
            ),
            "include_unhealthy": self._as_bool(marketplace_sync_cfg.get("include_unhealthy"), True),
        }
        if any(ch in next_marketplace_sync_cfg["auth_header"] for ch in ("\r", "\n", ":")):
            next_marketplace_sync_cfg["auth_header"] = "Authorization"
            changed = True
        next_marketplace_sync_targets = _normalize_marketplace_sync_targets(next_marketplace_sync_cfg.get("target_urls"))
        normalized_targets_csv = ",".join(next_marketplace_sync_targets)
        if normalized_targets_csv != next_marketplace_sync_cfg["target_urls"]:
            next_marketplace_sync_cfg["target_urls"] = normalized_targets_csv
            changed = True
        next_cfg["marketplace_sync"] = next_marketplace_sync_cfg
        next_marketplace_sync_runtime = dict(next_marketplace_sync_cfg)
        next_marketplace_sync_runtime["target_urls_list"] = list(next_marketplace_sync_targets)
        service_publication_cfg = self._normalize_service_publication_map(
            next_cfg.get("service_publication"),
            marketplace_cfg=next_marketplace_cfg,
        )
        next_cfg["service_publication"] = service_publication_cfg

        auth_cfg = dict(next_cfg.get("auth", {}))
        next_auth_cfg = {
            "require_service_rpc_ticket": self._as_bool(auth_cfg.get("require_service_rpc_ticket"), True),
            "require_billing_preflight": self._as_bool(auth_cfg.get("require_billing_preflight"), True),
            "require_quote_for_billable": self._as_bool(auth_cfg.get("require_quote_for_billable"), True),
            "require_charge_for_billable": self._as_bool(auth_cfg.get("require_charge_for_billable"), True),
            "allow_unauthenticated_resolve": self._as_bool(auth_cfg.get("allow_unauthenticated_resolve"), True),
            "clock_skew_seconds": self._as_int(auth_cfg.get("clock_skew_seconds"), 30, minimum=0, maximum=300),
            "replay_cache_seconds": self._as_int(auth_cfg.get("replay_cache_seconds"), 900, minimum=30, maximum=3600),
            "ticket_ttl_seconds": self._as_int(auth_cfg.get("ticket_ttl_seconds"), 300, minimum=30, maximum=1800),
            "active_kid": str(auth_cfg.get("active_kid") or DEFAULT_TICKET_KID).strip() or DEFAULT_TICKET_KID,
            "default_scope": _normalize_scope(auth_cfg.get("default_scope"), default="infer"),
        }
        next_ticket_keys = _normalize_ticket_key_map(
            next_cfg.get("ticket_keys"),
            fallback=_normalize_ticket_key_map(self.ticket_keys or next_cfg.get("ticket_keys", {}), fallback={DEFAULT_TICKET_KID: DEFAULT_TICKET_SECRET}),
        )
        if next_auth_cfg["active_kid"] not in next_ticket_keys:
            first_kid = next(iter(next_ticket_keys.keys()), DEFAULT_TICKET_KID)
            next_auth_cfg["active_kid"] = first_kid
        next_ticket_kids = _normalize_ticket_kid_list(
            next_cfg.get("ticket_accepted_kids"),
            fallback=[next_auth_cfg["active_kid"], *list(next_ticket_keys.keys())],
        )
        next_ticket_kids = [kid for kid in next_ticket_kids if kid in next_ticket_keys]
        if next_auth_cfg["active_kid"] not in next_ticket_kids:
            next_ticket_kids.insert(0, next_auth_cfg["active_kid"])
        next_cfg["auth"] = next_auth_cfg
        next_cfg["ticket_keys"] = dict(next_ticket_keys)
        next_cfg["ticket_accepted_kids"] = list(next_ticket_kids)

        http_cfg = dict(next_cfg.get("http", {}))
        chunk_upload_b = self._as_int(http_cfg.get("chunk_upload_b"), 600 * 1024, minimum=4 * 1024)
        next_chunk_upload_kb = max(4, chunk_upload_b // 1024)

        # Atomic assignment of runtime settings after full validation.
        self.cfg = next_cfg
        self.targets = next_targets
        self.api_enabled = next_api_enabled
        self.api_host = next_api_host
        self.api_port = next_api_port
        self.feature_flags = next_feature_flags
        self.nkn_settings = next_nkn_settings
        self.cloudflared_enabled = next_cloudflared_enabled
        self.cloudflared_cfg = cloudflared_cfg
        self.auth_cfg = next_auth_cfg
        self.ticket_keys = dict(next_ticket_keys)
        self.ticket_accepted_kids = list(next_ticket_kids)
        self.marketplace_cfg = next_marketplace_cfg
        self.marketplace_sync_cfg = next_marketplace_sync_runtime
        self.service_publication_cfg = service_publication_cfg

        if hasattr(self, "marketplace_sync_state_lock") and hasattr(self, "marketplace_sync_state"):
            with self.marketplace_sync_state_lock:
                self.marketplace_sync_state["target_urls"] = list(next_marketplace_sync_targets)
                if self.marketplace_sync_cfg.get("enable_auto_publish") and next_marketplace_sync_targets:
                    if int(self.marketplace_sync_state.get("next_due_ts_ms") or 0) <= 0:
                        self.marketplace_sync_state["next_due_ts_ms"] = int(time.time() * 1000)
                else:
                    self.marketplace_sync_state["next_due_ts_ms"] = 0

        if getattr(self, "ui", None):
            self.ui.set_chunk_upload_kb(next_chunk_upload_kb)

        if changed:
            self.config_dirty = True

    @staticmethod
    def _is_loopback_host(host: str) -> bool:
        host = (host or "").strip().lower()
        return host in ("127.0.0.1", "localhost", "::1")

    @staticmethod
    def _canonical_router_service(hint: Any) -> str:
        text = str(hint or "").strip().lower()
        if not text:
            return ""
        if text in SERVICE_TARGETS:
            return text
        for svc_name, info in SERVICE_TARGETS.items():
            aliases = info.get("aliases", []) if isinstance(info, dict) else []
            for alias in aliases:
                if str(alias or "").strip().lower() == text:
                    return svc_name
        return text

    def _default_service_publication_entry(
        self,
        service_name: str,
        marketplace_cfg: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        defaults = _default_service_publication().get(service_name, {})
        if not isinstance(defaults, dict) or not defaults:
            info = SERVICE_TARGETS.get(service_name) if isinstance(SERVICE_TARGETS.get(service_name), dict) else {}
            target = str((info or {}).get("target") or service_name)
            defaults = {
                "enabled": True,
                "visibility": "public",
                "repository": "hydra",
                "category": target or service_name,
                "capacity_hint": 1,
                "tags": [service_name],
                "pricing": {
                    "currency": "USDC",
                    "unit": "request",
                    "base_price": 0.0,
                    "min_units": 1,
                    "quote_public": True,
                },
            }
        market = marketplace_cfg if isinstance(marketplace_cfg, dict) else (self.marketplace_cfg if isinstance(self.marketplace_cfg, dict) else {})
        pricing = defaults.get("pricing") if isinstance(defaults.get("pricing"), dict) else {}
        currency = str(market.get("default_currency") or pricing.get("currency") or "USDC").strip().upper() or "USDC"
        unit = str(market.get("default_unit") or pricing.get("unit") or "request").strip().lower() or "request"
        try:
            base_price = float(pricing.get("base_price") if pricing.get("base_price") is not None else market.get("default_price_per_unit") or 0.0)
        except Exception:
            base_price = 0.0
        base_price = max(0.0, base_price)
        tags = _normalize_marketplace_tags(defaults.get("tags"), fallback=[service_name])
        return {
            "enabled": bool(defaults.get("enabled", True)),
            "visibility": _normalize_marketplace_visibility(defaults.get("visibility"), default="public"),
            "repository": str(defaults.get("repository") or "hydra").strip() or "hydra",
            "category": str(defaults.get("category") or service_name).strip() or service_name,
            "capacity_hint": self._as_int(defaults.get("capacity_hint"), 1, minimum=0, maximum=100000),
            "tags": tags,
            "pricing": {
                "currency": currency,
                "unit": unit,
                "base_price": round(base_price, 8),
                "min_units": self._as_int(pricing.get("min_units"), 1, minimum=1, maximum=1000000),
                "quote_public": self._as_bool(pricing.get("quote_public"), True),
            },
        }

    def _normalize_service_publication_entry(
        self,
        service_name: str,
        raw_entry: Any,
        *,
        marketplace_cfg: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        base = self._default_service_publication_entry(service_name, marketplace_cfg=marketplace_cfg)
        if not isinstance(raw_entry, dict):
            return base
        out = dict(base)
        out["enabled"] = self._as_bool(raw_entry.get("enabled"), base.get("enabled", True))
        out["visibility"] = _normalize_marketplace_visibility(raw_entry.get("visibility"), default=str(base.get("visibility") or "public"))
        out["repository"] = str(raw_entry.get("repository") or base.get("repository") or "hydra").strip() or "hydra"
        out["category"] = str(raw_entry.get("category") or base.get("category") or service_name).strip() or service_name
        out["capacity_hint"] = self._as_int(raw_entry.get("capacity_hint"), int(base.get("capacity_hint") or 1), minimum=0, maximum=100000)
        out["tags"] = _normalize_marketplace_tags(raw_entry.get("tags"), fallback=base.get("tags") if isinstance(base.get("tags"), list) else [service_name])
        market = marketplace_cfg if isinstance(marketplace_cfg, dict) else (self.marketplace_cfg if isinstance(self.marketplace_cfg, dict) else {})
        default_pricing = base.get("pricing") if isinstance(base.get("pricing"), dict) else {}
        raw_pricing = raw_entry.get("pricing") if isinstance(raw_entry.get("pricing"), dict) else {}
        out["pricing"] = {
            "currency": str(raw_pricing.get("currency") or market.get("default_currency") or default_pricing.get("currency") or "USDC").strip().upper() or "USDC",
            "unit": str(raw_pricing.get("unit") or market.get("default_unit") or default_pricing.get("unit") or "request").strip().lower() or "request",
            "base_price": round(
                self._as_float(
                    raw_pricing.get("base_price"),
                    float(default_pricing.get("base_price") or market.get("default_price_per_unit") or 0.0),
                    minimum=0.0,
                    maximum=1_000_000.0,
                ),
                8,
            ),
            "min_units": self._as_int(raw_pricing.get("min_units"), int(default_pricing.get("min_units") or 1), minimum=1, maximum=1_000_000),
            "quote_public": self._as_bool(raw_pricing.get("quote_public"), self._as_bool(default_pricing.get("quote_public"), True)),
        }
        return out

    def _normalize_service_publication_map(
        self,
        raw_section: Any,
        *,
        marketplace_cfg: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Dict[str, Any]]:
        raw_map = raw_section if isinstance(raw_section, dict) else {}
        out: Dict[str, Dict[str, Any]] = {}
        for service_name in SERVICE_TARGETS.keys():
            out[service_name] = self._normalize_service_publication_entry(
                service_name,
                raw_map.get(service_name, {}),
                marketplace_cfg=marketplace_cfg,
            )
        return out

    @staticmethod
    def _deep_merge_obj(base: Any, overlay: Any) -> Any:
        if not isinstance(base, dict) or not isinstance(overlay, dict):
            return overlay
        out = dict(base)
        for key, value in overlay.items():
            if isinstance(value, dict) and isinstance(out.get(key), dict):
                out[key] = Router._deep_merge_obj(out.get(key), value)
            else:
                out[key] = value
        return out

    def _effective_service_publication(self, service_name: str) -> Dict[str, Any]:
        canonical = self._canonical_router_service(service_name) or service_name
        base = self.service_publication_cfg.get(canonical) if isinstance(self.service_publication_cfg.get(canonical), dict) else {}
        merged = dict(base) if isinstance(base, dict) else {}
        override = self.catalog_runtime_overrides.get(canonical) if isinstance(self.catalog_runtime_overrides.get(canonical), dict) else {}
        if override:
            merged = self._deep_merge_obj(merged, override)
        return self._normalize_service_publication_entry(canonical, merged, marketplace_cfg=self.marketplace_cfg)

    def _router_provider_fingerprint(self, addresses: Optional[List[str]] = None) -> str:
        raw_list = addresses if isinstance(addresses, list) else self._current_node_addresses()
        values = [str(item or "").strip() for item in raw_list if str(item or "").strip()]
        if not values:
            provider_id = str((self.marketplace_cfg or {}).get("provider_id") or "hydra-router")
            values = [provider_id]
        digest = hashlib.sha256("|".join(sorted(values)).encode("utf-8", errors="replace")).hexdigest()
        return digest[:16]

    def _router_network_urls(self) -> Dict[str, str]:
        host = self.api_host
        port = self.api_port
        local_base = f"http://127.0.0.1:{port}"
        urls = {"local": local_base}
        if host and host not in ("127.0.0.1", "localhost", "::1"):
            urls["bind"] = f"http://{host}:{port}"
        if host in ("0.0.0.0", "::", ""):
            lan_ip = self._detect_lan_ip()
            if lan_ip:
                urls["lan"] = f"http://{lan_ip}:{port}"
        return urls

    def _detect_lan_ip(self) -> str:
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.connect(("8.8.8.8", 80))
            ip = sock.getsockname()[0]
            sock.close()
            if ip and not ip.startswith("127."):
                return ip
        except Exception:
            pass
        return ""

    def _deepcopy_json(self, payload: Any) -> Any:
        try:
            return json.loads(json.dumps(payload))
        except Exception:
            return payload

    def _redact_public_payload(self, payload: Any) -> Any:
        copied = self._deepcopy_json(payload)
        return _redact_sensitive_fields(copied)

    def _probe_json(self, base_url: str, path: str, timeout_s: float = 2.5) -> Dict[str, Any]:
        started = time.time()
        full = base_url.rstrip("/") + path
        out: Dict[str, Any] = {
            "url": full,
            "path": path,
            "ok": False,
            "status": 0,
            "latency_ms": 0,
            "json": None,
            "error": "",
        }
        try:
            resp = requests.get(full, timeout=timeout_s)
            out["status"] = int(resp.status_code)
            out["latency_ms"] = round((time.time() - started) * 1000, 2)
            if resp.status_code < 400 or resp.status_code in (401, 403):
                try:
                    out["json"] = resp.json()
                    out["ok"] = True
                except Exception as exc:
                    if resp.status_code in (401, 403):
                        out["json"] = {"status": "unauthorized", "code": int(resp.status_code)}
                        out["ok"] = True
                    else:
                        out["error"] = f"json_parse: {exc}"
            else:
                out["error"] = f"http_{resp.status_code}"
        except Exception as exc:
            out["latency_ms"] = round((time.time() - started) * 1000, 2)
            out["error"] = f"{type(exc).__name__}: {exc}"
        return out

    def _service_target_base(self, service_name: str, info: Optional[Dict[str, Any]] = None) -> str:
        svc_info = info if isinstance(info, dict) else (SERVICE_TARGETS.get(service_name) or {})
        target_key = str(svc_info.get("target") or service_name)
        return (
            self.targets.get(target_key)
            or self.cfg.get("targets", {}).get(target_key)
            or svc_info.get("endpoint")
            or DEFAULT_TARGETS.get(target_key, "")
        )

    def _sync_cloudflared_tunnels(self) -> None:
        manager = self.cloudflared_manager
        if not manager:
            return
        for service_name, info in SERVICE_TARGETS.items():
            base_url = str(self._service_target_base(service_name, info) or "").strip()
            status = self.latest_service_status.get(service_name, {})
            running = bool(status.get("running"))
            if running and base_url:
                manager.set_service_target(service_name, base_url, enabled=True)
            else:
                manager.clear_service(service_name)

    def _cloudflared_tunnel_state(self, service_name: str) -> Dict[str, Any]:
        manager = self.cloudflared_manager
        if not manager:
            return {}
        return manager.get_state(service_name)

    def _default_fallback_payload(self, base_url: str, tunnel_url: str = "") -> Dict[str, Any]:
        selected = "cloudflare" if tunnel_url else "local"
        local_payload = {
            "state": "active" if base_url else "inactive",
            "base_url": base_url,
            "http_endpoint": base_url,
            "ws_endpoint": "",
        }
        cloudflare_payload = {
            "state": "active" if tunnel_url else "inactive",
            "public_base_url": tunnel_url or "",
            "http_endpoint": tunnel_url or "",
            "ws_endpoint": tunnel_url.replace("https://", "wss://") if tunnel_url else "",
            "error": "",
        }
        return {
            "selected_transport": selected,
            "order": ["cloudflare", "upnp", "nats", "nkn", "local"],
            "cloudflare": cloudflare_payload,
            "upnp": {"state": "inactive", "public_base_url": "", "http_endpoint": "", "ws_endpoint": "", "error": ""},
            "nats": {"state": "inactive", "broker_url": "", "subject": "", "error": ""},
            "nkn": {"state": "inactive", "nkn_address": "", "topic": "", "error": ""},
            "local": local_payload,
        }

    def _normalize_service_payload(
        self,
        service_name: str,
        target_key: str,
        base_url: str,
        probes: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Any]:
        router_info = probes.get("/router_info", {}).get("json")
        tunnel_info = probes.get("/tunnel_info", {}).get("json")
        health_payload = None
        for hp in ("/health", "/healthz", "/_health", "/api/v1/health"):
            payload = probes.get(hp, {}).get("json")
            if isinstance(payload, dict):
                health_payload = payload
                break
        out: Dict[str, Any] = {
            "status": "error",
            "service": target_key,
            "watchdog_service": service_name,
            "interop_contract": self._interop_contract_payload(),
            "interop_contract_version": str(self._interop_contract_payload().get("version") or ""),
            "interop_contract_compat_min_version": str(self._interop_contract_payload().get("compat_min_version") or ""),
            "interop_contract_namespace": str(self._interop_contract_payload().get("namespace") or ""),
            "transport": "local",
            "base_url": base_url,
            "http_endpoint": base_url,
            "ws_endpoint": "",
            "local": {
                "base_url": base_url,
                "health_url": f"{base_url.rstrip('/')}/health" if base_url else "",
                "listen_host": "127.0.0.1",
            },
            "tunnel": {
                "state": "inactive",
                "tunnel_url": "",
                "stale_tunnel_url": "",
                "error": "",
            },
            "fallback": self._default_fallback_payload(base_url),
            "security": {},
            "routes": {},
            "probe": probes,
        }

        cloudflared_state = self._cloudflared_tunnel_state(service_name)
        if isinstance(cloudflared_state, dict) and cloudflared_state:
            tunnel_url = str(cloudflared_state.get("active_url") or "")
            stale_tunnel_url = str(cloudflared_state.get("stale_url") or "")
            tunnel_err = str(cloudflared_state.get("last_error") or "")
            tunnel_state = str(cloudflared_state.get("state") or "inactive")
            tunnel_running = bool(cloudflared_state.get("running", False))
            out["tunnel"] = {
                "state": tunnel_state,
                "running": tunnel_running,
                "tunnel_url": tunnel_url,
                "stale_tunnel_url": stale_tunnel_url,
                "error": tunnel_err,
                "restarts": int(cloudflared_state.get("restarts") or 0),
                "rate_limited": bool(cloudflared_state.get("rate_limited", False)),
            }
            fallback = out.get("fallback") if isinstance(out.get("fallback"), dict) else self._default_fallback_payload(base_url)
            cloudflare_fallback = fallback.get("cloudflare") if isinstance(fallback.get("cloudflare"), dict) else {}
            cloudflare_fallback.update(
                {
                    "state": tunnel_state,
                    "public_base_url": tunnel_url,
                    "http_endpoint": tunnel_url,
                    "ws_endpoint": tunnel_url.replace("https://", "wss://") if tunnel_url else "",
                    "stale_tunnel_url": stale_tunnel_url,
                    "error": tunnel_err,
                    "restarts": int(cloudflared_state.get("restarts") or 0),
                    "rate_limited": bool(cloudflared_state.get("rate_limited", False)),
                }
            )
            fallback["cloudflare"] = cloudflare_fallback
            if tunnel_url:
                fallback["selected_transport"] = "cloudflare"
            out["fallback"] = fallback
            if tunnel_url:
                out["transport"] = "cloudflare"
                out["base_url"] = tunnel_url
                out["http_endpoint"] = tunnel_url
                out["ws_endpoint"] = tunnel_url.replace("https://", "wss://")

        if isinstance(router_info, dict):
            out["status"] = str(router_info.get("status") or "success")
            out["service"] = str(router_info.get("service") or target_key)
            out["transport"] = str(router_info.get("transport") or out["transport"])
            out["base_url"] = str(router_info.get("base_url") or out["base_url"])
            out["http_endpoint"] = str(router_info.get("http_endpoint") or out["http_endpoint"])
            out["ws_endpoint"] = str(router_info.get("ws_endpoint") or out["ws_endpoint"])
            if isinstance(router_info.get("local"), dict):
                out["local"] = router_info.get("local")
            if isinstance(router_info.get("tunnel"), dict):
                out["tunnel"] = router_info.get("tunnel")
            if isinstance(router_info.get("fallback"), dict):
                out["fallback"] = router_info.get("fallback")
            if isinstance(router_info.get("security"), dict):
                out["security"] = router_info.get("security")
            if isinstance(router_info.get("routes"), dict):
                out["routes"] = router_info.get("routes")

        if isinstance(tunnel_info, dict):
            tun = out.get("tunnel", {})
            if not isinstance(tun, dict):
                tun = {}
            tunnel_url = str(tunnel_info.get("tunnel_url") or tun.get("tunnel_url") or "")
            stale_tunnel_url = str(tunnel_info.get("stale_tunnel_url") or tun.get("stale_tunnel_url") or "")
            tunnel_err = str(tunnel_info.get("error") or tun.get("error") or "")
            running = bool(tunnel_info.get("running", False))
            state = "active" if (running and tunnel_url) else ("stale" if stale_tunnel_url else ("error" if tunnel_err else "inactive"))
            tun.update({
                "state": state,
                "tunnel_url": tunnel_url,
                "stale_tunnel_url": stale_tunnel_url,
                "error": tunnel_err,
            })
            out["tunnel"] = tun
            if isinstance(tunnel_info.get("fallback"), dict):
                out["fallback"] = tunnel_info.get("fallback")
            if tunnel_url and out.get("transport") in ("local", "", None):
                out["transport"] = "cloudflare"
                out["base_url"] = tunnel_url
                out["http_endpoint"] = tunnel_url

        if isinstance(health_payload, dict):
            status = health_payload.get("status")
            ok = health_payload.get("ok")
            if status:
                out["status"] = str(status)
            elif ok is True:
                out["status"] = "ok"
            out["health"] = health_payload
        elif any(p.get("ok") for p in probes.values()):
            out["status"] = "ok"

        fallback = out.get("fallback") if isinstance(out.get("fallback"), dict) else self._default_fallback_payload(base_url)
        fallback.setdefault("order", ["cloudflare", "upnp", "nats", "nkn", "local"])
        fallback.setdefault("cloudflare", {"state": "inactive", "public_base_url": "", "http_endpoint": "", "ws_endpoint": "", "error": ""})
        fallback.setdefault("upnp", {"state": "inactive", "public_base_url": "", "http_endpoint": "", "ws_endpoint": "", "error": ""})
        fallback.setdefault("nats", {"state": "inactive", "broker_url": "", "subject": "", "error": ""})
        fallback.setdefault("nkn", {"state": "inactive", "nkn_address": "", "topic": "", "error": ""})
        fallback.setdefault(
            "local",
            {
                "state": "active" if base_url else "inactive",
                "base_url": base_url,
                "http_endpoint": base_url,
                "ws_endpoint": "",
            },
        )

        selected = str(fallback.get("selected_transport") or "").strip().lower()
        if selected not in {"cloudflare", "upnp", "nats", "nkn", "local"}:
            selected = ""
        if not selected:
            cloudflare_url = str((fallback.get("cloudflare") or {}).get("public_base_url") or "")
            upnp_url = str((fallback.get("upnp") or {}).get("public_base_url") or "")
            nats_url = str((fallback.get("nats") or {}).get("public_base_url") or "")
            nkn_url = str((fallback.get("nkn") or {}).get("public_base_url") or "")
            if cloudflare_url:
                selected = "cloudflare"
            elif upnp_url:
                selected = "upnp"
            elif nats_url:
                selected = "nats"
            elif nkn_url:
                selected = "nkn"
            else:
                selected = "local"
            fallback["selected_transport"] = selected
        out["fallback"] = fallback
        if selected:
            out["transport"] = selected

        if not out.get("base_url"):
            tunnel_url = str((out.get("tunnel") or {}).get("tunnel_url") or "")
            if tunnel_url:
                out["base_url"] = tunnel_url
            else:
                local_base = str((out.get("local") or {}).get("base_url") or "")
                out["base_url"] = local_base or base_url
        if not out.get("http_endpoint"):
            out["http_endpoint"] = out.get("base_url") or base_url
        return out

    def _build_resolved_endpoints(self, services: Dict[str, dict]) -> Dict[str, dict]:
        resolved: Dict[str, dict] = {}
        for service in sorted(services.keys()):
            payload = services.get(service) or {}
            fallback = payload.get("fallback") if isinstance(payload.get("fallback"), dict) else {}
            local = payload.get("local") if isinstance(payload.get("local"), dict) else {}
            tunnel = payload.get("tunnel") if isinstance(payload.get("tunnel"), dict) else {}

            tunnel_url = str(tunnel.get("tunnel_url") or "")
            stale_tunnel = str(tunnel.get("stale_tunnel_url") or "")
            fallback_cloudflare = fallback.get("cloudflare") if isinstance(fallback.get("cloudflare"), dict) else {}
            fallback_upnp = fallback.get("upnp") if isinstance(fallback.get("upnp"), dict) else {}
            fallback_nats = fallback.get("nats") if isinstance(fallback.get("nats"), dict) else {}
            fallback_nkn = fallback.get("nkn") if isinstance(fallback.get("nkn"), dict) else {}
            fallback_local = fallback.get("local") if isinstance(fallback.get("local"), dict) else {}
            upnp_base = str((fallback.get("upnp") or {}).get("public_base_url") or "")
            nats_base = str((fallback_nats or {}).get("public_base_url") or "")
            nkn_base = str((fallback_nkn or {}).get("public_base_url") or "")
            nkn_address = str(
                (fallback_nkn or {}).get("nkn_address")
                or (fallback_nkn or {}).get("address")
                or (fallback_nkn or {}).get("target_address")
                or ""
            )
            local_base = str(local.get("base_url") or payload.get("base_url") or "")
            selected_transport = str(fallback.get("selected_transport") or payload.get("transport") or "").strip().lower()
            if selected_transport not in ("cloudflare", "upnp", "nats", "nkn", "local"):
                selected_transport = "local"

            selected_base = ""
            selected_reason = ""
            if selected_transport == "cloudflare" and tunnel_url:
                selected_base = tunnel_url
                selected_reason = "fallback.selected_transport=cloudflare with active tunnel"
            elif selected_transport == "upnp" and upnp_base:
                selected_base = upnp_base
                selected_reason = "fallback.selected_transport=upnp with active public_base_url"
            elif selected_transport == "nats" and nats_base:
                selected_base = nats_base
                selected_reason = "fallback.selected_transport=nats with active public_base_url"
            elif selected_transport == "nkn" and nkn_base:
                selected_base = nkn_base
                selected_reason = "fallback.selected_transport=nkn with active public_base_url"
            elif selected_transport == "local" and local_base:
                selected_base = local_base
                selected_reason = "fallback.selected_transport=local"

            if not selected_base:
                if tunnel_url:
                    selected_transport = "cloudflare"
                    selected_base = tunnel_url
                    selected_reason = "auto-selected cloudflare from tunnel_url"
                elif upnp_base:
                    selected_transport = "upnp"
                    selected_base = upnp_base
                    selected_reason = "auto-selected upnp from fallback"
                elif nats_base:
                    selected_transport = "nats"
                    selected_base = nats_base
                    selected_reason = "auto-selected nats from fallback"
                elif nkn_base:
                    selected_transport = "nkn"
                    selected_base = nkn_base
                    selected_reason = "auto-selected nkn from fallback"
                else:
                    selected_transport = "local"
                    selected_base = local_base
                    selected_reason = "defaulted to local base URL"

            candidates = {
                "cloudflare": tunnel_url or str((fallback_cloudflare or {}).get("public_base_url") or ""),
                "upnp": upnp_base,
                "nats": nats_base,
                "nkn": nkn_address or nkn_base,
                "local": local_base,
            }
            stale_cloudflare = bool(stale_tunnel) and not bool(tunnel_url)
            stale_rejected = False
            stale_reason = ""
            if selected_transport == "cloudflare" and stale_cloudflare:
                fallback_order = ["nkn", "upnp", "nats", "local"]
                for alt in fallback_order:
                    alt_base = str(candidates.get(alt) or "")
                    if alt_base:
                        selected_transport = alt
                        selected_base = alt_base
                        selected_reason = f"demoted stale cloudflare candidate to {alt}"
                        stale_rejected = True
                        stale_reason = "cloudflare_stale_tunnel_demoted"
                        break
            if isinstance(fallback, dict):
                fallback["selected_transport"] = selected_transport

            local_host = ""
            try:
                local_host = urllib.parse.urlparse(selected_base).hostname or ""
            except Exception:
                local_host = ""
            remote_routable = bool(selected_base) and not self._is_loopback_host(local_host)
            http_endpoint = str(payload.get("http_endpoint") or "")
            ws_endpoint = str(payload.get("ws_endpoint") or "")
            if selected_transport == "cloudflare" and selected_base:
                if not http_endpoint or self._is_loopback_host(urllib.parse.urlparse(http_endpoint).hostname or ""):
                    http_endpoint = selected_base
                if not ws_endpoint and str(selected_base).startswith("https://"):
                    ws_endpoint = selected_base.replace("https://", "wss://")
            if not http_endpoint:
                http_endpoint = selected_base

            resolved[service] = {
                "service": service,
                "interop_contract": self._interop_contract_payload(),
                "interop_contract_version": str(self._interop_contract_payload().get("version") or ""),
                "interop_contract_compat_min_version": str(self._interop_contract_payload().get("compat_min_version") or ""),
                "interop_contract_namespace": str(self._interop_contract_payload().get("namespace") or ""),
                "transport": selected_transport,
                "selected_transport": selected_transport,
                "selection_reason": selected_reason,
                "base_url": selected_base,
                "http_endpoint": http_endpoint,
                "ws_endpoint": ws_endpoint,
                "tunnel_url": tunnel_url,
                "stale_tunnel_url": stale_tunnel,
                "tunnel_error": str(tunnel.get("error") or ""),
                "candidates": candidates,
                "discovery_source": "router_snapshot",
                "stale_rejected": stale_rejected,
                "stale_reason": stale_reason,
                "selection_telemetry": {
                    "selected_transport": selected_transport,
                    "selection_reason": selected_reason,
                    "stale_rejected": stale_rejected,
                },
                "fallback": fallback,
                "local": local,
                "cloudflare": fallback_cloudflare,
                "upnp": fallback_upnp,
                "nats": fallback_nats,
                "nkn": fallback_nkn,
                "local_fallback": fallback_local,
                "security": payload.get("security") if isinstance(payload.get("security"), dict) else {},
                "routes": payload.get("routes") if isinstance(payload.get("routes"), dict) else {},
                "remote_routable": remote_routable,
                "loopback_only": bool(selected_base) and not remote_routable,
                "is_loopback": bool(selected_base) and self._is_loopback_host(local_host),
                "is_public": remote_routable,
            }
        return resolved

    def _collect_service_snapshot(self) -> Dict[str, Any]:
        services: Dict[str, dict] = {}
        ts_ms = int(time.time() * 1000)
        self._sync_cloudflared_tunnels()
        probe_paths = ["/router_info", "/tunnel_info", "/health", "/healthz", "/_health", "/api/v1/health"]
        for service_name, info in SERVICE_TARGETS.items():
            target_key = str(info.get("target") or service_name)
            base_url = (
                self.targets.get(target_key)
                or self.cfg.get("targets", {}).get(target_key)
                or info.get("endpoint")
                or DEFAULT_TARGETS.get(target_key, "")
            )
            probes: Dict[str, Dict[str, Any]] = {}
            if base_url:
                for path in probe_paths:
                    probes[path] = self._probe_json(base_url, path)
            normalized = self._normalize_service_payload(service_name, target_key, base_url, probes)
            healthy = any(p.get("ok") for p in probes.values())
            if healthy:
                self.last_good_service_payloads[target_key] = self._deepcopy_json(normalized)
                self.snapshot_failures[target_key] = 0
            else:
                self.snapshot_failures[target_key] = int(self.snapshot_failures.get(target_key, 0)) + 1
                if target_key in self.last_good_service_payloads:
                    stale_copy = self._deepcopy_json(self.last_good_service_payloads[target_key])
                    stale_copy["stale"] = True
                    stale_copy["stale_reason"] = "probe_failure"
                    stale_copy["probe"] = probes
                    stale_copy["status"] = stale_copy.get("status") or "degraded"
                    normalized = stale_copy
            services[target_key] = normalized
        resolved = self._build_resolved_endpoints(services)
        return {
            "ts_ms": ts_ms,
            "services": services,
            "resolved": resolved,
            "stale": False,
            "discovery_source": "local_snapshot",
            "interop_contract": self._interop_contract_payload(),
            "interop_contract_version": str(self._interop_contract_payload().get("version") or ""),
            "interop_contract_compat_min_version": str(self._interop_contract_payload().get("compat_min_version") or ""),
            "interop_contract_namespace": str(self._interop_contract_payload().get("namespace") or ""),
        }

    def get_service_snapshot(self, force_refresh: bool = False) -> Dict[str, Any]:
        now = time.time()
        with self.snapshot_lock:
            ts_ms = int(self.snapshot_cache.get("ts_ms") or 0)
            age_s = (now - (ts_ms / 1000.0)) if ts_ms else 9999.0
            if not force_refresh and ts_ms and age_s < 4.0:
                return self._deepcopy_json(self.snapshot_cache)
        snapshot = self._collect_service_snapshot()
        with self.snapshot_lock:
            self.snapshot_cache = snapshot
        return self._deepcopy_json(snapshot)

    def _append_activity_log(self, message: str, **meta: Any) -> None:
        entry = {
            "ts_ms": int(time.time() * 1000),
            "message": message,
        }
        safe_meta = _redact_sensitive_fields(meta or {})
        if isinstance(safe_meta, dict):
            entry.update(safe_meta)
        self.activity_log.append(entry)

    def _sample_telemetry(self) -> None:
        with self.telemetry_lock:
            history = self.telemetry_state.get("history")
            if isinstance(history, deque):
                history.append(
                    {
                        "ts_ms": int(time.time() * 1000),
                        "inbound_messages": int(self.telemetry_state.get("inbound_messages", 0)),
                        "outbound_messages": int(self.telemetry_state.get("outbound_messages", 0)),
                        "resolve_success_out": int(self.telemetry_state.get("resolve_success_out", 0)),
                        "resolve_fail_out": int(self.telemetry_state.get("resolve_fail_out", 0)),
                    }
                )

    @staticmethod
    def _payload_size_bytes(payload: Any) -> int:
        if payload is None:
            return 0
        if isinstance(payload, bytes):
            return len(payload)
        if isinstance(payload, str):
            return len(payload.encode("utf-8", errors="replace"))
        try:
            encoded = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
            return len(encoded.encode("utf-8", errors="replace"))
        except Exception:
            return len(str(payload).encode("utf-8", errors="replace"))

    def _ensure_peer_entry(self, peer_usage: Dict[str, Any], peer: str) -> Dict[str, Any]:
        key = str(peer or "(unknown)")
        entry = peer_usage.setdefault(
            key,
            {
                "peer": key,
                "inbound": 0,
                "outbound": 0,
                "inbound_messages": 0,
                "outbound_messages": 0,
                "inbound_bytes": 0,
                "outbound_bytes": 0,
                "events_in": {},
                "events_out": {},
                "endpoint_hits": {},
                "last_endpoints": {},
                "last_event": "",
                "last_ts_ms": 0,
            },
        )
        entry.setdefault("peer", key)
        entry.setdefault("events_in", {})
        entry.setdefault("events_out", {})
        entry.setdefault("endpoint_hits", {})
        entry.setdefault("last_endpoints", {})
        return entry

    def _record_nkn_traffic(
        self,
        direction: str,
        peer: str,
        payload: Any,
        event_name: str = "",
        endpoint_labels: Optional[List[str]] = None,
        count_message: bool = True,
    ) -> None:
        dir_norm = "in" if str(direction).strip().lower() in ("in", "inbound", "rx", "recv") else "out"
        size_b = self._payload_size_bytes(payload)
        now_ms = int(time.time() * 1000)
        event_norm = str(event_name or "").strip().lower()
        labels = [str(label) for label in (endpoint_labels or []) if str(label or "").strip()]

        with self.telemetry_lock:
            if count_message:
                if dir_norm == "in":
                    self.telemetry_state["inbound_messages"] = int(self.telemetry_state.get("inbound_messages", 0)) + 1
                    self.telemetry_state["inbound_bytes"] = int(self.telemetry_state.get("inbound_bytes", 0)) + size_b
                else:
                    self.telemetry_state["outbound_messages"] = int(self.telemetry_state.get("outbound_messages", 0)) + 1
                    self.telemetry_state["outbound_bytes"] = int(self.telemetry_state.get("outbound_bytes", 0)) + size_b

            peer_usage = self.telemetry_state.setdefault("peer_usage", {})
            entry = self._ensure_peer_entry(peer_usage, peer)
            entry["last_ts_ms"] = now_ms
            if event_norm:
                entry["last_event"] = event_norm
            if count_message:
                if dir_norm == "in":
                    entry["inbound"] = int(entry.get("inbound", 0)) + 1
                    entry["inbound_messages"] = int(entry.get("inbound_messages", 0)) + 1
                    entry["inbound_bytes"] = int(entry.get("inbound_bytes", 0)) + size_b
                    if event_norm:
                        events_in = entry.setdefault("events_in", {})
                        events_in[event_norm] = int(events_in.get(event_norm, 0)) + 1
                else:
                    entry["outbound"] = int(entry.get("outbound", 0)) + 1
                    entry["outbound_messages"] = int(entry.get("outbound_messages", 0)) + 1
                    entry["outbound_bytes"] = int(entry.get("outbound_bytes", 0)) + size_b
                    if event_norm:
                        events_out = entry.setdefault("events_out", {})
                        events_out[event_norm] = int(events_out.get(event_norm, 0)) + 1

            if labels:
                endpoint_hits = self.telemetry_state.setdefault("endpoint_hits", {})
                per_peer_hits = entry.setdefault("endpoint_hits", {})
                last_eps = entry.setdefault("last_endpoints", {})
                for label in labels:
                    endpoint_hits[label] = int(endpoint_hits.get(label, 0)) + 1
                    per_peer_hits[label] = int(per_peer_hits.get(label, 0)) + 1
                    last_eps[label] = label

            # Bound peer map growth in long-running routers.
            if len(peer_usage) > 1000:
                ranked = sorted(
                    peer_usage.items(),
                    key=lambda kv: int((kv[1] or {}).get("last_ts_ms", 0)),
                    reverse=True,
                )
                keep = dict(ranked[:800])
                peer_usage.clear()
                peer_usage.update(keep)

    def _record_inbound_nkn(self, source: str, payload: dict) -> None:
        event_name = ""
        if isinstance(payload, dict):
            event_name = str(payload.get("event") or "")
        self._record_nkn_traffic("in", source, payload, event_name=event_name)

    def _record_outbound_nkn(self, target: str, payload: dict) -> None:
        event_name = ""
        if isinstance(payload, dict):
            event_name = str(payload.get("event") or "")
        self._record_nkn_traffic("out", target, payload, event_name=event_name)

    def _on_node_nkn_traffic(self, direction: str, peer: str, payload: dict, event_name: str = "") -> None:
        self._record_nkn_traffic(direction, peer, payload, event_name=event_name)

    def _collect_endpoint_labels(self, resolved: Dict[str, dict]) -> List[str]:
        labels: List[str] = []
        for svc in sorted((resolved or {}).keys()):
            item = resolved.get(svc) or {}
            endpoint = str(item.get("base_url") or item.get("http_endpoint") or "")
            labels.append(f"{svc}:{endpoint}")
        return labels

    def _resolved_discovery_summary(self, resolved: Dict[str, dict]) -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "service_count": 0,
            "transport_counts": {},
            "transport_total": 0,
            "stale_rejections": [],
            "has_stale_rejections": False,
            "candidates": {},
            "candidate_coverage": {},
            "interop_contract_version": str(self._interop_contract_payload().get("version") or ""),
            "interop_contract_compat_min_version": str(self._interop_contract_payload().get("compat_min_version") or ""),
            "interop_contract_namespace": str(self._interop_contract_payload().get("namespace") or ""),
            "generated_at_ms": int(time.time() * 1000),
        }
        if not isinstance(resolved, dict):
            return out
        transport_counts: Dict[str, int] = {}
        stale_rejections: List[Dict[str, Any]] = []
        candidates: Dict[str, Dict[str, str]] = {}
        candidate_coverage: Dict[str, int] = {}
        service_count = 0
        for svc in sorted(resolved.keys()):
            item = resolved.get(svc) or {}
            if not isinstance(item, dict):
                continue
            service_count += 1
            selected = str(item.get("selected_transport") or item.get("transport") or "").strip().lower()
            if selected:
                transport_counts[selected] = int(transport_counts.get(selected, 0)) + 1
            raw_candidates = item.get("candidates") if isinstance(item.get("candidates"), dict) else {}
            candidates[svc] = {
                "cloudflare": str((raw_candidates or {}).get("cloudflare") or ""),
                "nkn": str((raw_candidates or {}).get("nkn") or ""),
                "local": str((raw_candidates or {}).get("local") or ""),
                "upnp": str((raw_candidates or {}).get("upnp") or ""),
                "nats": str((raw_candidates or {}).get("nats") or ""),
            }
            candidate_coverage[svc] = sum(1 for value in candidates[svc].values() if value)
            if bool(item.get("stale_rejected")):
                stale_rejections.append(
                    {
                        "service": svc,
                        "selected_transport": selected,
                        "reason": str(item.get("stale_reason") or "stale_candidate_rejected"),
                        "candidates": candidates[svc],
                    }
                )
        out["service_count"] = service_count
        out["transport_counts"] = transport_counts
        out["transport_total"] = sum(int(value) for value in transport_counts.values())
        out["stale_rejections"] = stale_rejections
        out["has_stale_rejections"] = bool(stale_rejections)
        out["stale_rejection_count"] = len(stale_rejections)
        out["candidates"] = candidates
        out["candidate_coverage"] = candidate_coverage
        return out

    def _record_endpoint_usage(self, peer: str, labels: List[str]) -> None:
        clean_labels = [str(label) for label in (labels or []) if str(label or "").strip()]
        if not clean_labels:
            return
        self._record_nkn_traffic("out", peer, {}, event_name="", endpoint_labels=clean_labels, count_message=False)

    def _record_resolve_outcome(self, ok: bool) -> None:
        with self.telemetry_lock:
            if ok:
                self.telemetry_state["resolve_success_out"] = int(self.telemetry_state.get("resolve_success_out", 0)) + 1
            else:
                self.telemetry_state["resolve_fail_out"] = int(self.telemetry_state.get("resolve_fail_out", 0)) + 1

    def _auth_capabilities_payload(self) -> Dict[str, Any]:
        auth = self.auth_cfg if isinstance(self.auth_cfg, dict) else {}
        keys = self.ticket_keys if isinstance(self.ticket_keys, dict) else {}
        accepted = self.ticket_accepted_kids if isinstance(self.ticket_accepted_kids, list) else []
        active_kid = str(auth.get("active_kid") or next(iter(keys.keys()), DEFAULT_TICKET_KID)).strip()
        accepted_kids = [str(kid) for kid in accepted if str(kid or "").strip() in keys]
        if active_kid and active_kid not in accepted_kids and active_kid in keys:
            accepted_kids.insert(0, active_kid)
        return {
            "require_service_rpc_ticket": bool(auth.get("require_service_rpc_ticket", True)),
            "require_billing_preflight": bool(auth.get("require_billing_preflight", True)),
            "require_quote_for_billable": bool(auth.get("require_quote_for_billable", True)),
            "require_charge_for_billable": bool(auth.get("require_charge_for_billable", True)),
            "allow_unauthenticated_resolve": bool(auth.get("allow_unauthenticated_resolve", True)),
            "algorithm": "HS256",
            "active_kid": active_kid,
            "accepted_kids": accepted_kids,
            "ticket_ttl_seconds": int(auth.get("ticket_ttl_seconds") or 300),
            "clock_skew_seconds": int(auth.get("clock_skew_seconds") or 30),
            "replay_cache_seconds": int(auth.get("replay_cache_seconds") or 900),
            "default_scope": _normalize_scope(auth.get("default_scope"), default="infer"),
            "allowed_scopes": sorted(AUTH_ALLOWED_SCOPES),
            "allowed_audience": sorted(AUTH_ALLOWED_AUDIENCE),
            "error_codes": [
                "ticket_missing",
                "ticket_expired",
                "ticket_not_yet_valid",
                "signature_invalid",
                "scope_denied",
                "audience_denied",
                "ticket_replay",
                "quote_missing",
                "credit_reservation_missing",
                "insufficient_credit_reservation",
            ],
            "billable_preflight_fields": ["quote_id", "charge_id|max_charge_micros|settled_micros"],
        }

    @staticmethod
    def _token_fingerprint(token: Any) -> str:
        text = str(token or "").strip()
        if not text:
            return ""
        digest = hashlib.sha256(text.encode("utf-8", errors="replace")).hexdigest()
        return digest[:16]

    @staticmethod
    def _audience_rank(value: Any) -> int:
        audience = _normalize_audience(value, default="public")
        if audience == "private":
            return 0
        if audience == "friends":
            return 1
        return 2

    def _sweep_ticket_replay_cache_locked(self, now: Optional[float] = None) -> None:
        ts = float(now if now is not None else time.time())
        stale_keys = [key for key, expires_at in self.ticket_replay_cache.items() if float(expires_at or 0.0) <= ts]
        for key in stale_keys:
            self.ticket_replay_cache.pop(key, None)
        if len(self.ticket_replay_cache) > 20000:
            ranked = sorted(self.ticket_replay_cache.items(), key=lambda kv: float(kv[1] or 0.0), reverse=True)
            self.ticket_replay_cache = dict(ranked[:12000])

    def _check_and_mark_ticket_replay(
        self,
        claims: Dict[str, Any],
        source: str,
        exp_ts: float,
    ) -> Tuple[bool, str]:
        jti = str(claims.get("jti") or "").strip()
        nonce = str(claims.get("nonce") or "").strip()
        if not jti and not nonce:
            return True, ""
        ttl = int(self.auth_cfg.get("replay_cache_seconds") or 900)
        now = float(time.time())
        expires_at = max(now + 30.0, min(exp_ts + float(self.auth_cfg.get("clock_skew_seconds") or 30), now + float(ttl)))
        source_key = str(source or "").strip()
        keys = []
        if jti:
            keys.append(f"jti:{jti}")
        if nonce:
            keys.append(f"nonce:{nonce}:{source_key}")
        with self.ticket_replay_lock:
            self._sweep_ticket_replay_cache_locked(now=now)
            for replay_key in keys:
                existing = float(self.ticket_replay_cache.get(replay_key) or 0.0)
                if existing > now:
                    return False, replay_key
            for replay_key in keys:
                self.ticket_replay_cache[replay_key] = expires_at
        return True, ""

    def _verify_access_ticket(
        self,
        token: str,
        *,
        required_scope: str,
        required_audience: str,
        source: str,
        service: str,
    ) -> Dict[str, Any]:
        token_text = str(token or "").strip()
        token_fp = self._token_fingerprint(token_text)
        if not token_text:
            return {"ok": False, "error_code": "ticket_missing", "error": "access ticket required", "ticket_fingerprint": token_fp}

        parts = token_text.split(".")
        if len(parts) != 3:
            return {"ok": False, "error_code": "signature_invalid", "error": "malformed ticket", "ticket_fingerprint": token_fp}
        header_b64, payload_b64, sig_b64 = parts

        try:
            header = json.loads(_base64url_decode(header_b64).decode("utf-8", errors="replace"))
            claims = json.loads(_base64url_decode(payload_b64).decode("utf-8", errors="replace"))
        except Exception:
            return {"ok": False, "error_code": "signature_invalid", "error": "invalid ticket encoding", "ticket_fingerprint": token_fp}
        if not isinstance(header, dict) or not isinstance(claims, dict):
            return {"ok": False, "error_code": "signature_invalid", "error": "invalid ticket envelope", "ticket_fingerprint": token_fp}

        kid_hint = str(header.get("kid") or claims.get("kid") or "").strip()
        keys = self.ticket_keys if isinstance(self.ticket_keys, dict) else {}
        accepted = [str(k) for k in (self.ticket_accepted_kids or []) if str(k or "").strip()]
        if not accepted:
            accepted = list(keys.keys())
        candidate_kids: List[str] = []
        if kid_hint:
            candidate_kids = [kid_hint]
        else:
            candidate_kids = list(accepted)

        signing_input = f"{header_b64}.{payload_b64}".encode("ascii", errors="ignore")
        provided_sig = _base64url_decode(sig_b64)
        verified_kid = ""
        for kid in candidate_kids:
            if kid not in keys:
                continue
            if kid not in accepted:
                continue
            secret = str(keys.get(kid) or "")
            if not secret:
                continue
            expected_sig = hmac.new(secret.encode("utf-8", errors="replace"), signing_input, hashlib.sha256).digest()
            if hmac.compare_digest(provided_sig, expected_sig):
                verified_kid = kid
                break
        if not verified_kid:
            return {
                "ok": False,
                "error_code": "signature_invalid",
                "error": "ticket signature validation failed",
                "ticket_fingerprint": token_fp,
                "kid": kid_hint,
            }

        now = int(time.time())
        skew = int(self.auth_cfg.get("clock_skew_seconds") or 30)
        exp = int(claims.get("exp") or 0)
        iat = int(claims.get("iat") or 0)
        nbf = int(claims.get("nbf") or iat or 0)
        if exp <= 0 or (now - skew) >= exp:
            return {"ok": False, "error_code": "ticket_expired", "error": "ticket expired", "ticket_fingerprint": token_fp, "kid": verified_kid}
        if nbf and (now + skew) < nbf:
            return {
                "ok": False,
                "error_code": "ticket_not_yet_valid",
                "error": "ticket not yet valid",
                "ticket_fingerprint": token_fp,
                "kid": verified_kid,
            }

        typ = str(claims.get("typ") or "").strip().lower()
        if typ and typ not in {"hydra_access_ticket", "hydra_access", "p2p", "p2p_ticket"}:
            return {"ok": False, "error_code": "signature_invalid", "error": "invalid ticket type", "ticket_fingerprint": token_fp, "kid": verified_kid}

        normalized_service = self._canonical_router_service(service or "")
        service_claim = str(
            claims.get("service_id")
            or claims.get("serviceId")
            or claims.get("service")
            or ""
        ).strip().lower()
        if service_claim and service_claim not in {"*", normalized_service}:
            return {"ok": False, "error_code": "scope_denied", "error": "service denied by ticket", "ticket_fingerprint": token_fp, "kid": verified_kid}

        req_scope = _normalize_scope(required_scope or self.auth_cfg.get("default_scope"), default="infer")
        scopes = set()
        claim_scope = _normalize_scope(claims.get("scope"), default="")
        if claim_scope:
            scopes.add(claim_scope)
        raw_scopes = claims.get("scopes")
        if isinstance(raw_scopes, list):
            for raw_scope in raw_scopes:
                normalized = _normalize_scope(raw_scope, default="")
                if normalized:
                    scopes.add(normalized)
        if req_scope and req_scope not in scopes:
            return {"ok": False, "error_code": "scope_denied", "error": "scope denied", "ticket_fingerprint": token_fp, "kid": verified_kid}

        ticket_audience = _normalize_audience(claims.get("audience"), default="public")
        req_audience = _normalize_audience(required_audience, default="public")
        if self._audience_rank(req_audience) > self._audience_rank(ticket_audience):
            return {"ok": False, "error_code": "audience_denied", "error": "audience denied", "ticket_fingerprint": token_fp, "kid": verified_kid}

        consumer_nkn = str(
            claims.get("consumer_nkn")
            or claims.get("consumerNkn")
            or claims.get("source_nkn")
            or claims.get("sourceNkn")
            or ""
        ).strip()
        if consumer_nkn and ticket_audience == "private" and str(source or "").strip() and consumer_nkn != str(source or "").strip():
            return {
                "ok": False,
                "error_code": "audience_denied",
                "error": "private ticket source mismatch",
                "ticket_fingerprint": token_fp,
                "kid": verified_kid,
            }

        replay_ok, replay_key = self._check_and_mark_ticket_replay(claims, source=source, exp_ts=float(exp))
        if not replay_ok:
            return {
                "ok": False,
                "error_code": "ticket_replay",
                "error": "ticket replay detected",
                "ticket_fingerprint": token_fp,
                "kid": verified_kid,
                "replay_key": replay_key,
            }

        return {
            "ok": True,
            "claims": claims,
            "kid": verified_kid,
            "scope": req_scope,
            "audience": ticket_audience,
            "ticket_fingerprint": token_fp,
        }

    def _authorize_service_rpc(self, source: str, body: Dict[str, Any], service: str) -> Dict[str, Any]:
        payload = body if isinstance(body, dict) else {}
        auth_block = payload.get("auth") if isinstance(payload.get("auth"), dict) else {}
        ticket = str(
            payload.get("access_ticket")
            or payload.get("ticket")
            or auth_block.get("ticket")
            or auth_block.get("access_ticket")
            or ""
        ).strip()
        required_scope = _normalize_scope(
            payload.get("scope")
            or auth_block.get("scope")
            or self.auth_cfg.get("default_scope")
            or "infer",
            default="infer",
        )
        required_audience = _normalize_audience(
            payload.get("audience")
            or auth_block.get("audience")
            or "public",
            default="public",
        )
        require_ticket = bool(self.auth_cfg.get("require_service_rpc_ticket", True))
        if not require_ticket and not ticket:
            return {
                "ok": True,
                "claims": {},
                "kid": "",
                "scope": required_scope,
                "audience": required_audience,
                "ticket_fingerprint": "",
                "unauthenticated": True,
            }
        return self._verify_access_ticket(
            ticket,
            required_scope=required_scope,
            required_audience=required_audience,
            source=source,
            service=service,
        )

    def _pick_send_node(self) -> Optional[RelayNode]:
        for node in self.nodes:
            if node.current_address:
                return node
        return self.nodes[0] if self.nodes else None

    def send_nkn_dm(self, target: str, payload: dict, tries: int = 1) -> Tuple[bool, str]:
        target = (target or "").strip()
        if not target:
            return False, "missing target"
        attempts = max(1, int(tries or 1))
        last_err = ""
        for _ in range(attempts):
            node = self._pick_send_node()
            if not node:
                last_err = "no active relay nodes"
                time.sleep(0.1)
                continue
            try:
                node.bridge.dm(target, payload, DM_OPTS_SINGLE)
                self._record_outbound_nkn(target, payload)
                return True, ""
            except Exception as exc:
                last_err = str(exc)
                time.sleep(0.1)
        return False, last_err or "send failed"

    def _create_pending_resolve(self, target_address: str) -> dict:
        request_id = f"resolve-{int(time.time() * 1000)}-{uuid.uuid4().hex[:8]}"
        pending = {
            "request_id": request_id,
            "target_address": target_address,
            "created_at": time.time(),
            "event": threading.Event(),
            "response": None,
        }
        with self.pending_resolves_lock:
            self.pending_resolves[request_id] = pending
        return pending

    def _pop_pending_resolve(self, request_id: str) -> Optional[dict]:
        with self.pending_resolves_lock:
            return self.pending_resolves.pop(request_id, None)

    def _sweep_pending_resolves_loop(self) -> None:
        while not self.resolve_sweeper_stop.is_set():
            now = time.time()
            stale: List[str] = []
            with self.pending_resolves_lock:
                for req_id, pending in self.pending_resolves.items():
                    created = float(pending.get("created_at") or now)
                    if now - created > 120.0:
                        stale.append(req_id)
                for req_id in stale:
                    self.pending_resolves.pop(req_id, None)
            self.resolve_sweeper_stop.wait(2.0)

    def _handle_resolve_tunnels_request(self, source: str, body: dict, node: RelayNode) -> None:
        self._record_inbound_nkn(source, body)
        request_id = str(body.get("request_id") or "")
        if not self._as_bool((self.auth_cfg or {}).get("allow_unauthenticated_resolve"), True):
            token = str(
                body.get("access_ticket")
                or body.get("ticket")
                or ((body.get("auth") or {}).get("ticket") if isinstance(body.get("auth"), dict) else "")
                or ""
            ).strip()
            authz = self._verify_access_ticket(
                token,
                required_scope=_normalize_scope((self.auth_cfg or {}).get("default_scope"), default="infer"),
                required_audience="public",
                source=source,
                service="*",
            )
            if not authz.get("ok"):
                reply = {
                    "event": "resolve_tunnels_result",
                    "interop_contract": self._interop_contract_payload(),
                    "interop_contract_version": str(self._interop_contract_payload().get("version") or ""),
                    "interop_contract_compat_min_version": str(self._interop_contract_payload().get("compat_min_version") or ""),
                    "interop_contract_namespace": str(self._interop_contract_payload().get("namespace") or ""),
                    "auth_capabilities": self._auth_capabilities_payload(),
                    "request_id": request_id,
                    "source_address": node.current_address or "",
                    "timestamp_ms": int(time.time() * 1000),
                    "status": "error",
                    "error": str(authz.get("error") or "resolve authorization denied"),
                    "error_code": str(authz.get("error_code") or "signature_invalid"),
                }
                node.bridge.dm(source, reply, DM_OPTS_SINGLE)
                self._record_outbound_nkn(source, reply)
                return
        with self.telemetry_lock:
            self.telemetry_state["resolve_requests_in"] = int(self.telemetry_state.get("resolve_requests_in", 0)) + 1
        snapshot = self.get_service_snapshot(force_refresh=True)
        public_snapshot = self._redact_public_payload(snapshot)
        resolved_payload = public_snapshot.get("resolved", {}) if isinstance(public_snapshot, dict) else {}
        resolve_summary = self._resolved_discovery_summary(resolved_payload if isinstance(resolved_payload, dict) else {})
        catalog_payload = self._marketplace_catalog_payload(public_snapshot if isinstance(public_snapshot, dict) else {})
        reply = {
            "event": "resolve_tunnels_result",
            "interop_contract": self._interop_contract_payload(),
            "interop_contract_version": str(self._interop_contract_payload().get("version") or ""),
            "interop_contract_compat_min_version": str(self._interop_contract_payload().get("compat_min_version") or ""),
            "interop_contract_namespace": str(self._interop_contract_payload().get("namespace") or ""),
            "auth_capabilities": self._auth_capabilities_payload(),
            "request_id": request_id,
            "source_address": node.current_address or "",
            "timestamp_ms": int(time.time() * 1000),
            "discovery_source": "nkn_dm",
            "snapshot": public_snapshot,
            "resolved": resolved_payload if isinstance(resolved_payload, dict) else {},
            "resolve_summary": resolve_summary,
            "catalog": catalog_payload,
        }
        node.bridge.dm(source, reply, DM_OPTS_SINGLE)
        self._record_outbound_nkn(source, reply)
        endpoint_labels = self._collect_endpoint_labels(
            resolved_payload if isinstance(resolved_payload, dict) else {}
        )
        self._record_endpoint_usage(source, endpoint_labels)

    def _handle_resolve_tunnels_result(self, source: str, body: dict) -> None:
        self._record_inbound_nkn(source, body)
        request_id = str(body.get("request_id") or "")
        if not request_id:
            return
        contract_status = self._interop_contract_status_from_payload(body)
        with self.pending_resolves_lock:
            pending = self.pending_resolves.get(request_id)
            if not pending:
                return
            payload = body if isinstance(body, dict) else {}
            if not contract_status.get("ok"):
                expected = contract_status.get("expected", {})
                incoming = contract_status.get("incoming", {})
                mismatch = (
                    "interop contract mismatch: "
                    f"expected {expected.get('name', '?')}@{expected.get('version', '?')} "
                    f"(compat>={expected.get('compat_min_version', '?')}) "
                    f"got {incoming.get('name', '?')}@{incoming.get('version', '?')} "
                    f"(compat>={incoming.get('compat_min_version', '?')})"
                )
                payload = dict(payload)
                payload["status"] = payload.get("status") or "error"
                payload["error"] = payload.get("error") or mismatch
                payload["error_code"] = payload.get("error_code") or "UNSUPPORTED_CONTRACT_VERSION"
                payload["contract_ok"] = False
                payload["expected_contract"] = expected
                payload["incoming_contract"] = incoming
            pending["response"] = {"source": source, "payload": payload}
            pending["event"].set()

    def _normalize_rpc_metering(self, body: Dict[str, Any], service: str) -> Dict[str, Any]:
        payload = body if isinstance(body, dict) else {}
        raw_metering = payload.get("metering") if isinstance(payload.get("metering"), dict) else {}
        request_b = int(self._payload_size_bytes(payload.get("json"))) if payload.get("json") is not None else 0
        if payload.get("body_b64") is not None:
            try:
                request_b = max(request_b, len(base64.b64decode(str(payload.get("body_b64")), validate=False)))
            except Exception:
                request_b = max(request_b, 0)
        unit = str(
            raw_metering.get("billable_unit")
            or raw_metering.get("unit")
            or raw_metering.get("usage_unit")
            or ""
        ).strip().lower()
        if not unit:
            svc = str(service or "").strip().lower()
            if svc in {"whisper_asr", "asr"}:
                unit = "audio_second"
            elif svc in {"piper_tts", "tts"}:
                unit = "text_char"
            elif svc in {"llm", "ollama_llm"}:
                unit = "token"
            elif svc in {"depth_any", "pointcloud"}:
                unit = "image"
            else:
                unit = "request"

        requested_units = self._as_float(
            raw_metering.get("requested_units"),
            self._as_float(payload.get("requested_units"), 1.0, minimum=0.000001),
            minimum=0.000001,
        )
        min_units = self._as_float(raw_metering.get("min_units"), 1.0, minimum=0.000001)
        price_per_unit_micros = self._as_int(
            raw_metering.get("price_per_unit_micros"),
            self._as_int(payload.get("price_per_unit_micros"), 0, minimum=0),
            minimum=0,
        )
        max_charge_micros = self._as_int(
            raw_metering.get("max_charge_micros"),
            self._as_int(payload.get("max_charge_micros"), 0, minimum=0),
            minimum=0,
        )
        settled_micros = self._as_int(
            raw_metering.get("settled_micros"),
            self._as_int(payload.get("settled_micros"), 0, minimum=0),
            minimum=0,
        )
        correlation_id = str(
            raw_metering.get("correlation_id")
            or payload.get("correlation_id")
            or payload.get("request_id")
            or ""
        ).strip()
        quote_id = str(
            raw_metering.get("quote_id")
            or payload.get("quote_id")
            or ""
        ).strip()
        charge_id = str(
            raw_metering.get("charge_id")
            or payload.get("charge_id")
            or ""
        ).strip()
        usage_label = str(
            raw_metering.get("usage_label")
            or payload.get("usage_label")
            or ""
        ).strip().lower()
        transport_tag = str(
            raw_metering.get("transport_tag")
            or payload.get("transport_tag")
            or ""
        ).strip().lower()
        reservation_id = str(
            raw_metering.get("reservation_id")
            or raw_metering.get("credit_reservation_id")
            or payload.get("reservation_id")
            or payload.get("credit_reservation_id")
            or ""
        ).strip()
        return {
            "unit": unit,
            "requested_units": requested_units,
            "min_units": min_units,
            "price_per_unit_micros": price_per_unit_micros,
            "max_charge_micros": max_charge_micros,
            "settled_micros": settled_micros,
            "quote_id": quote_id,
            "charge_id": charge_id,
            "correlation_id": correlation_id,
            "usage_label": usage_label,
            "transport_tag": transport_tag,
            "reservation_id": reservation_id,
            "request_bytes": max(0, request_b),
            "extra": _json_clone(raw_metering) if isinstance(raw_metering, dict) else {},
        }

    def _evaluate_rpc_execution_gate(
        self,
        source: str,
        service: str,
        metering: Dict[str, Any],
        authz: Dict[str, Any],
    ) -> Dict[str, Any]:
        canonical_service = self._canonical_router_service(service) or str(service or "").strip().lower()
        publication = self._effective_service_publication(canonical_service) if canonical_service else {}
        pricing = publication.get("pricing") if isinstance(publication.get("pricing"), dict) else {}
        configured_unit = str(pricing.get("unit") or metering.get("unit") or "request").strip().lower() or "request"
        configured_price = self._as_float(pricing.get("base_price"), 0.0, minimum=0.0)
        configured_price_micros = int(max(0, round(float(configured_price) * 1_000_000.0)))
        requested_units = float(self._as_float(metering.get("requested_units"), 1.0, minimum=0.000001))
        min_units = float(self._as_float(metering.get("min_units"), 1.0, minimum=0.000001))
        requested_units = max(requested_units, min_units)
        requested_price_micros = int(self._as_int(metering.get("price_per_unit_micros"), 0, minimum=0))
        effective_price_micros = requested_price_micros if requested_price_micros > 0 else configured_price_micros
        max_charge_micros = int(self._as_int(metering.get("max_charge_micros"), 0, minimum=0))
        settled_micros = int(self._as_int(metering.get("settled_micros"), 0, minimum=0))
        quote_id = str(metering.get("quote_id") or "").strip()
        charge_id = str(metering.get("charge_id") or "").strip()
        reservation_id = str(metering.get("reservation_id") or "").strip()
        reservation_micros = settled_micros if settled_micros > 0 else max_charge_micros
        estimated_preflight_micros = int(max(0, round(float(requested_units) * float(effective_price_micros))))
        enforce_preflight = bool(self.auth_cfg.get("require_billing_preflight", True))
        billable = (
            effective_price_micros > 0
            or max_charge_micros > 0
            or settled_micros > 0
            or bool(quote_id)
            or bool(charge_id)
            or bool(reservation_id)
        )

        result: Dict[str, Any] = {
            "ok": True,
            "service": canonical_service,
            "billable": bool(billable),
            "enforced": bool(enforce_preflight and billable),
            "requirements": {
                "ticket": bool(authz.get("ok")),
                "quote": bool(quote_id),
                "charge": bool(charge_id or reservation_id or reservation_micros > 0),
            },
            "pricing": {
                "unit": configured_unit,
                "requested_units": float(requested_units),
                "configured_price_per_unit_micros": int(configured_price_micros),
                "effective_price_per_unit_micros": int(effective_price_micros),
                "estimated_preflight_micros": int(estimated_preflight_micros),
                "max_charge_micros": int(max_charge_micros),
                "settled_micros": int(settled_micros),
            },
            "correlation_id": str(metering.get("correlation_id") or ""),
            "quote_id": quote_id,
            "charge_id": charge_id,
            "reservation_id": reservation_id,
            "error_code": "",
            "error": "",
        }

        if enforce_preflight and billable:
            require_quote = bool(self.auth_cfg.get("require_quote_for_billable", True))
            require_charge = bool(self.auth_cfg.get("require_charge_for_billable", True))
            ticket_ok = bool(authz.get("ok"))
            ticket_fingerprint = str(authz.get("ticket_fingerprint") or "")
            if not ticket_ok or not ticket_fingerprint:
                result["ok"] = False
                result["error_code"] = str(authz.get("error_code") or "ticket_missing")
                result["error"] = str(authz.get("error") or "billable rpc requires a valid access ticket")
            elif require_quote and not quote_id:
                result["ok"] = False
                result["error_code"] = "quote_missing"
                result["error"] = "billable rpc requires quote_id preflight"
            elif require_charge and not (charge_id or reservation_id or reservation_micros > 0):
                result["ok"] = False
                result["error_code"] = "credit_reservation_missing"
                result["error"] = "billable rpc requires a credit reservation"
            elif require_charge and reservation_micros > 0 and estimated_preflight_micros > reservation_micros:
                result["ok"] = False
                result["error_code"] = "insufficient_credit_reservation"
                result["error"] = (
                    f"estimated charge {estimated_preflight_micros} exceeds reservation {reservation_micros}"
                )

        with self.telemetry_lock:
            rpc_enforcement = self.telemetry_state.setdefault("rpc_enforcement", {})
            rpc_enforcement["checks"] = int(rpc_enforcement.get("checks", 0)) + 1
            if result.get("ok"):
                rpc_enforcement["allow"] = int(rpc_enforcement.get("allow", 0)) + 1
            else:
                rpc_enforcement["deny"] = int(rpc_enforcement.get("deny", 0)) + 1
                denied_codes_global = rpc_enforcement.setdefault("denied_codes", {})
                code_key = str(result.get("error_code") or "denied")
                denied_codes_global[code_key] = int(denied_codes_global.get(code_key, 0)) + 1

            by_service = rpc_enforcement.setdefault("by_service", {})
            service_key = canonical_service or "(unknown)"
            service_bucket = by_service.setdefault(
                service_key,
                {"checks": 0, "allow": 0, "deny": 0, "denied_codes": {}},
            )
            service_bucket["checks"] = int(service_bucket.get("checks", 0)) + 1
            if result.get("ok"):
                service_bucket["allow"] = int(service_bucket.get("allow", 0)) + 1
            else:
                service_bucket["deny"] = int(service_bucket.get("deny", 0)) + 1
                denied_codes = service_bucket.setdefault("denied_codes", {})
                code_key = str(result.get("error_code") or "denied")
                denied_codes[code_key] = int(denied_codes.get(code_key, 0)) + 1
            rpc_enforcement["last_event"] = {
                "ts_ms": int(time.time() * 1000),
                "source": str(source or ""),
                "service": service_key,
                "ok": bool(result.get("ok")),
                "billable": bool(result.get("billable")),
                "error_code": str(result.get("error_code") or ""),
                "correlation_id": str(result.get("correlation_id") or ""),
                "quote_id": str(result.get("quote_id") or ""),
                "charge_id": str(result.get("charge_id") or ""),
            }

        if not result.get("ok"):
            self._append_activity_log(
                "service_rpc_preflight_denied",
                source=source,
                service=canonical_service,
                error_code=result.get("error_code"),
                error=result.get("error"),
                billable=billable,
                quote_id=quote_id,
                charge_id=charge_id,
                correlation_id=result.get("correlation_id"),
            )

        return result

    def _estimate_rpc_units(
        self,
        req: Dict[str, Any],
        result: Dict[str, Any],
        metering: Dict[str, Any],
        duration_ms: float,
    ) -> float:
        unit = str(metering.get("unit") or "request").strip().lower()
        requested_units = float(self._as_float(metering.get("requested_units"), 1.0, minimum=0.000001))
        min_units = float(self._as_float(metering.get("min_units"), 1.0, minimum=0.000001))
        safe_requested = max(min_units, requested_units)
        if unit in {"request", "operation", "ops"}:
            return max(min_units, 1.0)
        if unit in {"audio_second", "seconds", "sec"}:
            src = None
            if isinstance(req.get("json"), dict):
                src = req.get("json")
            if isinstance(result.get("json"), dict):
                src = result.get("json")
            seconds = self._as_float((src or {}).get("duration_seconds") if isinstance(src, dict) else None, 0.0, minimum=0.0)
            if seconds > 0.0:
                return max(min_units, float(seconds))
            return max(min_units, safe_requested)
        if unit in {"text_char", "char", "chars"}:
            text_len = 0
            if isinstance(req.get("json"), dict):
                payload = req.get("json") or {}
                text_candidate = str(payload.get("text") or payload.get("prompt") or "")
                text_len = len(text_candidate)
            if text_len > 0:
                return max(min_units, float(text_len))
            return max(min_units, safe_requested)
        if unit in {"token", "tokens"}:
            usage = result.get("json") if isinstance(result.get("json"), dict) else {}
            usage_meta = usage.get("usage") if isinstance(usage, dict) and isinstance(usage.get("usage"), dict) else {}
            total_tokens = self._as_int(
                usage_meta.get("total_tokens"),
                self._as_int(usage.get("total_tokens") if isinstance(usage, dict) else None, 0, minimum=0),
                minimum=0,
            )
            if total_tokens > 0:
                return max(min_units, float(total_tokens))
            return max(min_units, safe_requested)
        if unit in {"mb", "megabyte", "megabytes"}:
            body_b64 = result.get("body_b64")
            response_b = 0
            if body_b64:
                try:
                    response_b = len(base64.b64decode(str(body_b64), validate=False))
                except Exception:
                    response_b = 0
            if response_b <= 0:
                response_b = int(self._payload_size_bytes(result.get("json")))
            if response_b > 0:
                return max(min_units, float(response_b) / float(1024 * 1024))
            return max(min_units, safe_requested)
        if unit in {"frame", "frames"}:
            frames = self._as_int(metering.get("frames"), 0, minimum=0)
            if frames > 0:
                return max(min_units, float(frames))
            return max(min_units, safe_requested)
        if unit in {"duration_ms"}:
            if duration_ms > 0:
                return max(min_units, float(duration_ms))
            return max(min_units, safe_requested)
        return max(min_units, safe_requested)

    def _record_rpc_metering(
        self,
        source: str,
        req: Dict[str, Any],
        result: Dict[str, Any],
        metering: Dict[str, Any],
        duration_ms: float,
    ) -> Dict[str, Any]:
        service = str(req.get("service") or "").strip()
        ok = bool(result.get("ok"))
        units = self._estimate_rpc_units(req, result, metering, duration_ms)
        price_per_unit_micros = int(self._as_int(metering.get("price_per_unit_micros"), 0, minimum=0))
        estimated_micros = int(max(0, round(units * float(price_per_unit_micros))))
        max_charge_micros = int(self._as_int(metering.get("max_charge_micros"), 0, minimum=0))
        if max_charge_micros > 0:
            estimated_micros = min(estimated_micros, max_charge_micros)
        settled_micros = int(self._as_int(metering.get("settled_micros"), 0, minimum=0))
        if settled_micros > 0 and max_charge_micros > 0:
            settled_micros = min(settled_micros, max_charge_micros)
        if settled_micros < 0:
            settled_micros = 0

        event_payload = {
            "ts_ms": int(time.time() * 1000),
            "source": str(source or ""),
            "service": service,
            "path": str(req.get("path") or ""),
            "method": str(req.get("method") or "").upper(),
            "ok": ok,
            "status": int(result.get("status") or 0),
            "duration_ms": int(max(0.0, duration_ms)),
            "unit": str(metering.get("unit") or "request"),
            "requested_units": float(self._as_float(metering.get("requested_units"), 1.0, minimum=0.000001)),
            "estimated_units": float(units),
            "price_per_unit_micros": int(price_per_unit_micros),
            "estimated_micros": int(estimated_micros),
            "settled_micros": int(settled_micros),
            "quote_id": str(metering.get("quote_id") or ""),
            "charge_id": str(metering.get("charge_id") or ""),
            "reservation_id": str(metering.get("reservation_id") or ""),
            "correlation_id": str(metering.get("correlation_id") or ""),
            "usage_label": str(metering.get("usage_label") or ""),
            "transport_tag": str(metering.get("transport_tag") or ""),
        }
        with self.telemetry_lock:
            rpc_metering = self.telemetry_state.setdefault("rpc_metering", {})
            rpc_metering["requests"] = int(rpc_metering.get("requests", 0)) + 1
            if ok:
                rpc_metering["success"] = int(rpc_metering.get("success", 0)) + 1
            else:
                rpc_metering["failure"] = int(rpc_metering.get("failure", 0)) + 1
            rpc_metering["estimated_units_total"] = float(rpc_metering.get("estimated_units_total", 0.0)) + float(units)
            rpc_metering["estimated_micros_total"] = int(rpc_metering.get("estimated_micros_total", 0)) + int(estimated_micros)
            rpc_metering["settled_micros_total"] = int(rpc_metering.get("settled_micros_total", 0)) + int(settled_micros)
            by_service = rpc_metering.setdefault("by_service", {})
            svc_entry = by_service.setdefault(
                service or "(unknown)",
                {
                    "requests": 0,
                    "success": 0,
                    "failure": 0,
                    "estimated_units_total": 0.0,
                    "estimated_micros_total": 0,
                    "settled_micros_total": 0,
                    "last_ts_ms": 0,
                },
            )
            svc_entry["requests"] = int(svc_entry.get("requests", 0)) + 1
            if ok:
                svc_entry["success"] = int(svc_entry.get("success", 0)) + 1
            else:
                svc_entry["failure"] = int(svc_entry.get("failure", 0)) + 1
            svc_entry["estimated_units_total"] = float(svc_entry.get("estimated_units_total", 0.0)) + float(units)
            svc_entry["estimated_micros_total"] = int(svc_entry.get("estimated_micros_total", 0)) + int(estimated_micros)
            svc_entry["settled_micros_total"] = int(svc_entry.get("settled_micros_total", 0)) + int(settled_micros)
            svc_entry["last_ts_ms"] = int(event_payload["ts_ms"])
            svc_entry["last_event"] = dict(event_payload)

            events = rpc_metering.get("events")
            if not isinstance(events, deque):
                events = deque(maxlen=300)
                rpc_metering["events"] = events
            events.append(dict(event_payload))

        return event_payload

    def _build_service_rpc_request(self, body: dict) -> dict:
        service = self._canonical_router_service(body.get("service"))
        if not service:
            raise ValueError("missing service")
        if service not in SERVICE_TARGETS:
            raise ValueError(f"unknown service '{service}'")

        path = str(body.get("path") or "/").strip() or "/"
        if not path.startswith("/"):
            path = "/" + path
        if len(path) > 1024:
            raise ValueError("path too long")
        if "\x00" in path:
            raise ValueError("path contains invalid bytes")
        norm_parts = [segment for segment in path.split("/") if segment]
        if any(segment == ".." for segment in norm_parts):
            raise ValueError("path traversal is not allowed")

        method = str(body.get("method") or "GET").strip().upper()
        allowed_methods = {"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"}
        if method not in allowed_methods:
            raise ValueError(f"unsupported method '{method}'")

        headers_in = body.get("headers") if isinstance(body.get("headers"), dict) else {}
        headers: Dict[str, str] = {}
        for key, value in headers_in.items():
            k = str(key or "").strip()
            if not k or len(k) > 128:
                continue
            if "\r" in k or "\n" in k:
                continue
            v = str(value or "")
            if len(v) > 4096:
                v = v[:4096]
            if "\r" in v or "\n" in v:
                continue
            headers[k] = v
            if len(headers) >= 64:
                break

        timeout_ms = self._as_int(body.get("timeout_ms"), 30000, minimum=1000, maximum=300000)
        max_request_b = int(self.nkn_settings.get("rpc_max_request_b") or (512 * 1024))

        req: Dict[str, Any] = {
            "service": service,
            "path": path,
            "method": method,
            "headers": headers,
            "timeout_ms": timeout_ms,
        }
        if body.get("json") is not None:
            try:
                json_bytes = len(json.dumps(body.get("json"), ensure_ascii=False).encode("utf-8"))
            except Exception:
                raise ValueError("invalid json payload")
            if json_bytes > max_request_b:
                raise ValueError(f"rpc json payload too large ({json_bytes} > {max_request_b})")
            req["json"] = body.get("json")
        if body.get("body_b64") is not None:
            try:
                raw = base64.b64decode(str(body.get("body_b64")), validate=False)
            except Exception:
                raise ValueError("invalid body_b64 payload")
            if len(raw) > max_request_b:
                raise ValueError(f"rpc body payload too large ({len(raw)} > {max_request_b})")
            req["body_b64"] = body.get("body_b64")
        metering = self._normalize_rpc_metering(body if isinstance(body, dict) else {}, service)
        req["metering"] = metering
        req["correlation_id"] = str(metering.get("correlation_id") or "")
        req["quote_id"] = str(metering.get("quote_id") or "")
        req["charge_id"] = str(metering.get("charge_id") or "")
        return req

    def _execute_service_rpc(self, req: dict) -> dict:
        relay_node = self._pick_send_node()
        if not relay_node:
            return {"ok": False, "status": 503, "error": "no relay nodes available", "headers": {}, "json": None, "body_b64": None}
        url = relay_node._resolve_url(req)
        method = str(req.get("method") or "GET").upper()
        headers = req.get("headers") if isinstance(req.get("headers"), dict) else {}
        timeout_s = float(self._as_int(req.get("timeout_ms"), 30000, minimum=1000, maximum=300000)) / 1000.0
        params: Dict[str, Any] = {"headers": headers, "timeout": timeout_s, "stream": True}
        if req.get("json") is not None:
            params["json"] = req.get("json")
        elif req.get("body_b64") is not None:
            try:
                params["data"] = base64.b64decode(str(req.get("body_b64")), validate=False)
            except Exception:
                params["data"] = b""
        max_response_b = int(self.nkn_settings.get("rpc_max_response_b") or (2 * 1024 * 1024))
        try:
            with requests.request(method, url, **params) as resp:
                body = bytearray()
                for chunk in resp.iter_content(chunk_size=64 * 1024):
                    if not chunk:
                        continue
                    body.extend(chunk)
                    if len(body) > max_response_b:
                        return {
                            "ok": False,
                            "status": 413,
                            "headers": {k.lower(): v for k, v in resp.headers.items()},
                            "json": None,
                            "body_b64": None,
                            "error": f"rpc response too large ({len(body)} > {max_response_b})",
                        }
                payload: Dict[str, Any] = {
                    "ok": resp.status_code < 400,
                    "status": int(resp.status_code),
                    "headers": {k.lower(): v for k, v in resp.headers.items()},
                    "json": None,
                    "body_b64": None,
                    "error": None,
                }
                ctype = (resp.headers.get("Content-Type") or "").lower()
                if "application/json" in ctype:
                    try:
                        payload["json"] = json.loads(bytes(body).decode("utf-8", errors="replace"))
                    except Exception:
                        payload["body_b64"] = base64.b64encode(bytes(body)).decode("ascii")
                else:
                    payload["body_b64"] = base64.b64encode(bytes(body)).decode("ascii")
                return payload
        except Exception as exc:
            return {"ok": False, "status": 0, "headers": {}, "json": None, "body_b64": None, "error": f"{type(exc).__name__}: {exc}"}

    def _handle_service_rpc_request(self, source: str, body: dict, node: RelayNode) -> None:
        self._record_inbound_nkn(source, body)
        with self.telemetry_lock:
            self.telemetry_state["rpc_requests_in"] = int(self.telemetry_state.get("rpc_requests_in", 0)) + 1
        request_id = str(body.get("request_id") or "")
        service_hint = self._canonical_router_service((body or {}).get("service")) if isinstance(body, dict) else ""
        authz = self._authorize_service_rpc(source, body if isinstance(body, dict) else {}, service_hint)
        rpc_start_ts = time.time()
        metering_event: Dict[str, Any] = {}
        enforcement: Dict[str, Any] = {}
        try:
            if not authz.get("ok"):
                denied_metering = self._normalize_rpc_metering(body if isinstance(body, dict) else {}, service_hint or "")
                req = {
                    "service": service_hint,
                    "path": str((body or {}).get("path") or "/"),
                    "method": str((body or {}).get("method") or "GET").upper(),
                    "metering": denied_metering,
                }
                enforcement = self._evaluate_rpc_execution_gate(source, service_hint or "", denied_metering, authz)
                denial_code = str(authz.get("error_code") or "signature_invalid")
                denial_error = str(authz.get("error") or "access denied")
                denial_status = 403
                if enforcement.get("billable") and not enforcement.get("ok"):
                    denial_code = str(enforcement.get("error_code") or denial_code)
                    denial_error = str(enforcement.get("error") or denial_error)
                    if denial_code in {"quote_missing", "credit_reservation_missing", "insufficient_credit_reservation"}:
                        denial_status = 402
                result = {
                    "ok": False,
                    "status": denial_status,
                    "headers": {},
                    "json": None,
                    "body_b64": None,
                    "error": denial_error,
                    "error_code": denial_code,
                    "auth": {
                        "authorized": False,
                        "kid": str(authz.get("kid") or ""),
                        "scope": str(authz.get("scope") or ""),
                        "audience": str(authz.get("audience") or ""),
                        "ticket_fingerprint": str(authz.get("ticket_fingerprint") or ""),
                    },
                }
            else:
                req = self._build_service_rpc_request(body)
                req_metering = req.get("metering") if isinstance(req.get("metering"), dict) else {}
                enforcement = self._evaluate_rpc_execution_gate(
                    source,
                    str(req.get("service") or service_hint or ""),
                    req_metering,
                    authz,
                )
                if not enforcement.get("ok"):
                    denial_code = str(enforcement.get("error_code") or "signature_invalid")
                    denial_status = 402 if denial_code in {"quote_missing", "credit_reservation_missing", "insufficient_credit_reservation"} else 403
                    result = {
                        "ok": False,
                        "status": denial_status,
                        "headers": {},
                        "json": None,
                        "body_b64": None,
                        "error": str(enforcement.get("error") or "rpc preflight denied"),
                        "error_code": denial_code,
                    }
                else:
                    result = self._execute_service_rpc(req)
                result["auth"] = {
                    "authorized": bool(enforcement.get("ok", True)),
                    "kid": str(authz.get("kid") or ""),
                    "scope": str(authz.get("scope") or ""),
                    "audience": str(authz.get("audience") or ""),
                    "ticket_fingerprint": str(authz.get("ticket_fingerprint") or ""),
                    "unauthenticated": bool(authz.get("unauthenticated")),
                }
        except Exception as exc:
            rejected_metering = self._normalize_rpc_metering(body if isinstance(body, dict) else {}, service_hint or "")
            req = {
                "service": self._canonical_router_service(body.get("service")),
                "path": str(body.get("path") or "/"),
                "method": str(body.get("method") or "GET").upper(),
                "metering": rejected_metering,
            }
            enforcement = self._evaluate_rpc_execution_gate(
                source,
                str(req.get("service") or service_hint or ""),
                rejected_metering,
                authz,
            )
            result = {
                "ok": False,
                "status": 400,
                "headers": {},
                "json": None,
                "body_b64": None,
                "error": f"{type(exc).__name__}: {exc}",
                "error_code": "invalid_request",
            }
        duration_ms = float(max(0.0, (time.time() - rpc_start_ts) * 1000.0))
        metering_payload = req.get("metering") if isinstance(req.get("metering"), dict) else self._normalize_rpc_metering(body if isinstance(body, dict) else {}, service_hint or "")
        metering_event = self._record_rpc_metering(source, req if isinstance(req, dict) else {}, result if isinstance(result, dict) else {}, metering_payload, duration_ms)
        if isinstance(result, dict):
            result["metering"] = {
                "unit": str(metering_event.get("unit") or metering_payload.get("unit") or ""),
                "requested_units": float(metering_event.get("requested_units") or metering_payload.get("requested_units") or 1.0),
                "estimated_units": float(metering_event.get("estimated_units") or 0.0),
                "price_per_unit_micros": int(metering_event.get("price_per_unit_micros") or metering_payload.get("price_per_unit_micros") or 0),
                "estimated_micros": int(metering_event.get("estimated_micros") or 0),
                "settled_micros": int(metering_event.get("settled_micros") or metering_payload.get("settled_micros") or 0),
                "quote_id": str(metering_event.get("quote_id") or metering_payload.get("quote_id") or ""),
                "charge_id": str(metering_event.get("charge_id") or metering_payload.get("charge_id") or ""),
                "reservation_id": str(metering_event.get("reservation_id") or metering_payload.get("reservation_id") or ""),
                "correlation_id": str(metering_event.get("correlation_id") or metering_payload.get("correlation_id") or ""),
                "usage_label": str(metering_event.get("usage_label") or metering_payload.get("usage_label") or ""),
                "transport_tag": str(metering_event.get("transport_tag") or metering_payload.get("transport_tag") or ""),
                "duration_ms": int(max(0.0, duration_ms)),
                "request_bytes": int(metering_payload.get("request_bytes") or 0),
            }
            result["enforcement"] = {
                "ok": bool(enforcement.get("ok", True)),
                "billable": bool(enforcement.get("billable")),
                "enforced": bool(enforcement.get("enforced")),
                "requirements": enforcement.get("requirements") if isinstance(enforcement.get("requirements"), dict) else {},
                "pricing": enforcement.get("pricing") if isinstance(enforcement.get("pricing"), dict) else {},
                "error_code": str(enforcement.get("error_code") or ""),
                "error": str(enforcement.get("error") or ""),
            }
        reply = {
            "event": "service_rpc_result",
            "interop_contract": self._interop_contract_payload(),
            "interop_contract_version": str(self._interop_contract_payload().get("version") or ""),
            "interop_contract_compat_min_version": str(self._interop_contract_payload().get("compat_min_version") or ""),
            "interop_contract_namespace": str(self._interop_contract_payload().get("namespace") or ""),
            "auth_capabilities": self._auth_capabilities_payload(),
            "request_id": request_id,
            "source_address": node.current_address or "",
            "timestamp_ms": int(time.time() * 1000),
            "service": req.get("service"),
            "path": req.get("path"),
            "duration_ms": int(max(0.0, duration_ms)),
            "correlation_id": str(metering_event.get("correlation_id") or metering_payload.get("correlation_id") or ""),
            "quote_id": str(metering_event.get("quote_id") or metering_payload.get("quote_id") or ""),
            "charge_id": str(metering_event.get("charge_id") or metering_payload.get("charge_id") or ""),
            "reservation_id": str(metering_event.get("reservation_id") or metering_payload.get("reservation_id") or ""),
        }
        reply.update(result)
        node.bridge.dm(source, reply, DM_OPTS_SINGLE)
        self._record_outbound_nkn(source, reply)
        with self.telemetry_lock:
            self.telemetry_state["rpc_requests_out"] = int(self.telemetry_state.get("rpc_requests_out", 0)) + 1
            if result.get("ok"):
                self.telemetry_state["rpc_success_out"] = int(self.telemetry_state.get("rpc_success_out", 0)) + 1
            else:
                self.telemetry_state["rpc_fail_out"] = int(self.telemetry_state.get("rpc_fail_out", 0)) + 1

    def _handle_service_rpc_result(self, source: str, body: dict) -> None:
        self._record_inbound_nkn(source, body)

    def _handle_router_nkn_event(self, event: str, source: str, body: dict, node: RelayNode) -> bool:
        ev = (event or "").strip().lower()
        if ev == "resolve_tunnels":
            self._handle_resolve_tunnels_request(source, body, node)
            return True
        if ev == "resolve_tunnels_result":
            self._handle_resolve_tunnels_result(source, body)
            return True
        if ev == "service_rpc_request":
            self._handle_service_rpc_request(source, body, node)
            return True
        if ev == "service_rpc_result":
            self._handle_service_rpc_result(source, body)
            return True
        return False

    def _index_payload(self) -> Dict[str, Any]:
        network_urls = self._router_network_urls()
        return {
            "status": "ok",
            "service": "hydra_router",
            "interop_contract": self._interop_contract_payload(),
            "interop_contract_version": str(self._interop_contract_payload().get("version") or ""),
            "interop_contract_compat_min_version": str(self._interop_contract_payload().get("compat_min_version") or ""),
            "interop_contract_namespace": str(self._interop_contract_payload().get("namespace") or ""),
            "auth_capabilities": self._auth_capabilities_payload(),
            "network": network_urls,
            "feature_flags": dict(getattr(self, "feature_flags", {}) or {}),
            "routes": {
                "api": "/api",
                "health": "/health",
                "services": "/services/snapshot",
                "nkn_info": "/nkn/info",
                "nkn_resolve": "/nkn/resolve",
                "marketplace_catalog": "/marketplace/catalog",
                "marketplace_catalog_overrides": "/marketplace/catalog/overrides",
                "marketplace_sync": "/marketplace/sync",
                "marketplace_publish": "/marketplace/catalog/publish",
                "dashboard_data": "/dashboard/data",
            },
        }

    def _current_node_addresses(self) -> List[str]:
        return sorted({node.current_address for node in self.nodes if node.current_address})

    def _interop_contract_payload(self) -> Dict[str, str]:
        payload = dict(INTEROP_CONTRACT)
        payload["name"] = str(payload.get("name") or "hydra_noclip_interop")
        payload["version"] = str(payload.get("version") or "1.0.0")
        payload["compat_min_version"] = str(payload.get("compat_min_version") or payload.get("version") or "1.0.0")
        payload["namespace"] = str(payload.get("namespace") or "hydra.noclip.marketplace.v1")
        payload["schema"] = str(payload.get("schema") or "hydra_noclip_marketplace_contract_v1")
        return payload

    def _marketplace_provider_payload(self, snapshot: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        addresses = self._current_node_addresses()
        market = self.marketplace_cfg if isinstance(self.marketplace_cfg, dict) else {}
        router_nkn = addresses[0] if addresses else ""
        network_urls = self._router_network_urls()
        return {
            "provider_id": str(market.get("provider_id") or "hydra-router"),
            "provider_label": str(market.get("provider_label") or "Hydra Router"),
            "provider_network": str(market.get("provider_network") or "hydra"),
            "provider_contact": str(market.get("provider_contact") or ""),
            "router_nkn": router_nkn,
            "router_nkn_addresses": addresses,
            "provider_key_fingerprint": self._router_provider_fingerprint(addresses),
            "network_urls": network_urls,
            "api_base_url": str(network_urls.get("local") or ""),
            "relay_count": len(addresses),
            "snapshot_ts_ms": int((snapshot or {}).get("ts_ms") or 0),
        }

    def _catalog_candidate_map(
        self,
        resolved_entry: Dict[str, Any],
        fallback: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, str]:
        raw_candidates = resolved_entry.get("candidates") if isinstance(resolved_entry.get("candidates"), dict) else {}
        fallback_map = fallback if isinstance(fallback, dict) else {}
        out = {
            "cloudflare": str((raw_candidates or {}).get("cloudflare") or str(fallback_map.get("cloudflare") or "")),
            "nkn": str((raw_candidates or {}).get("nkn") or str(fallback_map.get("nkn") or "")),
            "local": str((raw_candidates or {}).get("local") or str(fallback_map.get("local") or "")),
            "upnp": str((raw_candidates or {}).get("upnp") or str(fallback_map.get("upnp") or "")),
            "nats": str((raw_candidates or {}).get("nats") or str(fallback_map.get("nats") or "")),
        }
        for key, value in list(out.items()):
            out[key] = str(value or "").strip()
        return out

    def _catalog_service_is_healthy(self, service_status: Dict[str, Any], service_payload: Dict[str, Any]) -> bool:
        status_text = str(service_payload.get("status") or "").strip().lower()
        running = bool(service_status.get("running"))
        if status_text in {"ok", "healthy", "running", "success"}:
            return True
        if service_payload.get("health") and isinstance(service_payload.get("health"), dict):
            health = service_payload.get("health") if isinstance(service_payload.get("health"), dict) else {}
            if health.get("ok") is True:
                return True
            if str(health.get("status") or "").strip().lower() in {"ok", "healthy", "running", "success"}:
                return True
        return running

    def _catalog_runtime_overrides_payload(self) -> Dict[str, Any]:
        keys = sorted(self.catalog_runtime_overrides.keys()) if isinstance(self.catalog_runtime_overrides, dict) else []
        return {
            "active": bool(keys),
            "service_count": len(keys),
            "services": keys,
        }

    def _apply_catalog_overrides(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        body = payload if isinstance(payload, dict) else {}
        raw_services = body.get("services") if isinstance(body.get("services"), dict) else {}
        if not raw_services:
            if self._as_bool(body.get("clear"), False):
                self.catalog_runtime_overrides = {}
            return self._catalog_runtime_overrides_payload()
        next_overrides = dict(self.catalog_runtime_overrides)
        for raw_service, raw_patch in raw_services.items():
            service = self._canonical_router_service(raw_service)
            if not service or service not in SERVICE_TARGETS:
                continue
            if raw_patch is None:
                next_overrides.pop(service, None)
                continue
            base = self._effective_service_publication(service)
            merged = self._deep_merge_obj(base, raw_patch if isinstance(raw_patch, dict) else {})
            normalized = self._normalize_service_publication_entry(service, merged, marketplace_cfg=self.marketplace_cfg)
            next_overrides[service] = normalized
        self.catalog_runtime_overrides = next_overrides
        return self._catalog_runtime_overrides_payload()

    def _marketplace_catalog_payload(
        self,
        snapshot: Dict[str, Any],
        *,
        include_unhealthy: Optional[bool] = None,
    ) -> Dict[str, Any]:
        safe_snapshot = snapshot if isinstance(snapshot, dict) else {}
        services_map = safe_snapshot.get("services") if isinstance(safe_snapshot.get("services"), dict) else {}
        resolved_map = safe_snapshot.get("resolved") if isinstance(safe_snapshot.get("resolved"), dict) else {}
        resolve_summary = self._resolved_discovery_summary(resolved_map)
        market = self.marketplace_cfg if isinstance(self.marketplace_cfg, dict) else {}
        provider = self._marketplace_provider_payload(safe_snapshot)
        include_all = bool(market.get("include_unhealthy", True)) if include_unhealthy is None else bool(include_unhealthy)
        services: List[Dict[str, Any]] = []
        healthy_count = 0
        published_count = 0
        healthy_with_candidates = 0

        for service_name in sorted(SERVICE_TARGETS.keys()):
            service_payload = services_map.get(service_name) if isinstance(services_map.get(service_name), dict) else {}
            resolved_entry = resolved_map.get(service_name) if isinstance(resolved_map.get(service_name), dict) else {}
            if not service_payload and not resolved_entry:
                continue

            publication = self._effective_service_publication(service_name)
            service_status = self.latest_service_status.get(service_name) if isinstance(self.latest_service_status.get(service_name), dict) else {}
            healthy = self._catalog_service_is_healthy(service_status, service_payload)
            if not include_all and not healthy:
                continue

            candidates = self._catalog_candidate_map(resolved_entry)
            selected_transport = str(
                resolved_entry.get("selected_transport")
                or resolved_entry.get("transport")
                or service_payload.get("transport")
                or "local"
            ).strip().lower() or "local"
            selected_endpoint = str(
                resolved_entry.get("base_url")
                or resolved_entry.get("http_endpoint")
                or service_payload.get("base_url")
                or ""
            ).strip()
            if not selected_endpoint:
                selected_endpoint = str(candidates.get(selected_transport) or candidates.get("local") or "")

            tunnel_payload = service_payload.get("tunnel") if isinstance(service_payload.get("tunnel"), dict) else {}
            fallback_payload = service_payload.get("fallback") if isinstance(service_payload.get("fallback"), dict) else {}
            cloudflare_payload = fallback_payload.get("cloudflare") if isinstance(fallback_payload.get("cloudflare"), dict) else {}
            stale_tunnel_url = str(
                resolved_entry.get("stale_tunnel_url")
                or tunnel_payload.get("stale_tunnel_url")
                or cloudflare_payload.get("stale_tunnel_url")
                or ""
            ).strip()
            tunnel_error = str(
                resolved_entry.get("tunnel_error")
                or tunnel_payload.get("error")
                or cloudflare_payload.get("error")
                or ""
            ).strip()
            stale_reason = str(resolved_entry.get("stale_reason") or service_payload.get("stale_reason") or "").strip()
            stale_rejected = bool(resolved_entry.get("stale_rejected"))
            has_candidate = any(bool(value) for value in candidates.values())
            if healthy and has_candidate:
                healthy_with_candidates += 1
            if healthy:
                healthy_count += 1
            if publication.get("enabled"):
                published_count += 1

            candidate_reachability = {
                "cloudflare": bool(candidates.get("cloudflare")) and str((cloudflare_payload or {}).get("state") or "").lower() in {"active", "running"},
                "nkn": bool(candidates.get("nkn")),
                "local": bool(candidates.get("local")) and healthy,
                "upnp": bool(candidates.get("upnp")),
                "nats": bool(candidates.get("nats")),
            }

            info = SERVICE_TARGETS.get(service_name) if isinstance(SERVICE_TARGETS.get(service_name), dict) else {}
            repository = str(publication.get("repository") or "hydra")
            services.append(
                {
                    "service_id": service_name,
                    "service": service_name,
                    "target": str((info or {}).get("target") or service_name),
                    "aliases": list((info or {}).get("aliases") or []),
                    "status": str(service_payload.get("status") or ("ok" if healthy else "degraded")),
                    "healthy": healthy,
                    "running": bool(service_status.get("running")),
                    "enabled": bool(publication.get("enabled")),
                    "visibility": str(publication.get("visibility") or "public"),
                    "capacity_hint": int(publication.get("capacity_hint") or 0),
                    "pricing": publication.get("pricing") if isinstance(publication.get("pricing"), dict) else {},
                    "tags": list(publication.get("tags") or []),
                    "selected_transport": selected_transport,
                    "selection_reason": str(resolved_entry.get("selection_reason") or ""),
                    "selected_endpoint": selected_endpoint,
                    "base_url": selected_endpoint,
                    "http_endpoint": str(resolved_entry.get("http_endpoint") or selected_endpoint),
                    "ws_endpoint": str(resolved_entry.get("ws_endpoint") or ""),
                    "endpoint_candidates": candidates,
                    "candidate_reachability": candidate_reachability,
                    "stale_rejected": stale_rejected,
                    "stale_reason": stale_reason,
                    "stale_tunnel_url": stale_tunnel_url,
                    "tunnel_error": tunnel_error,
                    "cloudflared": {
                        "state": str((cloudflare_payload or {}).get("state") or (tunnel_payload or {}).get("state") or "inactive"),
                        "running": bool((tunnel_payload or {}).get("running")),
                        "tunnel_url": str((tunnel_payload or {}).get("tunnel_url") or (cloudflare_payload or {}).get("public_base_url") or ""),
                        "stale_tunnel_url": stale_tunnel_url,
                        "error": tunnel_error,
                        "restarts": int((cloudflare_payload or {}).get("restarts") or (tunnel_payload or {}).get("restarts") or 0),
                        "rate_limited": bool((cloudflare_payload or {}).get("rate_limited") or (tunnel_payload or {}).get("rate_limited")),
                    },
                    "health": service_payload.get("health") if isinstance(service_payload.get("health"), dict) else {},
                    "provenance": {
                        "parent_router": "hydra_router",
                        "watchdog_service": service_name,
                        "repository": repository,
                        "scope": "parent" if repository in {"", "hydra"} else "subordinate",
                        "discovery_source": str(safe_snapshot.get("discovery_source") or "local_snapshot"),
                    },
                    "updated_at_ms": int(safe_snapshot.get("ts_ms") or 0),
                }
            )

        service_count = len(services)
        catalog_ready = bool(service_count > 0)
        return {
            "status": "success",
            "interop_contract": self._interop_contract_payload(),
            "interop_contract_version": str(self._interop_contract_payload().get("version") or ""),
            "interop_contract_compat_min_version": str(self._interop_contract_payload().get("compat_min_version") or ""),
            "interop_contract_namespace": str(self._interop_contract_payload().get("namespace") or ""),
            "provider": provider,
            "generated_at_ms": int(time.time() * 1000),
            "snapshot_ts_ms": int(safe_snapshot.get("ts_ms") or 0),
            "discovery_source": str(safe_snapshot.get("discovery_source") or "local_snapshot"),
            "services": services,
            "summary": {
                "service_count": service_count,
                "published_count": published_count,
                "healthy_count": healthy_count,
                "healthy_with_candidate_count": healthy_with_candidates,
                "catalog_ready": catalog_ready,
                "include_unhealthy": include_all,
            },
            "resolve_summary": resolve_summary,
            "runtime_overrides": self._catalog_runtime_overrides_payload(),
        }

    def _marketplace_sync_auth_token(self, *, token_override: Any = "", cfg_override: Optional[Dict[str, Any]] = None) -> str:
        override = str(token_override or "").strip()
        if override:
            return override
        cfg = cfg_override if isinstance(cfg_override, dict) else (
            self.marketplace_sync_cfg if isinstance(self.marketplace_sync_cfg, dict) else {}
        )
        token = str(cfg.get("auth_token") or "").strip()
        if token:
            return token
        env_name = str(cfg.get("auth_token_env") or "").strip()
        if not env_name:
            return ""
        return str(os.environ.get(env_name) or "").strip()

    def _marketplace_sync_backoff_seconds(self, consecutive_failures: int = 0) -> int:
        cfg = self.marketplace_sync_cfg if isinstance(self.marketplace_sync_cfg, dict) else {}
        base_interval = self._as_int(cfg.get("publish_interval_seconds"), 45, minimum=5, maximum=3600)
        max_backoff = self._as_int(cfg.get("max_backoff_seconds"), 300, minimum=5, maximum=7200)
        failures = int(max(0, consecutive_failures))
        if failures <= 0:
            return base_interval
        exponent = min(6, failures - 1)
        backoff = base_interval * (2 ** exponent)
        return int(min(max_backoff, max(base_interval, backoff)))

    def _marketplace_sync_config_payload(self) -> Dict[str, Any]:
        cfg = self.marketplace_sync_cfg if isinstance(self.marketplace_sync_cfg, dict) else {}
        target_urls = _normalize_marketplace_sync_targets(
            cfg.get("target_urls_list"),
            fallback=_normalize_marketplace_sync_targets(cfg.get("target_urls")),
        )
        token_present = bool(self._marketplace_sync_auth_token(cfg_override=cfg))
        return {
            "enable_auto_publish": bool(cfg.get("enable_auto_publish")),
            "target_urls": list(target_urls),
            "target_count": len(target_urls),
            "publish_interval_seconds": self._as_int(cfg.get("publish_interval_seconds"), 45, minimum=5, maximum=3600),
            "publish_timeout_seconds": self._as_float(cfg.get("publish_timeout_seconds"), 6.0, minimum=1.0, maximum=120.0),
            "max_backoff_seconds": self._as_int(cfg.get("max_backoff_seconds"), 300, minimum=5, maximum=7200),
            "include_unhealthy": self._as_bool(cfg.get("include_unhealthy"), True),
            "auth_header": str(cfg.get("auth_header") or "Authorization"),
            "auth_scheme": str(cfg.get("auth_scheme") or "Bearer"),
            "auth_token_present": token_present,
            "auth_token_env": str(cfg.get("auth_token_env") or ""),
        }

    def _marketplace_sync_state_payload(self) -> Dict[str, Any]:
        cfg = self._marketplace_sync_config_payload()
        with self.marketplace_sync_state_lock:
            st = self.marketplace_sync_state if isinstance(self.marketplace_sync_state, dict) else {}
            results = list(st.get("results", [])) if isinstance(st.get("results"), deque) else []
            return {
                "status": "success",
                "config": cfg,
                "state": {
                    "in_flight": bool(st.get("in_flight")),
                    "last_trigger": str(st.get("last_trigger") or ""),
                    "last_attempt_ts_ms": int(st.get("last_attempt_ts_ms") or 0),
                    "last_success_ts_ms": int(st.get("last_success_ts_ms") or 0),
                    "last_failure_ts_ms": int(st.get("last_failure_ts_ms") or 0),
                    "last_error": str(st.get("last_error") or ""),
                    "last_status_code": int(st.get("last_status_code") or 0),
                    "next_due_ts_ms": int(st.get("next_due_ts_ms") or 0),
                    "success_count": int(st.get("success_count") or 0),
                    "failure_count": int(st.get("failure_count") or 0),
                    "consecutive_failures": int(st.get("consecutive_failures") or 0),
                    "last_result": _json_clone(st.get("last_result")) if isinstance(st.get("last_result"), dict) else {},
                    "recent_results": results[-10:],
                },
            }

    def _build_marketplace_sync_event(
        self,
        snapshot: Dict[str, Any],
        *,
        include_unhealthy: Optional[bool] = None,
        target_url: str = "",
    ) -> Dict[str, Any]:
        catalog = self._marketplace_catalog_payload(snapshot, include_unhealthy=include_unhealthy)
        provider = catalog.get("provider") if isinstance(catalog.get("provider"), dict) else {}
        summary = catalog.get("summary") if isinstance(catalog.get("summary"), dict) else {}
        interop = self._interop_contract_payload()
        addresses = self._current_node_addresses()
        source_address = addresses[0] if addresses else ""
        ts_ms = int(time.time() * 1000)
        return {
            "type": "market-service-catalog",
            "event": "market.service.catalog",
            "message_id": f"market-sync-{uuid.uuid4().hex[:16]}",
            "ts": ts_ms,
            "source_address": source_address,
            "target_url": str(target_url or ""),
            "interop_contract": interop,
            "interop_contract_version": str(interop.get("version") or ""),
            "interop_contract_compat_min_version": str(interop.get("compat_min_version") or ""),
            "interop_contract_namespace": str(interop.get("namespace") or ""),
            "provider": provider,
            "summary": summary,
            "catalog": catalog,
            "payload": {
                "provider": provider,
                "summary": summary,
                "catalog": catalog,
            },
        }

    def _publish_marketplace_catalog_targets(
        self,
        *,
        targets: Optional[List[str]] = None,
        include_unhealthy: Optional[bool] = None,
        timeout_s: Optional[float] = None,
        auth_token: str = "",
        trigger: str = "manual",
        dry_run: bool = False,
        force_refresh: bool = False,
    ) -> Dict[str, Any]:
        cfg_payload = self._marketplace_sync_config_payload()
        cfg = self.marketplace_sync_cfg if isinstance(self.marketplace_sync_cfg, dict) else {}
        normalized_targets = _normalize_marketplace_sync_targets(
            targets,
            fallback=cfg_payload.get("target_urls") if isinstance(cfg_payload.get("target_urls"), list) else [],
        )
        effective_timeout = self._as_float(
            timeout_s,
            cfg_payload.get("publish_timeout_seconds") if isinstance(cfg_payload, dict) else 6.0,
            minimum=1.0,
            maximum=120.0,
        )
        if not normalized_targets:
            return {
                "ok": False,
                "trigger": str(trigger or "manual"),
                "attempted": 0,
                "sent": 0,
                "failed": 0,
                "target_urls": [],
                "duration_ms": 0,
                "error": "no target URLs configured",
                "results": [],
                "dry_run": bool(dry_run),
            }

        snapshot = self.get_service_snapshot(force_refresh=force_refresh)
        include_unhealthy_value: Optional[bool] = include_unhealthy
        if include_unhealthy_value is None:
            include_unhealthy_value = self._as_bool(cfg.get("include_unhealthy"), True)
        effective_token = self._marketplace_sync_auth_token(token_override=auth_token, cfg_override=cfg)
        auth_header = str(cfg.get("auth_header") or "Authorization").strip() or "Authorization"
        auth_scheme = str(cfg.get("auth_scheme") or "Bearer").strip()
        started_at = time.time()

        results: List[Dict[str, Any]] = []
        sent = 0
        for target_url in normalized_targets:
            packet = self._build_marketplace_sync_event(
                snapshot,
                include_unhealthy=include_unhealthy_value,
                target_url=target_url,
            )
            catalog_checksum = hashlib.sha256(
                json.dumps(packet.get("catalog"), sort_keys=True, ensure_ascii=False).encode("utf-8", errors="replace")
            ).hexdigest()
            if dry_run:
                results.append(
                    {
                        "url": target_url,
                        "ok": True,
                        "status": 0,
                        "latency_ms": 0.0,
                        "error": "",
                        "catalog_checksum": catalog_checksum,
                        "dry_run": True,
                    }
                )
                sent += 1
                continue

            headers = {
                "Accept": "application/json",
                "Content-Type": "application/json",
                "X-Hydra-Interop-Event": "market.service.catalog",
            }
            if effective_token:
                if auth_header.lower() == "authorization":
                    prefix = f"{auth_scheme} " if auth_scheme else ""
                    headers["Authorization"] = f"{prefix}{effective_token}".strip()
                else:
                    headers[auth_header] = effective_token

            req_started = time.time()
            try:
                resp = requests.post(
                    target_url,
                    json=packet,
                    timeout=effective_timeout,
                    headers=headers,
                )
                latency_ms = round((time.time() - req_started) * 1000.0, 2)
                status_code = int(resp.status_code or 0)
                body_text = ""
                payload_json: Any = None
                try:
                    payload_json = resp.json()
                except Exception:
                    payload_json = None
                    body_text = resp.text[:240] if resp.text else ""
                ok = 200 <= status_code < 300
                error_text = ""
                if not ok:
                    if isinstance(payload_json, dict):
                        error_text = str(
                            payload_json.get("error")
                            or payload_json.get("message")
                            or f"http_{status_code}"
                        )
                    else:
                        error_text = body_text or f"http_{status_code}"
                result_entry: Dict[str, Any] = {
                    "url": target_url,
                    "ok": ok,
                    "status": status_code,
                    "latency_ms": latency_ms,
                    "error": error_text,
                    "catalog_checksum": catalog_checksum,
                }
                if isinstance(payload_json, dict):
                    response_status = str(payload_json.get("status") or "").strip()
                    if response_status:
                        result_entry["response_status"] = response_status
                elif body_text:
                    result_entry["response_text"] = body_text
                results.append(result_entry)
                if ok:
                    sent += 1
            except Exception as exc:
                latency_ms = round((time.time() - req_started) * 1000.0, 2)
                results.append(
                    {
                        "url": target_url,
                        "ok": False,
                        "status": 0,
                        "latency_ms": latency_ms,
                        "error": f"{type(exc).__name__}: {exc}",
                        "catalog_checksum": catalog_checksum,
                    }
                )

        attempted = len(normalized_targets)
        failed = max(0, attempted - sent)
        ok = bool(attempted > 0 and failed == 0)
        duration_ms = int(max(0.0, (time.time() - started_at) * 1000.0))
        first_error = ""
        for entry in results:
            if not entry.get("ok"):
                first_error = str(entry.get("error") or "")
                if first_error:
                    break
        return {
            "ok": ok,
            "trigger": str(trigger or "manual"),
            "attempted": attempted,
            "sent": sent,
            "failed": failed,
            "target_urls": list(normalized_targets),
            "duration_ms": duration_ms,
            "error": first_error,
            "results": results,
            "dry_run": bool(dry_run),
            "generated_at_ms": int(time.time() * 1000),
        }

    def _marketplace_sync_mark_started(self, trigger: str, targets: List[str]) -> bool:
        with self.marketplace_sync_state_lock:
            if bool(self.marketplace_sync_state.get("in_flight")):
                return False
            now_ms = int(time.time() * 1000)
            self.marketplace_sync_state["in_flight"] = True
            self.marketplace_sync_state["last_trigger"] = str(trigger or "")
            self.marketplace_sync_state["last_attempt_ts_ms"] = now_ms
            self.marketplace_sync_state["last_error"] = ""
            self.marketplace_sync_state["last_status_code"] = 0
            self.marketplace_sync_state["target_urls"] = list(targets)
            return True

    def _marketplace_sync_finalize(self, result: Dict[str, Any]) -> None:
        safe_result = result if isinstance(result, dict) else {}
        now_ms = int(time.time() * 1000)
        cfg_payload = self._marketplace_sync_config_payload()
        with self.marketplace_sync_state_lock:
            self.marketplace_sync_state["in_flight"] = False
            ok = bool(safe_result.get("ok"))
            status_code = 200 if ok else 502
            self.marketplace_sync_state["last_status_code"] = int(safe_result.get("status") or status_code)
            if ok:
                self.marketplace_sync_state["success_count"] = int(self.marketplace_sync_state.get("success_count") or 0) + 1
                self.marketplace_sync_state["consecutive_failures"] = 0
                self.marketplace_sync_state["last_success_ts_ms"] = now_ms
                self.marketplace_sync_state["last_error"] = ""
            else:
                self.marketplace_sync_state["failure_count"] = int(self.marketplace_sync_state.get("failure_count") or 0) + 1
                self.marketplace_sync_state["consecutive_failures"] = int(
                    self.marketplace_sync_state.get("consecutive_failures") or 0
                ) + 1
                self.marketplace_sync_state["last_failure_ts_ms"] = now_ms
                self.marketplace_sync_state["last_error"] = str(safe_result.get("error") or "publish failed")

            result_record = {
                "ts_ms": now_ms,
                "trigger": str(safe_result.get("trigger") or ""),
                "ok": ok,
                "attempted": int(safe_result.get("attempted") or 0),
                "sent": int(safe_result.get("sent") or 0),
                "failed": int(safe_result.get("failed") or 0),
                "duration_ms": int(safe_result.get("duration_ms") or 0),
                "error": str(safe_result.get("error") or ""),
                "dry_run": bool(safe_result.get("dry_run")),
            }
            results = self.marketplace_sync_state.get("results")
            if not isinstance(results, deque):
                results = deque(maxlen=24)
                self.marketplace_sync_state["results"] = results
            results.append(result_record)
            self.marketplace_sync_state["last_result"] = _json_clone(safe_result)

            if bool(cfg_payload.get("enable_auto_publish")) and int(cfg_payload.get("target_count") or 0) > 0:
                failures = int(self.marketplace_sync_state.get("consecutive_failures") or 0)
                delay_s = self._marketplace_sync_backoff_seconds(failures)
                self.marketplace_sync_state["next_due_ts_ms"] = now_ms + int(delay_s * 1000)
            else:
                self.marketplace_sync_state["next_due_ts_ms"] = 0

    def _run_marketplace_sync_job(
        self,
        *,
        targets: Optional[List[str]] = None,
        include_unhealthy: Optional[bool] = None,
        timeout_s: Optional[float] = None,
        auth_token: str = "",
        trigger: str = "manual",
        dry_run: bool = False,
        force_refresh: bool = False,
        prestarted: bool = False,
    ) -> Dict[str, Any]:
        normalized_targets = _normalize_marketplace_sync_targets(targets)
        if not prestarted:
            if not self._marketplace_sync_mark_started(trigger, normalized_targets):
                return {
                    "ok": False,
                    "trigger": str(trigger or "manual"),
                    "attempted": 0,
                    "sent": 0,
                    "failed": 0,
                    "target_urls": normalized_targets,
                    "duration_ms": 0,
                    "error": "sync already in progress",
                    "results": [],
                    "dry_run": bool(dry_run),
                }
        try:
            result = self._publish_marketplace_catalog_targets(
                targets=normalized_targets,
                include_unhealthy=include_unhealthy,
                timeout_s=timeout_s,
                auth_token=auth_token,
                trigger=trigger,
                dry_run=dry_run,
                force_refresh=force_refresh,
            )
        except Exception as exc:
            result = {
                "ok": False,
                "trigger": str(trigger or "manual"),
                "attempted": len(normalized_targets),
                "sent": 0,
                "failed": len(normalized_targets),
                "target_urls": normalized_targets,
                "duration_ms": 0,
                "error": f"{type(exc).__name__}: {exc}",
                "results": [],
                "dry_run": bool(dry_run),
            }
        self._marketplace_sync_finalize(result)
        return result

    def _run_marketplace_sync_worker(
        self,
        *,
        targets: List[str],
        include_unhealthy: Optional[bool],
        timeout_s: Optional[float],
        auth_token: str,
    ) -> None:
        try:
            self._run_marketplace_sync_job(
                targets=targets,
                include_unhealthy=include_unhealthy,
                timeout_s=timeout_s,
                auth_token=auth_token,
                trigger="auto",
                dry_run=False,
                force_refresh=False,
                prestarted=True,
            )
        finally:
            self.marketplace_sync_worker = None

    def _maybe_start_marketplace_sync(self) -> None:
        cfg = self.marketplace_sync_cfg if isinstance(self.marketplace_sync_cfg, dict) else {}
        if not self._as_bool(cfg.get("enable_auto_publish"), False):
            return
        targets = _normalize_marketplace_sync_targets(cfg.get("target_urls_list"))
        if not targets:
            return
        with self.marketplace_sync_state_lock:
            if bool(self.marketplace_sync_state.get("in_flight")):
                return
            now_ms = int(time.time() * 1000)
            due_ms = int(self.marketplace_sync_state.get("next_due_ts_ms") or 0)
            if due_ms > 0 and now_ms < due_ms:
                return
        if not self._marketplace_sync_mark_started("auto", targets):
            return

        timeout_s = self._as_float(cfg.get("publish_timeout_seconds"), 6.0, minimum=1.0, maximum=120.0)
        include_unhealthy = self._as_bool(cfg.get("include_unhealthy"), True)
        token = self._marketplace_sync_auth_token(cfg_override=cfg)
        worker = threading.Thread(
            target=self._run_marketplace_sync_worker,
            kwargs={
                "targets": list(targets),
                "include_unhealthy": include_unhealthy,
                "timeout_s": timeout_s,
                "auth_token": token,
            },
            daemon=True,
            name="marketplace-sync",
        )
        self.marketplace_sync_worker = worker
        try:
            worker.start()
        except Exception as exc:
            self.marketplace_sync_worker = None
            self._marketplace_sync_finalize(
                {
                    "ok": False,
                    "trigger": "auto",
                    "attempted": len(targets),
                    "sent": 0,
                    "failed": len(targets),
                    "target_urls": list(targets),
                    "duration_ms": 0,
                    "error": f"{type(exc).__name__}: {exc}",
                    "results": [],
                    "dry_run": False,
                }
            )

    def _interop_contract_status_from_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        expected = self._interop_contract_payload()
        body = payload if isinstance(payload, dict) else {}
        incoming_contract_raw = body.get("interop_contract") if isinstance(body.get("interop_contract"), dict) else {}
        incoming_name = str(
            incoming_contract_raw.get("name")
            or body.get("interop_contract_name")
            or ""
        ).strip()
        incoming_version = str(
            body.get("interop_contract_version")
            or incoming_contract_raw.get("version")
            or ""
        ).strip()
        incoming_compat_min = str(
            body.get("interop_contract_compat_min_version")
            or incoming_contract_raw.get("compat_min_version")
            or incoming_contract_raw.get("compatMinVersion")
            or incoming_version
            or "0.0.0"
        ).strip()
        if not incoming_name:
            incoming_name = expected["name"]
        if not incoming_version:
            incoming_version = "0.0.0"

        name_ok = incoming_name == expected["name"]
        version_ok = _semver_gte(incoming_version, expected["compat_min_version"])
        compat_ok = _semver_gte(expected["version"], incoming_compat_min)
        ok = bool(name_ok and version_ok and compat_ok)
        return {
            "ok": ok,
            "name_ok": name_ok,
            "version_ok": version_ok,
            "compat_ok": compat_ok,
            "expected": expected,
            "incoming": {
                "name": incoming_name,
                "version": incoming_version,
                "compat_min_version": incoming_compat_min,
            },
        }

    def _health_telemetry_totals(self) -> Dict[str, Any]:
        with self.telemetry_lock:
            rpc_metering = self.telemetry_state.get("rpc_metering") if isinstance(self.telemetry_state.get("rpc_metering"), dict) else {}
            rpc_enforcement = self.telemetry_state.get("rpc_enforcement") if isinstance(self.telemetry_state.get("rpc_enforcement"), dict) else {}
            return {
                "inbound_messages": int(self.telemetry_state.get("inbound_messages", 0)),
                "outbound_messages": int(self.telemetry_state.get("outbound_messages", 0)),
                "inbound_bytes": int(self.telemetry_state.get("inbound_bytes", 0)),
                "outbound_bytes": int(self.telemetry_state.get("outbound_bytes", 0)),
                "resolve_requests_in": int(self.telemetry_state.get("resolve_requests_in", 0)),
                "resolve_requests_out": int(self.telemetry_state.get("resolve_requests_out", 0)),
                "resolve_success_out": int(self.telemetry_state.get("resolve_success_out", 0)),
                "resolve_fail_out": int(self.telemetry_state.get("resolve_fail_out", 0)),
                "rpc_requests_in": int(self.telemetry_state.get("rpc_requests_in", 0)),
                "rpc_requests_out": int(self.telemetry_state.get("rpc_requests_out", 0)),
                "rpc_success_out": int(self.telemetry_state.get("rpc_success_out", 0)),
                "rpc_fail_out": int(self.telemetry_state.get("rpc_fail_out", 0)),
                "rpc_metering_requests": int(rpc_metering.get("requests", 0)),
                "rpc_metering_success": int(rpc_metering.get("success", 0)),
                "rpc_metering_failure": int(rpc_metering.get("failure", 0)),
                "rpc_metering_estimated_units_total": float(rpc_metering.get("estimated_units_total", 0.0)),
                "rpc_metering_estimated_micros_total": int(rpc_metering.get("estimated_micros_total", 0)),
                "rpc_metering_settled_micros_total": int(rpc_metering.get("settled_micros_total", 0)),
                "rpc_enforcement_checks": int(rpc_enforcement.get("checks", 0)),
                "rpc_enforcement_allow": int(rpc_enforcement.get("allow", 0)),
                "rpc_enforcement_deny": int(rpc_enforcement.get("deny", 0)),
                "active_peers": len(self.telemetry_state.get("peer_usage", {})),
            }

    def _rollout_gates(
        self,
        snapshot: Dict[str, Any],
        pending_count: int,
        resolve_summary: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        safe_snapshot = snapshot if isinstance(snapshot, dict) else {}
        resolved = safe_snapshot.get("resolved", {})
        resolved_map = resolved if isinstance(resolved, dict) else {}
        summary = resolve_summary if isinstance(resolve_summary, dict) else self._resolved_discovery_summary(resolved_map)
        telemetry = self._health_telemetry_totals()
        resolve_success = int(telemetry.get("resolve_success_out", 0))
        resolve_fail = int(telemetry.get("resolve_fail_out", 0))
        resolve_total = resolve_success + resolve_fail
        resolve_error_rate = float(resolve_fail / resolve_total) if resolve_total > 0 else 0.0
        rpc_requests_out = int(telemetry.get("rpc_requests_out", 0))
        rpc_metering_requests = int(telemetry.get("rpc_metering_requests", 0))
        rpc_metering_coverage = float(rpc_metering_requests / rpc_requests_out) if rpc_requests_out > 0 else 1.0
        rpc_metering_ready = bool(rpc_metering_coverage >= 0.8)

        addresses = self._current_node_addresses()
        contract_status = self._interop_contract_status_from_payload(safe_snapshot)
        expected_contract = contract_status.get("expected", {}) if isinstance(contract_status, dict) else {}
        incoming_contract = contract_status.get("incoming", {}) if isinstance(contract_status, dict) else {}
        expected_version = str(expected_contract.get("version") or "")
        reported_contract = str(incoming_contract.get("version") or expected_version)
        contract_ok = bool(contract_status.get("ok"))
        nkn_ready = bool(self.nkn_settings.get("enable", True) and addresses)
        resolved_services = int(summary.get("service_count") or len(resolved_map))
        stale_count = int(summary.get("stale_rejection_count") or 0)
        pending_clear = int(max(0, pending_count)) == 0
        resolve_error_rate_ok = resolve_error_rate <= 0.30
        ready = bool(
            contract_ok
            and nkn_ready
            and resolved_services > 0
            and pending_clear
            and resolve_error_rate_ok
            and rpc_metering_ready
        )

        return {
            "ready": ready,
            "contract_name_expected": str(expected_contract.get("name") or ""),
            "contract_name_reported": str(incoming_contract.get("name") or ""),
            "contract_name_ok": bool(contract_status.get("name_ok")),
            "contract_version_expected": expected_version,
            "contract_version_reported": reported_contract,
            "contract_version_ok": bool(contract_status.get("version_ok")),
            "contract_compat_min_version_expected": str(expected_contract.get("compat_min_version") or ""),
            "contract_compat_min_version_reported": str(incoming_contract.get("compat_min_version") or ""),
            "contract_compat_ok": bool(contract_status.get("compat_ok")),
            "contract_ok": contract_ok,
            "nkn_ready": nkn_ready,
            "resolved_services": resolved_services,
            "pending_resolves_clear": pending_clear,
            "stale_rejection_count": stale_count,
            "resolve_total": resolve_total,
            "resolve_error_rate": round(resolve_error_rate, 4),
            "resolve_error_rate_ok": resolve_error_rate_ok,
            "rpc_metering_requests": rpc_metering_requests,
            "rpc_metering_coverage": round(rpc_metering_coverage, 4),
            "rpc_metering_ready": rpc_metering_ready,
        }

    def _health_payload(self, snapshot: Dict[str, Any], pending_count: int) -> Dict[str, Any]:
        addresses = self._current_node_addresses()
        resolved = snapshot.get("resolved", {}) if isinstance(snapshot, dict) else {}
        resolved_map = resolved if isinstance(resolved, dict) else {}
        resolve_summary = self._resolved_discovery_summary(resolved_map)
        catalog = self._marketplace_catalog_payload(snapshot if isinstance(snapshot, dict) else {})
        telemetry = self._health_telemetry_totals()
        rpc_out = int(telemetry.get("rpc_requests_out", 0))
        rpc_metered = int(telemetry.get("rpc_metering_requests", 0))
        metering_coverage = float(rpc_metered / rpc_out) if rpc_out > 0 else 1.0
        return {
            "status": "ok",
            "service": "hydra_router",
            "interop_contract": self._interop_contract_payload(),
            "interop_contract_version": str(self._interop_contract_payload().get("version") or ""),
            "interop_contract_compat_min_version": str(self._interop_contract_payload().get("compat_min_version") or ""),
            "interop_contract_namespace": str(self._interop_contract_payload().get("namespace") or ""),
            "auth_capabilities": self._auth_capabilities_payload(),
            "uptime_seconds": round(time.time() - self.startup_time, 2),
            "requests_served": int(self.request_counter.get("value", 0)),
            "pending_resolves": int(max(0, pending_count)),
            "network": self._router_network_urls(),
            "nkn": {
                "enabled": bool(self.nkn_settings.get("enable", True)),
                "ready": bool(addresses),
                "addresses": addresses,
            },
            "telemetry": telemetry,
            "metering": {
                "requests": int(telemetry.get("rpc_metering_requests", 0)),
                "success": int(telemetry.get("rpc_metering_success", 0)),
                "failure": int(telemetry.get("rpc_metering_failure", 0)),
                "estimated_units_total": float(telemetry.get("rpc_metering_estimated_units_total", 0.0)),
                "estimated_micros_total": int(telemetry.get("rpc_metering_estimated_micros_total", 0)),
                "settled_micros_total": int(telemetry.get("rpc_metering_settled_micros_total", 0)),
                "coverage": round(metering_coverage, 4),
                "ready": bool(metering_coverage >= 0.8),
            },
            "resolve_summary": resolve_summary,
            "catalog_summary": catalog.get("summary", {}) if isinstance(catalog, dict) else {},
            "provider": catalog.get("provider", {}) if isinstance(catalog, dict) else {},
            "marketplace_sync": self._marketplace_sync_state_payload(),
            "rollout_gates": self._rollout_gates(snapshot, pending_count, resolve_summary=resolve_summary),
            "snapshot": snapshot if isinstance(snapshot, dict) else {},
        }

    def _services_snapshot_payload(self, snapshot: Dict[str, Any]) -> Dict[str, Any]:
        resolved = snapshot.get("resolved", {}) if isinstance(snapshot, dict) else {}
        summary = self._resolved_discovery_summary(resolved if isinstance(resolved, dict) else {})
        catalog = self._marketplace_catalog_payload(snapshot if isinstance(snapshot, dict) else {})
        return {
            "status": "success",
            "interop_contract": self._interop_contract_payload(),
            "interop_contract_version": str(self._interop_contract_payload().get("version") or ""),
            "interop_contract_compat_min_version": str(self._interop_contract_payload().get("compat_min_version") or ""),
            "interop_contract_namespace": str(self._interop_contract_payload().get("namespace") or ""),
            "auth_capabilities": self._auth_capabilities_payload(),
            "snapshot": snapshot if isinstance(snapshot, dict) else {},
            "resolve_summary": summary,
            "catalog": catalog,
            "marketplace_sync": self._marketplace_sync_state_payload(),
            "rollout_gates": self._rollout_gates(snapshot, pending_count=0, resolve_summary=summary),
        }

    def _nkn_info_payload(self, snapshot: Dict[str, Any]) -> Dict[str, Any]:
        addresses = self._current_node_addresses()
        resolved = snapshot.get("resolved", {}) if isinstance(snapshot, dict) else {}
        summary = self._resolved_discovery_summary(resolved if isinstance(resolved, dict) else {})
        catalog = self._marketplace_catalog_payload(snapshot if isinstance(snapshot, dict) else {})
        return {
            "status": "success",
            "interop_contract": self._interop_contract_payload(),
            "interop_contract_version": str(self._interop_contract_payload().get("version") or ""),
            "interop_contract_compat_min_version": str(self._interop_contract_payload().get("compat_min_version") or ""),
            "interop_contract_namespace": str(self._interop_contract_payload().get("namespace") or ""),
            "auth_capabilities": self._auth_capabilities_payload(),
            "network": self._router_network_urls(),
            "nkn": {
                "enabled": bool(self.nkn_settings.get("enable", True)),
                "ready": bool(addresses),
                "addresses": addresses,
                "service_relays": {svc: entry.get("name") for svc, entry in self.service_relays.items()},
                "seed_persisted": any(bool((entry or {}).get("seed_hex")) for entry in self.service_relays.values()),
            },
            "snapshot": snapshot if isinstance(snapshot, dict) else {},
            "resolve_summary": summary,
            "catalog_summary": catalog.get("summary", {}) if isinstance(catalog, dict) else {},
            "provider": catalog.get("provider", {}) if isinstance(catalog, dict) else {},
            "marketplace_sync": self._marketplace_sync_state_payload(),
            "rollout_gates": self._rollout_gates(snapshot, pending_count=0, resolve_summary=summary),
        }

    def _nkn_resolve_local_payload(self, snapshot: Dict[str, Any], target_address: str = "") -> Dict[str, Any]:
        addresses = self._current_node_addresses()
        resolved = snapshot.get("resolved", {}) if isinstance(snapshot, dict) else {}
        resolved_map = resolved if isinstance(resolved, dict) else {}
        summary = self._resolved_discovery_summary(resolved_map)
        catalog = self._marketplace_catalog_payload(snapshot if isinstance(snapshot, dict) else {})
        return {
            "status": "success",
            "interop_contract": self._interop_contract_payload(),
            "interop_contract_version": str(self._interop_contract_payload().get("version") or ""),
            "interop_contract_compat_min_version": str(self._interop_contract_payload().get("compat_min_version") or ""),
            "interop_contract_namespace": str(self._interop_contract_payload().get("namespace") or ""),
            "auth_capabilities": self._auth_capabilities_payload(),
            "mode": "local",
            "discovery_source": "local_snapshot",
            "target_address": str(target_address or (addresses[0] if addresses else "")),
            "snapshot": snapshot if isinstance(snapshot, dict) else {},
            "resolved": resolved_map,
            "resolve_summary": summary,
            "endpoint_candidates": summary.get("candidates", {}),
            "stale_rejections": summary.get("stale_rejections", []),
            "stale_rejection_count": int(summary.get("stale_rejection_count", 0)),
            "catalog": catalog,
            "marketplace_sync": self._marketplace_sync_state_payload(),
            "rollout_gates": self._rollout_gates(snapshot, pending_count=0, resolve_summary=summary),
        }

    def _nkn_resolve_remote_payload(
        self,
        request_id: str,
        target_address: str,
        source_address: str,
        response_payload: Dict[str, Any],
        snapshot: Dict[str, Any],
        resolved: Dict[str, Any],
    ) -> Dict[str, Any]:
        resolved_map = resolved if isinstance(resolved, dict) else {}
        summary = self._resolved_discovery_summary(resolved_map)
        reply_catalog = response_payload.get("catalog") if isinstance(response_payload.get("catalog"), dict) else {}
        catalog = reply_catalog if reply_catalog else self._marketplace_catalog_payload(snapshot if isinstance(snapshot, dict) else {})
        remote_auth = response_payload.get("auth_capabilities") if isinstance(response_payload.get("auth_capabilities"), dict) else {}
        return {
            "status": "success",
            "interop_contract": self._interop_contract_payload(),
            "interop_contract_version": str(self._interop_contract_payload().get("version") or ""),
            "interop_contract_compat_min_version": str(self._interop_contract_payload().get("compat_min_version") or ""),
            "interop_contract_namespace": str(self._interop_contract_payload().get("namespace") or ""),
            "auth_capabilities": self._auth_capabilities_payload(),
            "mode": "remote",
            "discovery_source": str(response_payload.get("discovery_source") or "nkn_dm"),
            "request_id": str(request_id or ""),
            "target_address": str(target_address or ""),
            "source_address": str(source_address or ""),
            "reply": response_payload if isinstance(response_payload, dict) else {},
            "snapshot": snapshot if isinstance(snapshot, dict) else {},
            "resolved": resolved_map,
            "resolve_summary": summary,
            "endpoint_candidates": summary.get("candidates", {}),
            "stale_rejections": summary.get("stale_rejections", []),
            "stale_rejection_count": int(summary.get("stale_rejection_count", 0)),
            "catalog": catalog,
            "remote_auth_capabilities": remote_auth,
            "marketplace_sync": self._marketplace_sync_state_payload(),
            "rollout_gates": self._rollout_gates(snapshot, pending_count=0, resolve_summary=summary),
        }

    def _snapshot_dashboard_data(self, history_limit: Any = 240, log_limit: Any = 120, peer_limit: Any = 50) -> Dict[str, Any]:
        with self.telemetry_lock:
            history = list(self.telemetry_state.get("history", []))
            peer_usage = dict(self.telemetry_state.get("peer_usage", {}))
            endpoint_hits = dict(self.telemetry_state.get("endpoint_hits", {}))
            rpc_metering = self.telemetry_state.get("rpc_metering") if isinstance(self.telemetry_state.get("rpc_metering"), dict) else {}
            rpc_metering_events = list(rpc_metering.get("events", [])) if isinstance(rpc_metering.get("events"), deque) else []
            rpc_metering_by_service = _json_clone(rpc_metering.get("by_service")) if isinstance(rpc_metering.get("by_service"), dict) else {}
            rpc_enforcement = self.telemetry_state.get("rpc_enforcement") if isinstance(self.telemetry_state.get("rpc_enforcement"), dict) else {}
            telemetry = {
                "inbound_messages": int(self.telemetry_state.get("inbound_messages", 0)),
                "outbound_messages": int(self.telemetry_state.get("outbound_messages", 0)),
                "inbound_bytes": int(self.telemetry_state.get("inbound_bytes", 0)),
                "outbound_bytes": int(self.telemetry_state.get("outbound_bytes", 0)),
                "resolve_requests_in": int(self.telemetry_state.get("resolve_requests_in", 0)),
                "resolve_requests_out": int(self.telemetry_state.get("resolve_requests_out", 0)),
                "resolve_success_out": int(self.telemetry_state.get("resolve_success_out", 0)),
                "resolve_fail_out": int(self.telemetry_state.get("resolve_fail_out", 0)),
                "rpc_requests_in": int(self.telemetry_state.get("rpc_requests_in", 0)),
                "rpc_requests_out": int(self.telemetry_state.get("rpc_requests_out", 0)),
                "rpc_success_out": int(self.telemetry_state.get("rpc_success_out", 0)),
                "rpc_fail_out": int(self.telemetry_state.get("rpc_fail_out", 0)),
                "rpc_metering_requests": int(rpc_metering.get("requests", 0)),
                "rpc_metering_success": int(rpc_metering.get("success", 0)),
                "rpc_metering_failure": int(rpc_metering.get("failure", 0)),
                "rpc_metering_estimated_units_total": float(rpc_metering.get("estimated_units_total", 0.0)),
                "rpc_metering_estimated_micros_total": int(rpc_metering.get("estimated_micros_total", 0)),
                "rpc_metering_settled_micros_total": int(rpc_metering.get("settled_micros_total", 0)),
                "rpc_enforcement_checks": int(rpc_enforcement.get("checks", 0)),
                "rpc_enforcement_allow": int(rpc_enforcement.get("allow", 0)),
                "rpc_enforcement_deny": int(rpc_enforcement.get("deny", 0)),
            }
        logs = list(self.activity_log)
        hist_limit = self._as_int(history_limit, 240, minimum=20, maximum=2000)
        logs_limit = self._as_int(log_limit, 120, minimum=20, maximum=1000)
        peers_limit = self._as_int(peer_limit, 50, minimum=10, maximum=500)
        peers_sorted = sorted(peer_usage.items(), key=lambda kv: int((kv[1] or {}).get("last_ts_ms", 0)), reverse=True)
        return {
            "status": "success",
            "uptime_seconds": round(time.time() - self.startup_time, 2),
            "requests_served": int(self.request_counter.get("value", 0)),
            "telemetry": telemetry,
            "history": history[-hist_limit:],
            "activity": logs[-logs_limit:],
            "peers": [{**(entry or {}), "peer": peer} for peer, entry in peers_sorted[:peers_limit]],
            "endpoint_hits": endpoint_hits,
            "rpc_metering": {
                "requests": int(rpc_metering.get("requests", 0)),
                "success": int(rpc_metering.get("success", 0)),
                "failure": int(rpc_metering.get("failure", 0)),
                "estimated_units_total": float(rpc_metering.get("estimated_units_total", 0.0)),
                "estimated_micros_total": int(rpc_metering.get("estimated_micros_total", 0)),
                "settled_micros_total": int(rpc_metering.get("settled_micros_total", 0)),
                "by_service": rpc_metering_by_service,
                "events": rpc_metering_events[-100:],
            },
            "rpc_enforcement": {
                "checks": int(rpc_enforcement.get("checks", 0)),
                "allow": int(rpc_enforcement.get("allow", 0)),
                "deny": int(rpc_enforcement.get("deny", 0)),
                "denied_codes": _json_clone(rpc_enforcement.get("denied_codes")) if isinstance(rpc_enforcement.get("denied_codes"), dict) else {},
                "by_service": _json_clone(rpc_enforcement.get("by_service")) if isinstance(rpc_enforcement.get("by_service"), dict) else {},
                "last_event": _json_clone(rpc_enforcement.get("last_event")) if isinstance(rpc_enforcement.get("last_event"), dict) else {},
            },
            "marketplace_sync": self._marketplace_sync_state_payload(),
        }

    def _build_control_plane_app(self):
        try:
            from flask import Flask, jsonify, request
        except Exception as exc:
            raise RuntimeError(f"Control-plane dependencies unavailable: {exc}") from exc

        app = Flask(__name__)

        def _public_json(payload: Any, status_code: int = 200):
            safe_payload = self._redact_public_payload(payload)
            if status_code == 200:
                return jsonify(safe_payload)
            return jsonify(safe_payload), status_code

        @app.before_request
        def _count_requests():
            self.request_counter["value"] = int(self.request_counter.get("value", 0)) + 1

        @app.route("/", methods=["GET"])
        def _index():
            return _public_json(self._index_payload())

        @app.route("/api", methods=["GET"])
        def _api_index():
            return _public_json(self._index_payload())

        @app.route("/health", methods=["GET"])
        def _health():
            snapshot = self.get_service_snapshot(force_refresh=False)
            with self.pending_resolves_lock:
                pending_count = len(self.pending_resolves)
            payload = self._health_payload(snapshot, pending_count)
            return _public_json(payload)

        @app.route("/services/snapshot", methods=["GET"])
        def _services_snapshot():
            force = self._as_bool(request.args.get("refresh"), default=False)
            snapshot = self.get_service_snapshot(force_refresh=force)
            return _public_json(self._services_snapshot_payload(snapshot))

        @app.route("/nkn/info", methods=["GET"])
        def _nkn_info():
            snapshot = self.get_service_snapshot(force_refresh=False)
            return _public_json(self._nkn_info_payload(snapshot))

        @app.route("/nkn/resolve", methods=["POST"])
        def _nkn_resolve():
            data = request.get_json(silent=True) or {}
            target_address = str(data.get("router_address") or data.get("target_address") or "").strip()
            timeout_seconds = self._as_int(
                data.get("timeout_seconds", self.nkn_settings["resolve_timeout_seconds"]),
                self.nkn_settings["resolve_timeout_seconds"],
                minimum=2,
                maximum=60,
            )
            force_refresh = self._as_bool(data.get("refresh_local", True), default=True)
            local_addresses = set(self._current_node_addresses())

            if not target_address or target_address in local_addresses:
                snapshot = self.get_service_snapshot(force_refresh=force_refresh)
                return _public_json(self._nkn_resolve_local_payload(snapshot, target_address=target_address))

            pending = self._create_pending_resolve(target_address)
            payload = {
                "event": "resolve_tunnels",
                "request_id": pending["request_id"],
                "from": (next(iter(sorted(local_addresses))) if local_addresses else ""),
                "timestamp_ms": int(time.time() * 1000),
            }
            with self.telemetry_lock:
                self.telemetry_state["resolve_requests_out"] = int(self.telemetry_state.get("resolve_requests_out", 0)) + 1
            ok, err = self.send_nkn_dm(target_address, payload, tries=self.nkn_settings["dm_retries"])
            if not ok:
                self._pop_pending_resolve(pending["request_id"])
                self._record_resolve_outcome(False)
                return _public_json({"status": "error", "message": f"Failed to send DM: {err}"}, status_code=503)

            if not pending["event"].wait(timeout_seconds):
                self._pop_pending_resolve(pending["request_id"])
                self._record_resolve_outcome(False)
                return _public_json(
                    {
                        "status": "error",
                        "message": f"Timed out waiting for resolve reply from {target_address}",
                        "request_id": pending["request_id"],
                    },
                    status_code=504,
                )

            complete = self._pop_pending_resolve(pending["request_id"])
            if not complete or not complete.get("response"):
                self._record_resolve_outcome(False)
                return _public_json(
                    {"status": "error", "message": "Resolve response missing", "request_id": pending["request_id"]},
                    status_code=502,
                )

            response_payload = complete["response"]["payload"]
            source_address = complete["response"]["source"]
            snapshot = response_payload.get("snapshot") if isinstance(response_payload, dict) else {}
            if not isinstance(snapshot, dict):
                snapshot = {}
            resolved = snapshot.get("resolved", {})
            endpoint_labels = self._collect_endpoint_labels(resolved if isinstance(resolved, dict) else {})
            self._record_endpoint_usage(source_address, endpoint_labels)
            self._record_resolve_outcome(True)
            return _public_json(
                self._nkn_resolve_remote_payload(
                    pending["request_id"],
                    target_address,
                    source_address,
                    response_payload if isinstance(response_payload, dict) else {},
                    snapshot,
                    resolved if isinstance(resolved, dict) else {},
                )
            )

        @app.route("/marketplace/catalog", methods=["GET"])
        def _marketplace_catalog():
            market_enabled = self._as_bool((self.marketplace_cfg or {}).get("enable_catalog"), True)
            if not market_enabled:
                return _public_json(
                    {
                        "status": "disabled",
                        "message": "Marketplace catalog publishing is disabled",
                        "runtime_overrides": self._catalog_runtime_overrides_payload(),
                    },
                    status_code=503,
                )
            force = self._as_bool(request.args.get("refresh"), default=False)
            include_unhealthy = request.args.get("include_unhealthy")
            include_arg: Optional[bool] = None
            if include_unhealthy is not None:
                include_arg = self._as_bool(include_unhealthy, default=True)
            snapshot = self.get_service_snapshot(force_refresh=force)
            payload = self._marketplace_catalog_payload(snapshot, include_unhealthy=include_arg)
            return _public_json(payload)

        @app.route("/marketplace/sync", methods=["GET"])
        def _marketplace_sync():
            return _public_json(self._marketplace_sync_state_payload())

        @app.route("/marketplace/catalog/publish", methods=["POST"])
        def _marketplace_publish():
            body = request.get_json(silent=True) or {}
            market_enabled = self._as_bool((self.marketplace_cfg or {}).get("enable_catalog"), True)
            if not market_enabled:
                return _public_json(
                    {
                        "status": "disabled",
                        "message": "Marketplace catalog publishing is disabled",
                        "marketplace_sync": self._marketplace_sync_state_payload(),
                    },
                    status_code=503,
                )

            include_unhealthy_raw = body.get("include_unhealthy")
            include_unhealthy: Optional[bool] = None
            if include_unhealthy_raw is not None:
                include_unhealthy = self._as_bool(include_unhealthy_raw, True)

            cfg = self.marketplace_sync_cfg if isinstance(self.marketplace_sync_cfg, dict) else {}
            default_timeout = self._as_float(cfg.get("publish_timeout_seconds"), 6.0, minimum=1.0, maximum=120.0)
            timeout_seconds = self._as_float(body.get("timeout_seconds"), default_timeout, minimum=1.0, maximum=120.0)
            dry_run = self._as_bool(body.get("dry_run"), False)
            force_refresh = self._as_bool(body.get("force_refresh"), False) or self._as_bool(body.get("force"), False)
            auth_token = str(body.get("auth_token") or "").strip()
            requested_targets = _normalize_marketplace_sync_targets(
                body.get("target_urls"),
                fallback=_normalize_marketplace_sync_targets(cfg.get("target_urls_list")),
            )
            if not requested_targets:
                return _public_json(
                    {
                        "status": "error",
                        "error": "no target URLs configured",
                        "marketplace_sync": self._marketplace_sync_state_payload(),
                    },
                    status_code=400,
                )

            if not self._marketplace_sync_mark_started("manual", requested_targets):
                return _public_json(
                    {
                        "status": "error",
                        "error": "sync already in progress",
                        "marketplace_sync": self._marketplace_sync_state_payload(),
                    },
                    status_code=409,
                )

            result = self._run_marketplace_sync_job(
                targets=requested_targets,
                include_unhealthy=include_unhealthy,
                timeout_s=timeout_seconds,
                auth_token=auth_token,
                trigger="manual",
                dry_run=dry_run,
                force_refresh=force_refresh,
                prestarted=True,
            )
            payload = {
                "status": "success" if bool(result.get("ok")) else "error",
                "result": result,
                "marketplace_sync": self._marketplace_sync_state_payload(),
            }
            status_code = 200 if bool(result.get("ok")) else 502
            if dry_run and bool(result.get("ok")):
                status_code = 200
            return _public_json(payload, status_code=status_code)

        @app.route("/marketplace/catalog/overrides", methods=["POST", "DELETE"])
        def _marketplace_catalog_overrides():
            if request.method == "DELETE":
                svc_hint = str(request.args.get("service") or "").strip()
                if svc_hint:
                    canonical = self._canonical_router_service(svc_hint)
                    if canonical in self.catalog_runtime_overrides:
                        self.catalog_runtime_overrides.pop(canonical, None)
                else:
                    self.catalog_runtime_overrides = {}
                return _public_json(
                    {
                        "status": "success",
                        "runtime_overrides": self._catalog_runtime_overrides_payload(),
                    }
                )
            body = request.get_json(silent=True) or {}
            runtime_overrides = self._apply_catalog_overrides(body if isinstance(body, dict) else {})
            return _public_json({"status": "success", "runtime_overrides": runtime_overrides})

        @app.route("/dashboard/data", methods=["GET"])
        def _dashboard_data():
            data = self._snapshot_dashboard_data(
                history_limit=request.args.get("history", 240),
                log_limit=request.args.get("logs", 120),
                peer_limit=request.args.get("peers", 50),
            )
            return _public_json(data)

        return app

    def _start_control_plane(self) -> None:
        if not self.api_enabled:
            LOGGER.info("Router control-plane API disabled by config")
            return
        if self.api_server is not None:
            return
        try:
            from werkzeug.serving import make_server
        except Exception as exc:
            LOGGER.warning("Control-plane dependencies unavailable: %s", exc)
            return

        try:
            app = self._build_control_plane_app()
        except Exception as exc:
            LOGGER.warning("Failed to initialize control-plane Flask app: %s", exc)
            return

        try:
            self.api_server = make_server(self.api_host, self.api_port, app, threaded=True)
        except Exception as exc:
            LOGGER.warning("Failed to bind control-plane API on %s:%s (%s)", self.api_host, self.api_port, exc)
            self.api_server = None
            return
        self.api_thread = threading.Thread(target=self.api_server.serve_forever, daemon=True, name="router-api")
        self.api_thread.start()
        LOGGER.info("Control-plane API listening on http://%s:%s", self.api_host, self.api_port)

    # Port Detection -------------------------------------------
    def _detect_service_port(self, service_name: str) -> Optional[int]:
        """Detect actual port a service is running on by parsing its log file."""
        log_file = LOGS_ROOT / f"{service_name}.log"
        if not log_file.exists():
            return None

        try:
            import re

            # Read last 100 lines of log file (most recent startup info)
            with open(log_file, 'r') as f:
                lines = f.readlines()[-100:]

            # Look for common port announcement patterns
            patterns = [
                r'Running on .*:(\d+)',  # Flask: "Running on http://127.0.0.1:5000"
                r'listening on .*:(\d+)',  # Generic: "listening on 0.0.0.0:8080"
                r'Listening on port (\d+)',  # Generic: "Listening on port 8080"
                r'Server.*port (\d+)',  # Generic: "Server started on port 8080"
                r'Started.*:(\d+)',  # Generic: "Started on :8080"
                r'http://[^:]+:(\d+)',  # URL pattern: "http://127.0.0.1:8080"
            ]

            for line in reversed(lines):  # Start from most recent
                for pattern in patterns:
                    match = re.search(pattern, line, re.IGNORECASE)
                    if match:
                        port = int(match.group(1))
                        # Sanity check: port should be in reasonable range
                        if 1024 <= port <= 65535:
                            return port

            return None
        except Exception as e:
            LOGGER.debug(f"Port detection failed for {service_name}: {e}")
            return None

    def _service_host_hint(self, service_name: str) -> str:
        info = SERVICE_TARGETS.get(service_name) or {}
        target_key = info.get("target") or service_name
        base = self.targets.get(target_key) or info.get("endpoint") or DEFAULT_TARGETS.get(target_key) or ""
        try:
            parsed = urllib.parse.urlparse(base)
            return parsed.hostname or "127.0.0.1"
        except Exception:
            return "127.0.0.1"

    def _probe_service_port(self, host: str, port: int, timeout: float = 0.35) -> bool:
        try:
            import socket

            with socket.create_connection((host, port), timeout=timeout):
                return True
        except Exception:
            return False

    def _update_service_ports(self):
        """Update SERVICE_TARGETS with detected ports from running services."""
        for service_name in SERVICE_TARGETS.keys():
            detected_port = self._detect_service_port(service_name)
            if not detected_port:
                continue

            host = self._service_host_hint(service_name)
            if not self._probe_service_port(host, detected_port):
                LOGGER.debug("Skipping port update for %s: %s not listening on %s", service_name, host, detected_port)
                continue

            current_ports = SERVICE_TARGETS[service_name].get("ports", [])
            if detected_port not in current_ports:
                # Port not in whitelist, add it
                SERVICE_TARGETS[service_name]["ports"].append(detected_port)
                LOGGER.info(f"Detected {service_name} running on port {detected_port} (added to whitelist)")

            # Update endpoint to show actual detected port
            current_endpoint = SERVICE_TARGETS[service_name].get("endpoint", "")
            if current_endpoint and f":{detected_port}" not in current_endpoint:
                # Update endpoint to reflect detected port
                base_url = current_endpoint.rsplit(":", 1)[0]  # Remove old port
                SERVICE_TARGETS[service_name]["endpoint"] = f"{base_url}:{detected_port}"
                LOGGER.debug(f"Updated {service_name} endpoint to port {detected_port}")

            # Update router targets/config to match detected port (no hard-coding)
            target_key = SERVICE_TARGETS[service_name].get("target") or service_name
            existing_base = self.targets.get(target_key) or SERVICE_TARGETS[service_name].get("endpoint") or ""
            try:
                parsed = urllib.parse.urlparse(existing_base or f"http://{host}:{detected_port}")
                host = parsed.hostname or host or "127.0.0.1"
                scheme = parsed.scheme or "http"
                new_base = f"{scheme}://{host}:{detected_port}"
                if new_base != existing_base:
                    self.targets[target_key] = new_base
                    self.cfg.setdefault("targets", {})[target_key] = new_base
                    self.config_dirty = True
                    self.ui.update_service_info(service_name, {"endpoint": new_base})
                    LOGGER.info("Aligned target for %s -> %s", target_key, new_base)
            except Exception as exc:
                LOGGER.debug("Failed to align target for %s: %s", target_key, exc)
        self._sync_cloudflared_tunnels()

    def start(self):
        LOGGER.info("Starting services via watchdog")
        self.watchdog.start_all()

        # Detect actual ports services are running on (after startup)
        time.sleep(2)  # Give services time to write to logs
        self._update_service_ports()
        for entry in self.watchdog.get_snapshot():
            self.latest_service_status[entry["name"]] = entry
        self._sync_cloudflared_tunnels()

        for node in self.nodes:
            node.start()
        self._start_control_plane()
        if not self.resolve_sweeper_thread or not self.resolve_sweeper_thread.is_alive():
            self.resolve_sweeper_thread = threading.Thread(
                target=self._sweep_pending_resolves_loop,
                daemon=True,
                name="resolve-sweeper",
            )
            self.resolve_sweeper_thread.start()
        self.status_thread = threading.Thread(target=self._status_monitor, daemon=True)
        self.status_thread.start()
        if self.config_dirty:
            self._save_config()

    def run(self):
        try:
            if self.use_ui:
                self.ui.run()
            else:
                while not self.stop.is_set():
                    time.sleep(1.0)
        except KeyboardInterrupt:
            pass
        finally:
            self.shutdown()

    def shutdown(self):
        if self.stop.is_set():
            return
        self.stop.set()
        LOGGER.info("Shutting down router")
        self.ui.shutdown()
        self.resolve_sweeper_stop.set()
        if self.resolve_sweeper_thread and self.resolve_sweeper_thread.is_alive():
            self.resolve_sweeper_thread.join(timeout=2)
        if self.api_server is not None:
            with contextlib.suppress(Exception):
                self.api_server.shutdown()
            with contextlib.suppress(Exception):
                self.api_server.server_close()
            self.api_server = None
        if self.api_thread and self.api_thread.is_alive():
            self.api_thread.join(timeout=3)
        for node in self.nodes:
            node.stop()
        if self.marketplace_sync_worker and self.marketplace_sync_worker.is_alive():
            self.marketplace_sync_worker.join(timeout=5)
        if self.cloudflared_manager:
            self.cloudflared_manager.shutdown(timeout=3.0)
        self.watchdog.shutdown()
        if self.status_thread and self.status_thread.is_alive():
            self.status_thread.join(timeout=5)
        if not self.config_dirty:
            return
        self._save_config()

    # ──────────────────────────────────────────
    # Naming helpers
    # ──────────────────────────────────────────
    @staticmethod
    def _service_slug(service: str) -> str:
        return Router._service_slug_static(service)

    def _relay_name_for_seed(self, service: str, seed_hex: str) -> str:
        return Router._relay_name_static(service, seed_hex)

    # ──────────────────────────────────────────
    # Assignments and status helpers
    # ──────────────────────────────────────────
    def _ensure_service_relays(self) -> Dict[str, dict]:
        relays = self.cfg.setdefault("service_relays", {})
        assignments = self.cfg.get("service_assignments", {})
        legacy_nodes = {node.get("name"): node for node in self.cfg.get("nodes", []) if node.get("name")}
        changed = False
        now = int(time.time())
        valid_services = {definition.name for definition in ServiceWatchdog.DEFINITIONS}

        for definition in ServiceWatchdog.DEFINITIONS:
            svc = definition.name
            entry = dict(relays.get(svc, {}))
            seed = (entry.get("seed_hex") or "").strip()
            if not seed:
                node_name = assignments.get(svc)
                if node_name and node_name in legacy_nodes:
                    seed = (legacy_nodes[node_name].get("seed_hex") or "").strip()
            if not seed:
                seed = generate_seed_hex()
                changed = True
            normalized_seed = seed.lower().replace("0x", "")
            old_seed = entry.get("seed_hex")
            entry["seed_hex"] = normalized_seed
            new_name = self._relay_name_for_seed(svc, normalized_seed)
            if entry.get("name") != new_name or old_seed != normalized_seed:
                changed = True
            entry["name"] = new_name
            entry.setdefault("created_at", now)
            relays[svc] = entry

        for svc in list(relays.keys()):
            if svc not in valid_services:
                relays.pop(svc, None)
                changed = True

        if changed:
            self.config_dirty = True
        return relays

    def _init_assignments(self) -> Dict[str, str]:
        assignments = self.cfg.setdefault("service_assignments", {})
        changed = False
        for definition in ServiceWatchdog.DEFINITIONS:
            svc = definition.name
            relay_entry = self.service_relays.get(svc, {})
            desired = relay_entry.get("name") or svc
            if assignments.get(svc) != desired:
                assignments[svc] = desired
                changed = True
        for svc in list(assignments.keys()):
            if svc not in self.service_relays:
                assignments.pop(svc, None)
                changed = True
        self.config_dirty = changed
        return assignments

    def _save_config(self):
        try:
            self.cfg["service_relays"] = self.service_relays
            self.cfg["service_assignments"] = {
                svc: self.service_relays.get(svc, {}).get("name") or svc
                for svc in self.service_relays
            }
            self.cfg["nodes"] = [
                {
                    "name": entry.get("name") or svc,
                    "seed_hex": entry.get("seed_hex"),
                }
                for svc, entry in self.service_relays.items()
            ]
            normalized, changed, warnings, errors = _normalize_router_config(self.cfg)
            for warning in warnings:
                LOGGER.warning("Config save migration: %s", warning)
            if errors:
                details = "; ".join(errors)
                raise ValueError(f"config validation failed before save: {details}")
            if changed:
                self.cfg = normalized
            CONFIG_PATH.write_text(json.dumps(normalized, indent=2))
            self.config_dirty = False
            LOGGER.info("Config saved to %s", CONFIG_PATH)
        except Exception as exc:
            LOGGER.warning("Failed to write config %s: %s", CONFIG_PATH, exc)

    def _refresh_node_assignments(self):
        mapping: Dict[str, List[str]] = {node.node_id: [] for node in self.nodes}
        with self.assignment_lock:
            for service, node_id in self.service_assignments.items():
                mapping.setdefault(node_id, []).append(service)
        for node_id, services in mapping.items():
            self.ui.set_node_services(node_id, sorted(services))
        for service in self.service_assignments.keys():
            self._publish_assignment(service)

    def _build_targets_for_service(self, service: str, relay_cfg: dict) -> Dict[str, str]:
        explicit = relay_cfg.get("targets")
        if explicit:
            return {k: v for k, v in explicit.items() if v}
        info = SERVICE_TARGETS.get(service)
        if not info:
            return self.cfg.get("targets", {}).copy()
        target_key = info.get("target")
        base_url = (
            relay_cfg.get("target_url")
            or self.cfg.get("targets", {}).get(target_key)
            or DEFAULT_TARGETS.get(target_key)
        )
        if not base_url:
            raise SystemExit(f"No target URL configured for service '{service}' (expected key '{info.get('target')}')")
        return {alias: base_url for alias in info.get("aliases", [service])}

    def _build_aliases_for_service(self, service: str, relay_cfg: dict) -> List[str]:
        aliases = list(dict.fromkeys(relay_cfg.get("aliases", [])))
        if aliases:
            return aliases
        info = SERVICE_TARGETS.get(service)
        if not info:
            return [service]
        ordered = list(info.get("aliases", []))
        if service not in ordered:
            ordered.append(service)
        return list(dict.fromkeys(ordered))

    def _create_relay_node(self, service: str, relay_cfg: dict) -> RelayNode:
        node_cfg = dict(relay_cfg)
        node_cfg["name"] = relay_cfg.get("name") or self._relay_name_for_seed(service, relay_cfg.get("seed_hex") or '')
        node_cfg["primary_service"] = service
        node_cfg["targets"] = self._build_targets_for_service(service, relay_cfg)
        node_cfg["aliases"] = self._build_aliases_for_service(service, relay_cfg)
        return RelayNode(
            node_cfg,
            self.cfg,
            self.ui,
            self.lookup_assignment,
            self._update_node_address,
            rate_limit_callback=self._on_rate_limit,
            router_event_handler=self._handle_router_nkn_event,
            nkn_traffic_callback=self._on_node_nkn_traffic,
            endpoint_usage_callback=self._record_endpoint_usage,
        )

    def _on_rate_limit(self, service: str, node_id: str) -> bool:
        state = self.rate_limit_state.setdefault(service, {"pending": False})
        if state.get("pending"):
            return False
        state["pending"] = True
        LOGGER.warning("Service %s on relay %s experiencing sustained 429 responses; rotating seed", service, node_id)
        threading.Thread(target=self._rotate_service_address, args=(service,), daemon=True).start()
        return True

    def _rotate_service_address(self, service: str) -> None:
        node = self.service_nodes.get(service)
        state = self.rate_limit_state.setdefault(service, {})
        try:
            if not node:
                return
            LOGGER.info("Rotating relay seed for service %s", service)
            node.stop()
            old_node_id = node.node_id
            with self.assignment_lock:
                self.node_addresses.pop(old_node_id, None)
            self.nodes = [n for n in self.nodes if n is not node]
            self.node_map.pop(old_node_id, None)
            entry = dict(self.service_relays.get(service, {}))
            new_seed = generate_seed_hex()
            entry["seed_hex"] = new_seed.lower().replace("0x", "")
            entry["name"] = self._relay_name_for_seed(service, entry["seed_hex"])
            entry["rotated_at"] = int(time.time())
            self.service_relays[service] = entry
            new_node = self._create_relay_node(service, entry)
            self.nodes.append(new_node)
            self.node_map[new_node.node_id] = new_node
            self.service_nodes[service] = new_node
            self.node_addresses[new_node.node_id] = None
            with self.assignment_lock:
                self.service_assignments[service] = new_node.node_id
            new_node.start()
            self._refresh_node_assignments()
            self.config_dirty = True
            self._save_config()
        finally:
            state["pending"] = False

    def _publish_assignment(self, service: str):
        status = self.latest_service_status.get(service, {})
        with self.assignment_lock:
            node_id = self.service_assignments.get(service)
        addr = self.node_addresses.get(node_id)
        info = dict(status)
        running = info.get("running")
        info["status"] = info.get("status") or ("running" if running else info.get("last_error") or "stopped")
        info.update({
            "assigned_node": node_id,
            "assigned_addr": addr,
        })
        self.ui.update_service_info(service, info)

    def _status_monitor(self):
        port_detection_counter = 0
        while not self.stop.is_set():
            try:
                snapshot = self.watchdog.get_snapshot()
                for entry in snapshot:
                    self.latest_service_status[entry["name"]] = entry
                    self._publish_assignment(entry["name"])

                # Update service ports every 30 seconds (5s * 6 iterations)
                port_detection_counter += 1
                if port_detection_counter >= 6:
                    self._update_service_ports()
                    port_detection_counter = 0
                self._sync_cloudflared_tunnels()
                self._maybe_start_marketplace_sync()
                self._sample_telemetry()
            except Exception as exc:
                LOGGER.debug("Status monitor error: %s", exc)
            time.sleep(5)

    def _update_node_address(self, node_id: str, addr: Optional[str]):
        self.node_addresses[node_id] = addr
        self._refresh_node_assignments()

    # ──────────────────────────────────────────
    # Assignment lookup & UI actions
    # ──────────────────────────────────────────
    def lookup_assignment(self, service_name: str):
        with self.assignment_lock:
            if service_name == "__map__":
                result = {}
                for svc, node_id in self.service_assignments.items():
                    result[svc] = {
                        "node": node_id,
                        "addr": self.node_addresses.get(node_id),
                    }
                return result
            node_id = self.service_assignments.get(service_name)
        addr = self.node_addresses.get(node_id)
        return (node_id, addr)

    def handle_ui_action(self, action: dict):
        typ = action.get("type")
        if typ == "service":
            op = action.get("op")
            service = action.get("service")
            if op == "cycle":
                self._cycle_service(service)
            elif op == "diagnostics" and service:
                LOGGER.info("Diagnostics requested for %s", service)
                # Placeholder: extend with real health checks or request simulations.
                status = self.latest_service_status.get(service, {})
                LOGGER.info("Current status: %s", json.dumps(self._redact_public_payload(status), default=str))
        elif typ == "service_toggle":
            service = action.get("service")
            enabled = bool(action.get("enabled", True))
            if service:
                self.ui.service_config[service] = enabled
                if enabled:
                    LOGGER.info("Enabling service %s", service)
                    self.watchdog.start_service(service)
                else:
                    LOGGER.info("Disabling service %s", service)
                    self.watchdog.stop_service(service)
                self.config_dirty = True
        elif typ == "node" and action.get("op") == "diagnostics":
            node = action.get("node")
            LOGGER.info("Diagnostics requested for node %s", node)
        elif typ == "daemon":
            if action.get("op") == "enable":
                info = self.daemon_mgr.enable(BASE_DIR, CONFIG_PATH)
                self.daemon_info = info
                self.ui.set_daemon_info(info)
                LOGGER.info("Daemon sentinel created at %s", info.get("path"))
            elif action.get("op") == "disable":
                self.daemon_mgr.disable()
                self.daemon_info = None
                self.ui.set_daemon_info(None)
                LOGGER.info("Daemon sentinel removed")
        elif typ == "config":
            key = action.get("key")
            if key == "chunk_upload_kb":
                try:
                    kb = max(4, int(action.get("value")))
                    self.cfg.setdefault("http", {})["chunk_upload_b"] = kb * 1024
                    for node in self.nodes:
                        node.chunk_upload_b = kb * 1024
                    self.ui.set_chunk_upload_kb(kb)
                    self.config_dirty = True
                    LOGGER.info("Updated chunk_upload_b to %dkB", kb)
                except Exception as exc:
                    LOGGER.warning("Failed to update chunk_upload_kb: %s", exc)
        elif typ == "port_isolation":
            enabled = bool(action.get("enabled", True))
            self.ui.port_isolation_enabled = enabled
            self.config_dirty = True
            LOGGER.info("Port isolation %s", "enabled" if enabled else "disabled")

    def _cycle_service(self, service: Optional[str]):
        if not service or service not in self.latest_service_status:
            return
        with self.assignment_lock:
            node_ids = [node.node_id for node in self.nodes]
            if not node_ids:
                return
            current = self.service_assignments.get(service)
            if current in node_ids:
                idx = node_ids.index(current)
                new_id = node_ids[(idx + 1) % len(node_ids)]
            else:
                new_id = node_ids[0]
            if self.service_assignments.get(service) == new_id:
                return
            self.service_assignments[service] = new_id
            self.config_dirty = True
        LOGGER.info("Reassigned %s to %s", service, new_id)
        self._refresh_node_assignments()
        self._save_config()



# ──────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────
def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Unified NKN relay router")
    ap.add_argument("--config", default=str(CONFIG_PATH), help="Path to router_config.json")
    ap.add_argument("--no-ui", action="store_true", help="Disable curses dashboard")
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    global CONFIG_PATH
    CONFIG_PATH = Path(args.config).resolve()
    cfg = load_config()
    router = Router(cfg, use_ui=not args.no_ui)
    router.start()
    router.run()


if __name__ == "__main__":
    main()
