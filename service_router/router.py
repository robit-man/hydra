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
import base64
import codecs
import contextlib
import json
import logging
import math
import os
import queue
import secrets
import shlex
import shutil
import signal
import subprocess
import sys
import threading
import time
import urllib.parse
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
    for mod in ("requests", "python-dotenv", "qrcode"):
        try:
            if mod == "python-dotenv":
                __import__("dotenv")
            else:
                __import__(mod)
        except Exception:
            need.append(mod)
    if need:
        subprocess.check_call([str(PIP_BIN), "install", *need], cwd=BASE_DIR)


if not _in_venv():
    _ensure_venv()
    os.execv(str(PY_BIN), [str(PY_BIN), *sys.argv])

_ensure_deps()

import requests  # type: ignore
import qrcode  # type: ignore

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


@dataclass
class ServiceDefinition:
    name: str
    repo_url: str
    script_path: str
    description: str
    preserve_repo: bool = False  # If True, keep full repo structure instead of extracting only script
    default_stream: bool = False  # If True, prefer streaming responses (chunks) by default

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

    def snapshot(self) -> Dict[str, object]:
        running = (self.process is not None and self.process.poll() is None) or self.fallback_mode
        if self.fallback_mode:
            status = "system fallback"
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
            "terminal_alive": self.terminal_proc is not None and self.terminal_proc.poll() is None,
            "fallback": self.fallback_mode,
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
        ),
        ServiceDefinition(
            name="whisper_asr",
            repo_url="https://github.com/robit-man/whisper-asr-service.git",
            script_path="asr/asr_service.py",
            description="Whisper ASR streaming/batch REST service",
        ),
        ServiceDefinition(
            name="ollama_farm",
            repo_url="https://github.com/robit-man/ollama-nkn-relay.git",
            script_path="farm/ollama_farm.py",
            description="Ollama parallel proxy with concurrency guard",
        ),
        ServiceDefinition(
            name="mcp_server",
            repo_url="https://github.com/robit-man/hydra-mcp-server.git",
            script_path="mcp_server/mcp_service.py",
            description="Hydra MCP context server with WebSocket + REST APIs",
        ),
        ServiceDefinition(
            name="web_scrape",
            repo_url="https://github.com/robit-man/web-scrape-service.git",
            script_path="scrape/web_scrape.py",
            description="Headless Chrome scrape/control service",
        ),
        ServiceDefinition(
            name="depth_any",
            repo_url="https://github.com/robit-man/Depth-Anything-3.git",
            script_path="app.py",
            description="Depth Anything 3 depth estimation and pointcloud generation",
            preserve_repo=True,  # Requires full repo structure for dependencies
            default_stream=True,  # Responses can be large (pointcloud) — stream by default
        ),
    ]

    def __init__(self, base_dir: Optional[Path] = None, enable_logs: bool = True):
        self.base_dir = Path(base_dir or BASE_DIR)
        self.enable_logs = enable_logs
        SERVICES_ROOT.mkdir(parents=True, exist_ok=True)
        LOGS_ROOT.mkdir(parents=True, exist_ok=True)
        METADATA_ROOT.mkdir(parents=True, exist_ok=True)
        self._states: Dict[str, ServiceState] = {}
        self._global_stop = threading.Event()
        self._lock = threading.Lock()
        self._terminal_template = self._detect_terminal()
        self._update_thread: Optional[threading.Thread] = None
        self._repo_thread: Optional[threading.Thread] = None
        self._core_repo_block_reason: Optional[str] = None
        self._restart_pending: bool = False
        if not self._terminal_template:
            print("[watchdog] No terminal emulator found; log windows will not be opened.")

    def ensure_sources(self, service_config: Optional[Dict[str, bool]] = None) -> None:
        if not shutil.which("git"):
            raise SystemExit("git is required for ServiceWatchdog; please install git")

        for definition in self.DEFINITIONS:
            if service_config is not None and not service_config.get(definition.name, True):
                continue
            state = self._prepare_service(definition)
            self._states[definition.name] = state

    def start_all(self) -> None:
        for state in self._states.values():
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
            state.stop_event.clear()
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
                return
            state.stop_event.set()
            proc = state.process
            if proc and proc.poll() is None:
                with contextlib.suppress(Exception):
                    proc.terminate()
                try:
                    proc.wait(timeout=timeout)
                except Exception:
                    with contextlib.suppress(Exception):
                        proc.kill()
            term = state.terminal_proc
            if term and term.poll() is None:
                with contextlib.suppress(Exception):
                    term.terminate()
                try:
                    term.wait(timeout=timeout)
                except Exception:
                    with contextlib.suppress(Exception):
                        term.kill()
            state.process = None
            state.terminal_proc = None
            self._cleanup_terminal_tail(state)
            if state.supervisor and state.supervisor.is_alive():
                state.supervisor.join(timeout=timeout)
            state.running_since = None

    def shutdown(self, timeout: float = 15.0) -> None:
        self._global_stop.set()
        if self._update_thread and self._update_thread.is_alive():
            self._update_thread.join(timeout=timeout)
        if self._repo_thread and self._repo_thread.is_alive():
            self._repo_thread.join(timeout=timeout)
        for state in self._states.values():
            state.stop_event.set()
            proc = state.process
            if proc and proc.poll() is None:
                with contextlib.suppress(Exception):
                    proc.terminate()
                try:
                    proc.wait(timeout=timeout)
                except Exception:
                    with contextlib.suppress(Exception):
                        proc.kill()
            term = state.terminal_proc
            if term and term.poll() is None:
                with contextlib.suppress(Exception):
                    term.terminate()
                try:
                    term.wait(timeout=timeout)
                except Exception:
                    with contextlib.suppress(Exception):
                        term.kill()
            state.terminal_proc = None
            self._cleanup_terminal_tail(state)
            if state.supervisor and state.supervisor.is_alive():
                state.supervisor.join(timeout=timeout)

    def get_snapshot(self) -> List[Dict[str, object]]:
        return [state.snapshot() for state in self._states.values()]

    # internal helpers -------------------------------------------------
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
            shutil.copytree(repo_dir, backup_dir, dirs_exist_ok=False)
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
            try:
                if state.definition.name == "ollama_farm":
                    if self._handle_ollama(state):
                        state.restart_attempts = 0
                        time.sleep(5)
                        continue
                else:
                    if self._manage_standard_service(state):
                        state.restart_attempts = 0
                        continue
            except Exception as exc:
                state.last_error = str(exc)
                state.last_exit_at = time.time()
                state.process = None
                self._close_log(state)
                state.last_exit_code = None
                state.restart_count += 1
                time.sleep(min(backoff, 60.0))
                backoff = min(backoff * 2.0, 60.0)
                state.restart_attempts += 1
                continue
            state.restart_count += 1
            state.restart_attempts += 1
            if state.definition.name != "ollama_farm" and state.restart_attempts == 1:
                self._free_ports(self._service_ports(state.definition.name))
                continue
            if state.restart_attempts <= 2:
                time.sleep(min(backoff, 60.0))
                backoff = min(backoff * 2.0, 60.0)
                continue
            state.last_error = state.last_error or "Repeated startup failures"
            state.process = None
            state.running_since = None
            self._close_log(state)
            break
        state.running_since = None

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
        state.running_since = time.time()
        state.last_error = None
        log_file.write(f"\n[{time.strftime('%Y-%m-%d %H:%M:%S')}] watchdog: started {cmd}\n")
        log_file.flush()
        self._ensure_terminal_tail(state)

    def _handle_ollama(self, state: ServiceState) -> bool:
        if state.fallback_mode:
            if self._ollama_health_ok():
                state.last_error = None
            else:
                state.last_error = "system ollama unhealthy"
            return True

        if self._ollama_health_ok():
            state.fallback_mode = True
            state.last_error = None
            state.running_since = time.time()
            return True

        self._free_ports([11434, 8080])
        self._start_process(state)
        proc = state.process
        if not proc:
            state.last_error = "ollama_farm failed to spawn"
            return False
        ready = self._wait_for_ollama_health(timeout=20)
        if ready:
            state.last_error = None
            state.restart_attempts = 0
            return True

        state.last_error = "ollama_farm failed to start; falling back"
        self._terminate_process(state)
        if self._ollama_health_ok():
            state.fallback_mode = True
            state.running_since = time.time()
            state.process = None
            self._close_log(state)
            state.restart_attempts = 0
            return True
        state.last_error = "ollama fallback unavailable"
        return False

    def _manage_standard_service(self, state: ServiceState) -> bool:
        service_ports = self._service_ports(state.definition.name)
        if any(self._port_in_use(p) for p in service_ports if p > 0):
            self._free_ports(service_ports)
        self._start_process(state)
        proc = state.process
        if not proc:
            state.last_error = "spawn failed"
            return False
        ret = proc.wait()
        state.last_exit_code = ret
        state.last_exit_at = time.time()
        state.process = None
        self._close_log(state)
        if state.stop_event.is_set() or self._global_stop.is_set():
            return True
        state.last_error = f"Exited with code {ret}"
        return False

    def _service_ports(self, service_name: str) -> List[int]:
        if service_name == "piper_tts":
            return [8123]
        if service_name == "whisper_asr":
            return [8126]
        if service_name == "ollama_farm":
            return [11434, 8080]
        if service_name == "mcp_server":
            return [9003]
        if service_name == "web_scrape":
            return [8130]
        if service_name == "depth_any":
            return [5000]
        return []

    def _terminate_process(self, state: ServiceState) -> None:
        proc = state.process
        if proc and proc.poll() is None:
            with contextlib.suppress(Exception):
                proc.terminate()
            try:
                proc.wait(timeout=5)
            except Exception:
                with contextlib.suppress(Exception):
                    proc.kill()
        state.process = None
        self._close_log(state)

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

    def _free_ports(self, ports: List[int]) -> None:
        for port in ports:
            if port <= 0:
                continue
            if not self._port_in_use(port):
                continue
            pids = self._find_pids_on_port(port)
            for pid in pids:
                with contextlib.suppress(Exception):
                    os.kill(pid, signal.SIGTERM)
            time.sleep(0.2)
            if self._port_in_use(port):
                pids = self._find_pids_on_port(port)
                for pid in pids:
                    with contextlib.suppress(Exception):
                        os.kill(pid, signal.SIGKILL)
                time.sleep(0.2)

    def _port_in_use(self, port: int) -> bool:
        import socket

        with contextlib.closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
            sock.settimeout(0.2)
            try:
                return sock.connect_ex(("127.0.0.1", port)) == 0
            except Exception:
                return False

    def _find_pids_on_port(self, port: int) -> List[int]:
        pids: List[int] = []
        if shutil.which("lsof"):
            try:
                out = subprocess.check_output(["lsof", "-ti", f":{port}"], text=True)
                for line in out.splitlines():
                    with contextlib.suppress(Exception):
                        pids.append(int(line.strip()))
            except subprocess.CalledProcessError:
                pass
        elif shutil.which("fuser"):
            try:
                out = subprocess.check_output(["fuser", "-n", "tcp", str(port)], text=True)
                for token in out.split():
                    with contextlib.suppress(Exception):
                        pids.append(int(token))
            except subprocess.CalledProcessError:
                pass
        return pids

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


def _default_config() -> dict:
    service_relays = {}
    nodes = []
    assignments = {}
    now = int(time.time())
    targets = dict(DEFAULT_TARGETS)

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
        "schema": 1,
        "targets": targets,
        "http": {
            "workers": 4,
            "max_body_b": 2 * 1024 * 1024,
            "verify_default": True,
            "chunk_raw_b": 12 * 1024,
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
        "service_relays": service_relays,
        "nodes": nodes,
        "service_assignments": assignments,
    }


def load_config() -> dict:
    if not CONFIG_PATH.exists():
        CONFIG_PATH.write_text(json.dumps(_default_config(), indent=2))
        print(f"→ wrote default config {CONFIG_PATH}")
    cfg = json.loads(CONFIG_PATH.read_text())
    if "schema" not in cfg:
        cfg["schema"] = 1
    # ensure targets exist
    cfg.setdefault("targets", DEFAULT_TARGETS.copy())
    http = cfg.setdefault("http", {})
    http.setdefault("workers", 4)
    http.setdefault("max_body_b", 2 * 1024 * 1024)
    http.setdefault("verify_default", True)
    http.setdefault("chunk_raw_b", 12 * 1024)
    http.setdefault("chunk_upload_b", 600 * 1024)
    http.setdefault("heartbeat_s", 10)
    http.setdefault("batch_lines", 24)
    http.setdefault("batch_latency", 0.08)
    http.setdefault("retries", 4)
    http.setdefault("retry_backoff", 0.5)
    http.setdefault("retry_cap", 4.0)
    bridge = cfg.setdefault("bridge", {})
    bridge.setdefault("num_subclients", 2)
    bridge.setdefault("seed_ws", "")
    bridge.setdefault("self_probe_ms", 12000)
    bridge.setdefault("self_probe_fails", 3)
    nodes = cfg.setdefault("nodes", [])
    if not nodes:
        nodes.extend(_default_config()["nodes"])
    cfg.setdefault("service_assignments", {})
    return cfg


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
            src_short = src if len(src) <= max_label else src[: max_label - 1] + "…"
            tgt_short = tgt if len(tgt) <= max_label else tgt[: max_label - 1] + "…"

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
                 rate_limit_callback: Optional[Callable[[str, str], bool]] = None):
        self.cfg = node_cfg
        self.global_cfg = global_cfg
        self.ui = ui
        self.node_id = node_cfg.get("name") or node_cfg.get("id") or secrets.token_hex(4)
        self.ui.add_node(self.node_id, self.node_id)
        explicit_targets = node_cfg.get("targets") or {}
        if explicit_targets:
            self.targets = {k: v for k, v in explicit_targets.items() if v}
        else:
            self.targets = global_cfg.get("targets", {}).copy()
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
        self.ui.bump(self.node_id, "IN", f"{event or '<unknown>'} {rid}")
        self._record_flow(src, self.node_id, f"{event or '<unknown>'} {rid}", service=coarse_service, channel=src)
        if event in ("relay.ping", "ping"):
            self.bridge.dm(src, {"event": "relay.pong", "ts": int(time.time() * 1000), "addr": self.bridge.addr})
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
            self.bridge.dm(src, info)
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
            self.bridge.dm(src, {
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
            self.bridge.dm(src, payload, DM_OPTS_SINGLE)
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
                if blocked:
                    target_label = self._flow_target_label(service_name, req.get("url") or req.get("path") or service_name or "")
                    method = (req.get("method") or "GET").upper()
                    path_snippet = req.get("path") or "/"
                    self._record_flow(src or "client", target_label, f"BLOCKED {method} {path_snippet}: {e}", service=service_name, channel=src, blocked=True)
                self.bridge.dm(src, {
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
        svc_def = None
        if service_name:
            svc_def = next((d for d in self.DEFINITIONS if d.name == service_name), None)

        target_label = self._flow_target_label(service_name, url)
        path_snippet = urllib.parse.urlparse(url).path or "/"
        self._record_flow(src or "client", target_label, f"EGRESS {method} {path_snippet} {rid}", service=service_name, channel=src)

        want_stream = False
        stream_mode = str(req.get("stream") or headers.get("X-Relay-Stream") or "").strip().lower()
        if stream_mode in ("1", "true", "yes", "on", "chunks", "dm", "lines", "ndjson", "sse", "events"):
            want_stream = True
        if svc_def and svc_def.default_stream:
            want_stream = True
            if "X-Relay-Stream" not in headers and "x-relay-stream" not in headers:
                headers["X-Relay-Stream"] = "chunks"

        params: Dict[str, Any] = {"headers": headers, "timeout": timeout_s, "verify": verify}
        body_bytes = 0
        body_chunks = req.get("body_chunks_b64") or []
        json_chunks = req.get("json_chunks_b64") or []
        if body_chunks:
            try:
                combined = b"".join(base64.b64decode(str(c), validate=False) for c in body_chunks if c is not None)
            except Exception:
                combined = b""
            params["data"] = combined
            body_bytes = len(combined)
        elif json_chunks:
            try:
                combined = b"".join(base64.b64decode(str(c), validate=False) for c in json_chunks if c is not None)
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
                body_bytes = len(req["data"]) if isinstance(req["data"], (bytes, bytearray)) else len(str(req["data"]).encode("utf-8"))
            except Exception:
                body_bytes = 0

        if want_stream:
            resp = self._http_request_with_retry(session, method, url, stream=True, **params)
            if resp.status_code == 429:
                self._register_rate_limit_hit()
                self._send_simple_response(src, rid, resp, req)
                self._record_usage_stats(service_name, src, bytes_in=body_bytes, bytes_out=len(resp.content or b""), start_ts=start_ts)
                return
            self._reset_rate_limit()
            stream_mode = self._infer_stream_mode(stream_mode, resp)
            bytes_out = self._handle_stream(src, rid, resp, stream_mode, service_name, body_bytes, start_ts)
            self._record_flow(target_label, src or "client", f"STREAM {resp.status_code} {method} {path_snippet} {rid}", service=service_name, channel=src, direction="←")
            self._record_usage_stats(service_name, src, bytes_in=body_bytes, bytes_out=bytes_out, start_ts=start_ts)
            return

        resp = self._http_request_with_retry(session, method, url, **params)
        self._handle_response_status(resp.status_code)
        bytes_out = self._send_simple_response(src, rid, resp, req)
        self._record_flow(target_label, src or "client", f"RESP {resp.status_code} {method} {path_snippet} {rid}", service=service_name, channel=src, direction="←")
        self._record_usage_stats(service_name, src, bytes_in=body_bytes, bytes_out=bytes_out, start_ts=start_ts)

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
        self.bridge.dm(src, payload, DM_OPTS_SINGLE)
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
        self.bridge.dm(src, payload, DM_OPTS_SINGLE)
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
            self.bridge.dm(src, payload, DM_OPTS_STREAM)

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
        self.bridge.dm(src, payload, DM_OPTS_SINGLE)
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
        self.bridge.dm(src, begin_payload, DM_OPTS_STREAM)
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

        def flush_batch():
            nonlocal batch, last_flush
            if not batch:
                return
            payload = {
                "event": "relay.response.lines",
                "id": rid,
                "lines": batch,
            }
            self.bridge.dm(src, payload, DM_OPTS_STREAM)
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
                        batch.append({"seq": seq, "ts": int(time.time() * 1000), "line": line})
                        if len(batch) >= self.batch_lines or (time.time() - last_flush) >= self.batch_latency:
                            flush_batch()
                if time.time() >= hb_deadline:
                    self.bridge.dm(src, {"event": "relay.response.keepalive", "id": rid, "ts": int(time.time() * 1000)}, DM_OPTS_STREAM)
                    hb_deadline = time.time() + self.heartbeat_s
            tail = decoder.decode(b"", final=True)
            if tail.strip():
                seq += 1
                total_lines += 1
                batch.append({"seq": seq, "ts": int(time.time() * 1000), "line": tail})
            flush_batch()
        except Exception as e:
            self.bridge.dm(src, {
                "event": "relay.response.end",
                "id": rid,
                "ok": False,
                "bytes": total_bytes,
                "last_seq": seq,
                "lines": total_lines,
                "error": f"{type(e).__name__}: {e}",
                "done_seen": done_seen,
            }, DM_OPTS_STREAM)
            self.ui.bump(self.node_id, "ERR", f"stream lines {e}")
            return
            self.bridge.dm(src, {
                "event": "relay.response.end",
                "id": rid,
                "ok": True,
                "bytes": total_bytes,
                "last_seq": seq,
                "lines": total_lines,
                "done_seen": done_seen,
            }, DM_OPTS_STREAM)
            self.ui.bump(self.node_id, "OUT", f"stream end {rid}")
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
                        self.bridge.dm(src, {"event": "relay.response.keepalive", "id": rid, "ts": int(time.time() * 1000)}, DM_OPTS_STREAM)
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
                self.bridge.dm(src, payload, DM_OPTS_STREAM)
                last_send = time.time()
        except Exception as e:
            self.bridge.dm(src, {
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
        self.bridge.dm(src, {
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
        self.cfg = cfg
        self.use_ui = use_ui
        self.ui = EnhancedUI(use_ui, STATE_CONFIG_PATH)
        self.ui.set_action_handler(self.handle_ui_action)
        ensure_bridge()
        http_cfg = self.cfg.get("http", {})
        if http_cfg:
            kb = int(http_cfg.get("chunk_upload_b", 600 * 1024) // 1024)
            self.ui.set_chunk_upload_kb(kb)

        # Local copy of target endpoints (defaults merged with config)
        self.targets: Dict[str, str] = dict(DEFAULT_TARGETS)
        self.targets.update(self.cfg.get("targets", {}))

        self.watchdog = ServiceWatchdog(BASE_DIR)
        self.watchdog.ensure_sources(service_config=self.ui.service_config)
        self.latest_service_status: Dict[str, dict] = {
            snap["name"]: snap for snap in self.watchdog.get_snapshot()
        }

        self.assignment_lock = threading.Lock()
        self.config_dirty = False
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

    def start(self):
        LOGGER.info("Starting services via watchdog")
        self.watchdog.start_all()

        # Detect actual ports services are running on (after startup)
        time.sleep(2)  # Give services time to write to logs
        self._update_service_ports()

        for node in self.nodes:
            node.start()
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
        for node in self.nodes:
            node.stop()
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
            CONFIG_PATH.write_text(json.dumps(self.cfg, indent=2))
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
        return RelayNode(node_cfg, self.cfg, self.ui, self.lookup_assignment, self._update_node_address, rate_limit_callback=self._on_rate_limit)

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
                LOGGER.info("Current status: %s", json.dumps(status, default=str))
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
