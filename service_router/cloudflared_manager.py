#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Shared cloudflared lifecycle manager for Hydra services."""

from __future__ import annotations

import contextlib
import os
import platform
import re
import shutil
import subprocess
import threading
import time
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Optional


@dataclass
class TunnelRuntime:
    service: str
    target_url: str = ""
    desired: bool = False
    state: str = "inactive"  # inactive | starting | active | stale | error
    active_url: str = ""
    stale_url: str = ""
    last_error: str = ""
    restarts: int = 0
    restart_failures: int = 0
    rate_limited: bool = False
    running: bool = False
    last_start_at: float = 0.0
    last_exit_at: float = 0.0
    next_restart_at: float = 0.0
    pid: Optional[int] = None
    process: Optional[subprocess.Popen] = None


class CloudflaredManager:
    def __init__(self, runtime_root: Path, settings: Optional[Dict[str, Any]] = None, logger: Optional[Callable[[str], None]] = None):
        self.runtime_root = Path(runtime_root)
        self.runtime_root.mkdir(parents=True, exist_ok=True)
        self.settings = dict(settings or {})
        self.logger = logger
        self.lock = threading.Lock()
        self.global_stop = threading.Event()
        self.states: Dict[str, TunnelRuntime] = {}
        self.threads: Dict[str, threading.Thread] = {}
        self.stop_events: Dict[str, threading.Event] = {}
        self.binary_cache: Optional[str] = None
        self.install_attempted = False
        self.install_error = ""
        self._launch_gate = threading.Lock()
        self._last_launch_at: float = 0.0
        self.stagger_seconds: float = max(0.0, float(self.settings.get("stagger_seconds", 8.0)))

        self.auto_install = bool(self.settings.get("auto_install_cloudflared", False))
        self.binary_path = str(self.settings.get("binary_path") or "").strip()
        self.protocol = str(self.settings.get("protocol") or "http2").strip() or "http2"
        self.restart_initial = max(1.0, float(self.settings.get("restart_initial_seconds", 2.0)))
        self.restart_cap = max(self.restart_initial, float(self.settings.get("restart_cap_seconds", 90.0)))

    def _log(self, message: str) -> None:
        text = f"[cloudflared] {message}"
        if callable(self.logger):
            try:
                self.logger(text)
                return
            except Exception:
                pass
        print(text)

    def _state_for(self, service: str) -> TunnelRuntime:
        with self.lock:
            state = self.states.get(service)
            if state is None:
                state = TunnelRuntime(service=service)
                self.states[service] = state
            return state

    def _binary_name(self) -> str:
        return "cloudflared.exe" if os.name == "nt" else "cloudflared"

    def _install_binary(self) -> Optional[str]:
        if self.install_attempted:
            return None
        self.install_attempted = True
        system = platform.system().lower()
        machine = platform.machine().lower()
        url = ""
        if system == "linux":
            if "aarch64" in machine or "arm64" in machine:
                url = "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-arm64"
            elif "arm" in machine:
                url = "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-arm"
            else:
                url = "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64"
        elif system == "windows":
            if "amd64" in machine or "x86_64" in machine:
                url = "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-windows-amd64.exe"
            else:
                url = "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-windows-386.exe"
        else:
            self.install_error = f"unsupported platform for auto-install: {system}/{machine}"
            return None

        target = self.runtime_root / self._binary_name()
        try:
            self._log(f"auto-installing cloudflared from {url}")
            urllib.request.urlretrieve(url, str(target))
            if os.name != "nt":
                target.chmod(0o755)
            self.install_error = ""
            return str(target)
        except Exception as exc:
            self.install_error = str(exc)
            self._log(f"auto-install failed: {exc}")
            with contextlib.suppress(Exception):
                if target.exists():
                    target.unlink()
            return None

    def _discover_binary(self) -> Optional[str]:
        if self.binary_cache:
            if self.binary_cache == "cloudflared":
                return self.binary_cache
            if Path(self.binary_cache).exists():
                return self.binary_cache
            self.binary_cache = None

        if self.binary_path:
            candidate = Path(self.binary_path).expanduser()
            if candidate.exists():
                self.binary_cache = str(candidate)
                return self.binary_cache

        local_candidate = self.runtime_root / self._binary_name()
        if local_candidate.exists():
            self.binary_cache = str(local_candidate)
            return self.binary_cache

        in_path = shutil.which("cloudflared")
        if in_path:
            self.binary_cache = "cloudflared"
            return self.binary_cache

        if self.auto_install:
            installed = self._install_binary()
            if installed:
                self.binary_cache = installed
                return self.binary_cache
        return None

    def set_service_target(self, service: str, target_url: str, enabled: bool = True) -> None:
        target = str(target_url or "").strip()
        state = self._state_for(service)
        with self.lock:
            state.target_url = target
            state.desired = bool(enabled and target)
            if not state.desired:
                state.state = "inactive"
                state.running = False
                state.next_restart_at = 0.0
                proc = state.process
                state.process = None
                state.pid = None
                if proc and proc.poll() is None:
                    with contextlib.suppress(Exception):
                        proc.terminate()
        if state.desired:
            self._ensure_worker(service)
        else:
            stop_ev = self.stop_events.get(service)
            if stop_ev:
                stop_ev.set()

    def clear_service(self, service: str) -> None:
        self.set_service_target(service, "", enabled=False)

    def _ensure_worker(self, service: str) -> None:
        with self.lock:
            thread = self.threads.get(service)
            if thread and thread.is_alive():
                return
            stop_ev = self.stop_events.get(service)
            if stop_ev is None:
                stop_ev = threading.Event()
                self.stop_events[service] = stop_ev
            else:
                stop_ev.clear()
            thread = threading.Thread(target=self._service_loop, args=(service, stop_ev), daemon=True, name=f"cloudflared-{service}")
            self.threads[service] = thread
            thread.start()

    def _next_backoff(self, failures: int, rate_limited: bool) -> float:
        delay = self.restart_initial * (2.0 ** max(0, failures - 1))
        if rate_limited:
            delay *= 2.0
        return min(self.restart_cap, delay)

    def _service_loop(self, service: str, stop_ev: threading.Event) -> None:
        while not self.global_stop.is_set() and not stop_ev.is_set():
            with self.lock:
                state = self.states.get(service)
                target = str(state.target_url if state else "").strip()
                desired = bool(state and state.desired and target)
            if not desired:
                break

            binary = self._discover_binary()
            if not binary:
                with self.lock:
                    state = self.states.get(service)
                    if state:
                        state.state = "error"
                        state.running = False
                        state.last_error = self.install_error or "cloudflared binary not found"
                        state.next_restart_at = time.time() + self.restart_cap
                stop_ev.wait(self.restart_cap)
                continue

            # Stagger launches so we don't hit Cloudflare rate limits
            if self.stagger_seconds > 0:
                with self._launch_gate:
                    elapsed = time.time() - self._last_launch_at
                    wait = self.stagger_seconds - elapsed
                    if wait > 0:
                        self._log(f"{service}: staggering launch ({wait:.1f}s)")
                        stop_ev.wait(wait)
                        if self.global_stop.is_set() or stop_ev.is_set():
                            break
                    self._last_launch_at = time.time()

            cmd = [binary, "tunnel", "--protocol", self.protocol, "--url", target]
            try:
                process = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    bufsize=1,
                )
            except Exception as exc:
                with self.lock:
                    state = self.states.get(service)
                    if state:
                        state.state = "error"
                        state.running = False
                        state.last_error = f"launch failed: {exc}"
                        state.restart_failures += 1
                        delay = self._next_backoff(state.restart_failures, False)
                        state.next_restart_at = time.time() + delay
                stop_ev.wait(delay if 'delay' in locals() else self.restart_initial)
                continue

            with self.lock:
                state = self.states.get(service)
                if state:
                    state.process = process
                    state.pid = int(process.pid or 0) or None
                    state.state = "starting"
                    state.running = True
                    state.last_start_at = time.time()
                    state.next_restart_at = 0.0
                    state.rate_limited = False

            found_url = False
            captured_url = ""
            rate_limited = False
            if process.stdout is not None:
                for raw in iter(process.stdout.readline, ""):
                    if self.global_stop.is_set() or stop_ev.is_set():
                        break
                    line = str(raw or "").strip()
                    if not line:
                        continue
                    lowered = line.lower()
                    if "429 too many requests" in lowered or "error code: 1015" in lowered:
                        rate_limited = True
                    if "trycloudflare.com" in line:
                        match = re.search(r"https://[a-zA-Z0-9-]+\.trycloudflare\.com", line)
                        if not match:
                            match = re.search(r"https://[^\s]+trycloudflare\.com[^\s]*", line)
                        if match:
                            captured_url = match.group(0)
                            found_url = True
                            with self.lock:
                                state = self.states.get(service)
                                if state:
                                    state.active_url = captured_url
                                    state.stale_url = ""
                                    state.state = "active"
                                    state.running = True
                                    state.last_error = ""
                                    state.restart_failures = 0
                                    state.rate_limited = False
            if self.global_stop.is_set() or stop_ev.is_set():
                with contextlib.suppress(Exception):
                    if process.poll() is None:
                        process.terminate()
                        process.wait(timeout=2.0)
                break

            rc = process.poll()
            if rc is None:
                with contextlib.suppress(Exception):
                    process.terminate()
                    process.wait(timeout=2.0)
                rc = process.poll()

            with self.lock:
                state = self.states.get(service)
                if state:
                    state.process = None
                    state.pid = None
                    state.running = False
                    state.last_exit_at = time.time()
                    state.rate_limited = rate_limited
                    state.restarts += 1
                    state.restart_failures += 0 if found_url else 1
                    if found_url and captured_url:
                        state.active_url = ""
                        state.stale_url = captured_url
                    if state.stale_url:
                        state.state = "stale"
                    else:
                        state.state = "error"
                    if found_url:
                        state.last_error = f"cloudflared exited (code {rc}); tunnel became stale"
                    elif rate_limited:
                        state.last_error = f"cloudflared rate-limited before URL (code {rc})"
                    else:
                        state.last_error = f"cloudflared exited before URL (code {rc})"
                    delay = self._next_backoff(max(1, state.restart_failures), rate_limited)
                    state.next_restart_at = time.time() + delay
            stop_ev.wait(delay if 'delay' in locals() else self.restart_initial)

        with self.lock:
            state = self.states.get(service)
            if state:
                proc = state.process
                state.desired = False
                state.process = None
                state.pid = None
                state.running = False
                if state.state == "active":
                    state.state = "stale"
                    state.stale_url = state.active_url or state.stale_url
                    state.active_url = ""
                elif state.state not in ("error", "stale"):
                    state.state = "inactive"
            else:
                proc = None
        if proc and proc.poll() is None:
            with contextlib.suppress(Exception):
                proc.terminate()

    def get_state(self, service: str) -> Dict[str, Any]:
        with self.lock:
            state = self.states.get(service)
            if not state:
                return {}
            return self._state_to_dict(state)

    def _state_to_dict(self, state: TunnelRuntime) -> Dict[str, Any]:
        return {
            "service": state.service,
            "target_url": state.target_url,
            "desired": bool(state.desired),
            "state": state.state,
            "active_url": state.active_url,
            "stale_url": state.stale_url,
            "last_error": state.last_error,
            "restarts": int(state.restarts),
            "restart_failures": int(state.restart_failures),
            "rate_limited": bool(state.rate_limited),
            "running": bool(state.running),
            "last_start_at": float(state.last_start_at),
            "last_exit_at": float(state.last_exit_at),
            "next_restart_at": float(state.next_restart_at),
            "pid": state.pid,
        }

    def snapshot(self) -> Dict[str, Dict[str, Any]]:
        with self.lock:
            return {service: self._state_to_dict(state) for service, state in self.states.items()}

    def shutdown(self, timeout: float = 5.0) -> None:
        self.global_stop.set()
        with self.lock:
            services = list(self.states.keys())
        for service in services:
            self.clear_service(service)
        for stop_ev in list(self.stop_events.values()):
            stop_ev.set()
        for thread in list(self.threads.values()):
            with contextlib.suppress(Exception):
                thread.join(timeout=timeout)
