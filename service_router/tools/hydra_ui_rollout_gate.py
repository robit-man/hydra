#!/usr/bin/env python3
"""Rollout-gate checks for Hydra's btop-informed curses UI."""

from __future__ import annotations

import argparse
import contextlib
import io
import json
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable, Dict, List

TOOLS_DIR = Path(__file__).resolve().parent
SERVICE_ROUTER_DIR = TOOLS_DIR.parent
if str(SERVICE_ROUTER_DIR) not in sys.path:
    sys.path.insert(0, str(SERVICE_ROUTER_DIR))

import router  # type: ignore


def _now_ms() -> int:
    return int(time.time() * 1000)


@dataclass
class GateResult:
    name: str
    ok: bool
    duration_ms: int
    detail: Dict[str, Any]
    error: str = ""
    required: bool = True

    def as_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "ok": bool(self.ok),
            "duration_ms": int(self.duration_ms),
            "detail": self.detail if isinstance(self.detail, dict) else {},
            "error": str(self.error or ""),
            "required": bool(self.required),
        }


def _mk_ui() -> router.EnhancedUI:
    return router.EnhancedUI(enabled=False, config_path=router.CONFIG_PATH)


def _run_gate(name: str, fn: Callable[[], Dict[str, Any]]) -> GateResult:
    started = _now_ms()
    try:
        detail = fn()
        return GateResult(
            name=name,
            ok=True,
            duration_ms=_now_ms() - started,
            detail=detail,
        )
    except Exception as exc:
        return GateResult(
            name=name,
            ok=False,
            duration_ms=_now_ms() - started,
            detail={},
            error=f"{type(exc).__name__}: {exc}",
        )


def _gate_symbol_border_engine() -> Dict[str, Any]:
    ui = _mk_ui()
    required_symbols = {
        "h",
        "v",
        "tl",
        "tr",
        "bl",
        "br",
        "t_down",
        "t_up",
        "l_right",
        "r_left",
        "cross",
    }
    missing = sorted(required_symbols - set(ui.border_symbols.keys()))
    if missing:
        raise AssertionError(f"missing border symbols: {missing}")
    if len(tuple(ui.border_halftone_h)) < 3:
        raise AssertionError("horizontal halftone ramp is too short")
    if len(tuple(ui.border_halftone_v)) < 3:
        raise AssertionError("vertical halftone ramp is too short")
    if not hasattr(ui, "_draw_panel_box"):
        raise AssertionError("missing _draw_panel_box renderer")
    if not hasattr(ui, "_render_frame_chrome"):
        raise AssertionError("missing _render_frame_chrome renderer")
    return {
        "symbol_count": len(ui.border_symbols),
        "halftone_h": list(ui.border_halftone_h),
        "halftone_v": list(ui.border_halftone_v),
        "layout_min_width": int(ui.layout_min_width),
        "layout_min_height": int(ui.layout_min_height),
    }


def _gate_layout_navigation_state_machine() -> Dict[str, Any]:
    ui = _mk_ui()
    states = {ui.BASE_VIEW, ui.OVERLAY_MENU, ui.OVERLAY_HELP, ui.OVERLAY_CONFIRM}
    missing_states = sorted(states - set(ui._state_keymaps.keys()))
    if missing_states:
        raise AssertionError(f"missing state keymaps: {missing_states}")
    base_map = ui._state_keymaps[ui.BASE_VIEW]
    overlay_map = ui._state_keymaps[ui.OVERLAY_MENU]
    expected_base = {
        ord("h"): "nav.left",
        ord("j"): "nav.down",
        ord("k"): "nav.up",
        ord("l"): "nav.right",
        ord("g"): "nav.home",
        ord("G"): "nav.end",
    }
    expected_overlay = {
        ord("h"): "overlay.back",
        ord("j"): "nav.down",
        ord("k"): "nav.up",
        ord("l"): "activate",
        ord("g"): "nav.home",
        ord("G"): "nav.end",
    }
    for key, command in expected_base.items():
        found = base_map.get(key)
        if found != command:
            raise AssertionError(f"BASE keymap mismatch for {chr(key)!r}: {found!r} != {command!r}")
    for key, command in expected_overlay.items():
        found = overlay_map.get(key)
        if found != command:
            raise AssertionError(f"OVERLAY keymap mismatch for {chr(key)!r}: {found!r} != {command!r}")

    ui._dispatch_command("overlay.menu.open")
    if ui.ui_state != ui.OVERLAY_MENU or not ui.overlay_menu_stack:
        raise AssertionError("overlay.menu.open did not enter menu state")
    ui._dispatch_command("overlay.close")
    if ui.ui_state != ui.BASE_VIEW:
        raise AssertionError("overlay.close did not return to BASE_VIEW")
    ui._dispatch_command("overlay.help.open")
    if ui.ui_state != ui.OVERLAY_HELP:
        raise AssertionError("overlay.help.open did not enter help state")
    ui._dispatch_command("overlay.close")
    if ui.ui_state != ui.BASE_VIEW:
        raise AssertionError("help overlay did not close back to BASE_VIEW")
    return {
        "states": sorted(states),
        "base_view": ui.base_view,
        "menu_stack_depth": len(ui.overlay_menu_stack),
    }


def _gate_log_dock_and_runtime_buffer() -> Dict[str, Any]:
    ui = _mk_ui()
    if ui.log_dock_rows <= 0:
        raise AssertionError("log dock rows disabled")
    base_len = len(ui.runtime_logs)
    ui.append_runtime_log("watchdog", "warn", "line-a\nline-b")
    added = len(ui.runtime_logs) - base_len
    if added < 2:
        raise AssertionError("runtime log append did not preserve multiline entries")
    ui.runtime_log_visible_rows = 1
    ui._scroll_runtime_logs(999)
    max_scroll = ui._runtime_log_max_scroll()
    if not (0 <= ui.runtime_log_scroll <= max_scroll):
        raise AssertionError("runtime log scroll out of bounds")
    return {
        "log_dock_rows": int(ui.log_dock_rows),
        "runtime_logs_len": len(ui.runtime_logs),
        "runtime_log_scroll": int(ui.runtime_log_scroll),
        "runtime_log_max_scroll": int(max_scroll),
    }


def _gate_watchdog_sink_isolation() -> Dict[str, Any]:
    captured: List[tuple[str, str, str]] = []
    sink_target = SimpleNamespace(
        _log_sink=lambda source, level, message: captured.append((str(source), str(level), str(message)))
    )
    router.ServiceWatchdog._emit_runtime_log(sink_target, "warn", "sink-message")
    if captured != [("watchdog", "WARN", "sink-message")]:
        raise AssertionError(f"watchdog sink mismatch: {captured!r}")

    fallback_target = SimpleNamespace(_log_sink=None)
    stdout = io.StringIO()
    with contextlib.redirect_stdout(stdout):
        router.ServiceWatchdog._emit_runtime_log(fallback_target, "info", "fallback-message")
    rendered = stdout.getvalue()
    if "[watchdog] fallback-message" not in rendered:
        raise AssertionError("watchdog stdout fallback not emitted when sink missing")
    return {
        "sink_events": len(captured),
        "stdout_fallback_bytes": len(rendered),
    }


def _gate_no_ui_stdout_path() -> Dict[str, Any]:
    ui = _mk_ui()
    ui.enabled = False
    stdout = io.StringIO()
    with contextlib.redirect_stdout(stdout):
        ui.bump("qa-node", "IN", "qa-message", service="qa-service")
    rendered = stdout.getvalue()
    if "qa-message" not in rendered:
        raise AssertionError("no-ui stdout did not contain activity message")
    if "IN" not in rendered:
        raise AssertionError("no-ui stdout did not include event kind")
    return {
        "stdout_bytes": len(rendered),
        "sample": rendered.strip()[:160],
    }


def run_rollout_gates() -> Dict[str, Any]:
    gates = [
        _run_gate("gate_a_symbol_border_engine", _gate_symbol_border_engine),
        _run_gate("gate_b_layout_navigation_state_machine", _gate_layout_navigation_state_machine),
        _run_gate("gate_c_log_dock_runtime_buffer", _gate_log_dock_and_runtime_buffer),
        _run_gate("gate_d_watchdog_sink_isolation", _gate_watchdog_sink_isolation),
        _run_gate("gate_e_no_ui_stdout_path", _gate_no_ui_stdout_path),
    ]
    required = [gate for gate in gates if gate.required]
    ok = all(gate.ok for gate in required)
    return {
        "status": "ok" if ok else "error",
        "ready": bool(ok),
        "ts_ms": _now_ms(),
        "gates": [gate.as_dict() for gate in gates],
    }


def _print_human(result: Dict[str, Any]) -> None:
    status = str(result.get("status") or "error").upper()
    print(f"[hydra-ui-rollout-gate] status={status}")
    for gate in result.get("gates", []):
        name = str(gate.get("name") or "gate")
        ok = bool(gate.get("ok"))
        duration = int(gate.get("duration_ms") or 0)
        suffix = "OK" if ok else f"FAIL ({gate.get('error')})"
        print(f" - {name}: {suffix} [{duration}ms]")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Hydra btop-style UI rollout-gate checks")
    parser.add_argument("--json", action="store_true", help="Emit full JSON result")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = run_rollout_gates()
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        _print_human(result)
    if str(result.get("status") or "").lower() != "ok":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
