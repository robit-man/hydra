#!/usr/bin/env python3
"""Regression checks for the Phase-13 Hydra curses rollout gates."""

from __future__ import annotations

import contextlib
import io
from pathlib import Path
from types import SimpleNamespace
import sys

SERVICE_ROUTER_DIR = Path(__file__).resolve().parents[1]
if str(SERVICE_ROUTER_DIR) not in sys.path:
    sys.path.insert(0, str(SERVICE_ROUTER_DIR))

import router  # type: ignore


def _mk_ui() -> router.EnhancedUI:
    return router.EnhancedUI(enabled=False, config_path=router.CONFIG_PATH)


def test_gate_a_border_symbol_and_halftone_baseline():
    ui = _mk_ui()
    required = {
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
    assert required.issubset(set(ui.border_symbols.keys()))
    assert len(tuple(ui.border_halftone_h)) >= 3
    assert len(tuple(ui.border_halftone_v)) >= 3
    assert hasattr(ui, "_draw_panel_box")
    assert hasattr(ui, "_render_frame_chrome")


def test_gate_b_navigation_state_machine_and_vim_aliases():
    ui = _mk_ui()
    assert ui.BASE_VIEW in ui._state_keymaps
    assert ui.OVERLAY_MENU in ui._state_keymaps
    assert ui.OVERLAY_HELP in ui._state_keymaps
    assert ui.OVERLAY_CONFIRM in ui._state_keymaps

    base_map = ui._state_keymaps[ui.BASE_VIEW]
    assert base_map[ord("h")] == "nav.left"
    assert base_map[ord("j")] == "nav.down"
    assert base_map[ord("k")] == "nav.up"
    assert base_map[ord("l")] == "nav.right"
    assert base_map[ord("g")] == "nav.home"
    assert base_map[ord("G")] == "nav.end"

    overlay_map = ui._state_keymaps[ui.OVERLAY_MENU]
    assert overlay_map[ord("h")] == "overlay.back"
    assert overlay_map[ord("j")] == "nav.down"
    assert overlay_map[ord("k")] == "nav.up"
    assert overlay_map[ord("l")] == "activate"

    ui._dispatch_command("overlay.menu.open")
    assert ui.ui_state == ui.OVERLAY_MENU
    assert len(ui.overlay_menu_stack) >= 1
    ui._dispatch_command("overlay.close")
    assert ui.ui_state == ui.BASE_VIEW


def test_gate_c_runtime_log_dock_buffer_and_scroll_bounds():
    ui = _mk_ui()
    assert ui.log_dock_rows > 0
    base_len = len(ui.runtime_logs)
    ui.append_runtime_log("watchdog", "warn", "line-1\nline-2")
    assert len(ui.runtime_logs) >= base_len + 2

    ui.runtime_log_visible_rows = 1
    ui._scroll_runtime_logs(999)
    max_scroll = ui._runtime_log_max_scroll()
    assert 0 <= ui.runtime_log_scroll <= max_scroll


def test_gate_d_watchdog_sink_isolation_and_stdout_fallback():
    events = []
    sink_target = SimpleNamespace(
        _log_sink=lambda source, level, message: events.append((source, level, message))
    )
    router.ServiceWatchdog._emit_runtime_log(sink_target, "warn", "sink-message")
    assert events == [("watchdog", "WARN", "sink-message")]

    fallback_target = SimpleNamespace(_log_sink=None)
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        router.ServiceWatchdog._emit_runtime_log(fallback_target, "info", "fallback-message")
    assert "[watchdog] fallback-message" in buf.getvalue()


def test_gate_e_no_ui_stdout_path_remains_functional():
    ui = _mk_ui()
    ui.enabled = False
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        ui.bump("qa-node", "IN", "qa-message", service="qa-service")
    text = buf.getvalue()
    assert "qa-message" in text
    assert "IN" in text
