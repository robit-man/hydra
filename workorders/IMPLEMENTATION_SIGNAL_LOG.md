# Implementation Signal Log

This log is a compact running record of implementation progress so later sessions can recover intent and status quickly.

## 2026-03-04 - HUD Dock Rebuild (Hydra Frontend)

### Scope
- Replace legacy header-triggered sidebars with HUD-aligned dock rails + panel stacks.
- Preserve existing feature hooks while re-homing controls into nested panel sections.
- Persist dock/panel state to avoid UI regressions across reloads.

### Files changed
- `docs/index.html`
- `docs/style.css`
- `docs/js/main.js`
- `docs/js/config.js`
- `docs/js/graph.js`
- `docs/js/ui/dockController.js`
- `docs/js/ui/sectionState.js`

### Implemented
- Added `leftDock`/`rightDock` structures with dedicated control rails and dock panels.
- Added dock backdrop + escape/close behavior for clean open/close lifecycle.
- Added collapsible nested sections with persisted collapse state per panel.
- Added persisted dock state (`uiDockState`) and section state (`uiSectionState`) in config.
- Added workspace KPI counters (nodes/links/selected/flows) and node catalog filtering.
- Added collaboration KPI counters wired to Hydra/NoClip peer badges.
- Added right-dock market/policy/router indicator tone syncing.
- Added graph API `getWorkspaceStats()` for panel KPI rendering.
- Completed responsive CSS conversion to dock-native mobile behavior (replacing legacy slide-sidebar references).
- Added mutation-observer based state refresh for peer/market/policy indicators to reduce timer-only updates.

### Remaining validation
- Manual browser verification pass for:
  - Dock open/close state restore after reload.
  - Panel nesting/collapse behavior per section.
  - Mobile layout at <= 640px.
  - Marketplace + owner-auth indicator tone transitions.
- Optional: visual parity polish against NoClip HUD for icon sizing/spacing after live run.

### Known risks / watchpoints
- Existing modules still rely on retained element IDs; future markup edits must preserve these IDs or update bindings.
- Mutation observers are intentionally broad (`subtree: true`) for resilience; if performance drops, narrow observer scope to attributes/text only.

## 2026-03-04 - WO-13.6 QA Matrix + Rollout Gates (Hydra Curses)

### Scope
- Convert phase-13 rollout acceptance from narrative-only docs into executable checks.
- Verify btop-style symbol/border baseline, navigation state machine, log dock isolation, and no-ui fallback path.

### Files changed
- `service_router/tools/hydra_ui_rollout_gate.py`
- `service_router/tests/test_ui_rollout_gate.py`

### Implemented
- Added `hydra_ui_rollout_gate.py` gate runner with five required gates:
  - Gate A: border symbol + halftone baseline.
  - Gate B: overlay/navigation state machine + vim key aliases.
  - Gate C: runtime log dock buffer + scroll bounds.
  - Gate D: watchdog log sink isolation + stdout fallback.
  - Gate E: `--no-ui` activity/stdout fallback behavior.
- Added pytest regression suite `test_ui_rollout_gate.py` mirroring the same gate checks.
- Made rollout gate tool executable for direct invocation from shell/automation.

### Verification evidence
- `python3 service_router/tools/hydra_ui_rollout_gate.py --json`
  - `status: ok`, `ready: true`, all gates passed.
- `python3 -m pytest -q service_router/tests/test_ui_rollout_gate.py`
  - `5 passed`.

### Known risks / watchpoints
- Current gate runner validates structure/behavior in non-curses mode; visual conformance under live curses still needs manual terminal soak checks.
- Test output includes an upstream `pytest-asyncio` deprecation warning unrelated to this workorder.
