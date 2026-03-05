# Phase 13: btop-Informed Hydra Curses Rebuild

This phase is a full terminal UI architecture rebuild for Hydra's `EnhancedUI`, using btop's panel system, symbol grammar, navigation model, and dark-theme strategy as implementation ground truth.

## Goals

- Replace all ad-hoc borders with a uniform, thick border system.
- Add a left-to-right halftone gradient model for border rendering based on X-position percentage.
- Rebuild layout composition into a panel system with clear section ownership.
- Rebuild navigation and overlay/menu behavior to match btop-like interaction patterns.
- Consolidate runtime logs into a dedicated bottom dock and stop stray stdout from corrupting curses rendering.

## btop Support References

- Border/symbol grammar:
  - `/home/robit/Documents/repositories/btop/src/btop_draw.hpp:33`
  - `/home/robit/Documents/repositories/btop/src/btop_draw.hpp:45`
- Box constructor pattern:
  - `/home/robit/Documents/repositories/btop/src/btop_draw.cpp:252`
- Halftone/TTY symbol ramp:
  - `/home/robit/Documents/repositories/btop/src/btop_draw.cpp:91`
- Dynamic panel layout sizing:
  - `/home/robit/Documents/repositories/btop/src/btop_draw.cpp:2396`
  - `/home/robit/Documents/repositories/btop/src/btop_draw.cpp:2498`
- Menu state machine and overlay lifecycle:
  - `/home/robit/Documents/repositories/btop/src/btop_menu.cpp:1801`
  - `/home/robit/Documents/repositories/btop/src/btop_menu.cpp:1814`
- Input/navigation conventions and vim aliases:
  - `/home/robit/Documents/repositories/btop/src/btop_input.cpp:218`
  - `/home/robit/Documents/repositories/btop/src/btop_input.cpp:513`
- Dark baseline and divider tonal strategy:
  - `/home/robit/Documents/repositories/btop/src/btop_theme.cpp:51`
  - `/home/robit/Documents/repositories/btop/src/btop_theme.cpp:102`

## Hydra Primary Touchpoints

- `/home/robit/Documents/repositories/hydra/service_router/router.py:3288`
- `/home/robit/Documents/repositories/hydra/service_router/router.py:3613`
- `/home/robit/Documents/repositories/hydra/service_router/router.py:3740`
- `/home/robit/Documents/repositories/hydra/service_router/router.py:3753`
- `/home/robit/Documents/repositories/hydra/service_router/router.py:3889`
- `/home/robit/Documents/repositories/hydra/service_router/router.py:4360`
- `/home/robit/Documents/repositories/hydra/service_router/router.py:4486`
- `/home/robit/Documents/repositories/hydra/service_router/router.py:966`
- `/home/robit/Documents/repositories/hydra/service_router/router.py:1227`
- `/home/robit/Documents/repositories/hydra/service_router/router.py:7325`

## Workorders

- `WO-13.1-border-halftone-system.md`
- `WO-13.2-layout-and-panel-grid.md`
- `WO-13.3-navigation-and-menu-overlays.md`
- `WO-13.4-view-content-recomposition.md`
- `WO-13.5-log-dock-and-output-isolation.md`
- `WO-13.6-qa-and-rollout-gates.md`

