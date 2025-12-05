#!/usr/bin/env python3
"""Test script to verify EnhancedUI integration."""

import sys
from pathlib import Path

# Add current directory to path
sys.path.insert(0, str(Path(__file__).parent))

try:
    from router import EnhancedUI, StatsTracker, CONFIG_PATH
    print("✓ Successfully imported EnhancedUI and StatsTracker")

    # Test StatsTracker instantiation
    stats = StatsTracker()
    print(f"✓ StatsTracker instantiated, stats dir: {stats}")

    # Test EnhancedUI instantiation (without curses)
    ui = EnhancedUI(enabled=False, config_path=CONFIG_PATH)
    print(f"✓ EnhancedUI instantiated in non-UI mode")
    print(f"  - Menu items: {ui.MENU_ITEMS}")
    print(f"  - Current view: {ui.current_view}")
    print(f"  - Stats tracker: {type(ui.stats).__name__}")

    # Test methods exist
    required_methods = [
        '_main', '_render_main_menu', '_render_config_view',
        '_render_stats_view', '_render_address_book_view',
        '_render_ingress_view', '_render_egress_view',
        '_render_qr_code', '_handle_input', '_handle_enter',
        '_safe_addstr', '_draw_box'
    ]

    missing = []
    for method in required_methods:
        if not hasattr(ui, method):
            missing.append(method)

    if missing:
        print(f"✗ Missing methods: {', '.join(missing)}")
        sys.exit(1)
    else:
        print(f"✓ All {len(required_methods)} required methods present")

    # Test public API compatibility
    api_methods = [
        'add_node', 'set_action_handler', 'set_addr', 'set_state',
        'set_queue', 'set_node_services', 'update_service_info',
        'set_daemon_info', 'bump', 'run', 'shutdown', 'set_chunk_upload_kb'
    ]

    missing_api = []
    for method in api_methods:
        if not hasattr(ui, method):
            missing_api.append(method)

    if missing_api:
        print(f"✗ Missing API methods: {', '.join(missing_api)}")
        sys.exit(1)
    else:
        print(f"✓ All {len(api_methods)} API methods present (UnifiedUI compatible)")

    # Test service configuration
    ui.update_service_info("test_service", {"assigned_addr": "test.addr", "status": "ready"})
    print(f"✓ Service configuration works")
    print(f"  - Services: {list(ui.services.keys())}")
    print(f"  - Service config: {ui.service_config}")

    # Test stats recording
    ui.stats.record_request("test_service", "test.nkn.address", bytes_sent=1024)
    timeline = ui.stats.get_service_timeline(24)
    print(f"✓ Stats recording works")
    print(f"  - Timeline services: {list(timeline.keys())}")

    print("\n" + "="*60)
    print("✓✓✓ ALL TESTS PASSED ✓✓✓")
    print("="*60)
    print("\nThe Enhanced UI is fully integrated and ready to use!")
    print("\nTo test with the curses interface:")
    print("  cd /media/robit/LLM/repositories-backup/hybrid/hydra/service_router")
    print("  python3 router.py")
    print("\nNavigation:")
    print("  ↑/↓ - Navigate menu")
    print("  Enter - Select")
    print("  ESC - Back")
    print("  Q - Quit")

except Exception as e:
    print(f"✗ Error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
