#!/bin/bash
# Verification script for Enhanced UI integration

echo "═══════════════════════════════════════════════════════════"
echo "  Enhanced UI Integration Verification"
echo "═══════════════════════════════════════════════════════════"
echo ""

cd "$(dirname "$0")"

echo "Step 1: Checking Python syntax..."
if python3 -m py_compile router.py 2>/dev/null; then
    echo "  ✓ router.py syntax is valid"
else
    echo "  ✗ Syntax error in router.py"
    exit 1
fi

echo ""
echo "Step 2: Running integration tests..."
if python3 test_enhanced_ui.py 2>&1 | grep -q "ALL TESTS PASSED"; then
    echo "  ✓ All integration tests passed"
else
    echo "  ✗ Integration tests failed"
    echo "  Running tests for details:"
    python3 test_enhanced_ui.py
    exit 1
fi

echo ""
echo "Step 3: Checking stats directory..."
if [ -d ".stats" ]; then
    echo "  ✓ Stats directory exists"
else
    echo "  ℹ Stats directory will be created on first run"
fi

echo ""
echo "Step 4: Verifying documentation..."
docs=(
    "ENHANCED_UI_README.md"
    "IMPLEMENTATION_SUMMARY.md"
    "INTEGRATION_COMPLETE.md"
    "test_enhanced_ui.py"
)

for doc in "${docs[@]}"; do
    if [ -f "$doc" ]; then
        echo "  ✓ $doc"
    else
        echo "  ✗ Missing: $doc"
    fi
done

echo ""
echo "═══════════════════════════════════════════════════════════"
echo "  ✅ VERIFICATION COMPLETE"
echo "═══════════════════════════════════════════════════════════"
echo ""
echo "Enhanced UI is fully integrated and ready to use!"
echo ""
echo "To start the router with Enhanced UI:"
echo "  python3 router.py"
echo ""
echo "To run without UI (for testing):"
echo "  python3 router.py --no-ui"
echo ""
echo "Navigation:"
echo "  ↑/↓ or k/j  - Navigate"
echo "  Enter       - Select"
echo "  Space       - Toggle (Config)"
echo "  S           - Save (Config)"
echo "  ESC         - Back/Close"
echo "  Q           - Quit"
echo ""
