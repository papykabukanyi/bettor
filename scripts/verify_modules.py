"""
Quick Module Verification - No API Calls
=========================================

Verifies that all modules compile and can be imported.
"""

import sys
from pathlib import Path

# Setup path
SRC_DIR = Path(__file__).resolve().parent.parent / "src"
sys.path.insert(0, str(SRC_DIR))

print("\n" + "="*70)
print("QUICK MODULE VERIFICATION")
print("="*70 + "\n")

tests_passed = 0
tests_total = 0

# Test 1: multi_sport_hf_manager
print("1. Testing multi_sport_hf_manager...")
tests_total += 1
try:
    from data.multi_sport_hf_manager import (
        MultiSportHFDataManager,
        get_multi_sport_manager,
        bootstrap_all_sports,
        fetch_and_push_live_games,
    )
    print("   [PASS] All exports available")
    tests_passed += 1
except Exception as e:
    print(f"   [FAIL] {e}")

# Test 2: multi_sport_scheduler
print("\n2. Testing multi_sport_scheduler...")
tests_total += 1
try:
    from data.multi_sport_scheduler import (
        run_multi_sport_bootstrap,
        run_multi_sport_live_fetch,
        get_multi_sport_scheduler_status,
    )
    print("   [PASS] All exports available")
    tests_passed += 1
except Exception as e:
    print(f"   [FAIL] {e}")

# Test 3: unified_sport_fetcher
print("\n3. Testing unified_sport_fetcher...")
tests_total += 1
try:
    from data.unified_sport_fetcher import UnifiedSportFetcher
    print("   [PASS] UnifiedSportFetcher available")
    tests_passed += 1
except Exception as e:
    print(f"   [FAIL] {e}")

# Test 4: hf_uploader
print("\n4. Testing hf_uploader...")
tests_total += 1
try:
    from data.hf_uploader import HFUploader
    print("   [PASS] HFUploader available")
    tests_passed += 1
except Exception as e:
    print(f"   [FAIL] {e}")

# Test 5: Check dashboard modifications
print("\n5. Checking dashboard modifications...")
tests_total += 1
try:
    dashboard_path = SRC_DIR / "dashboard.py"
    with open(dashboard_path) as f:
        content = f.read()
    
    required_items = [
        "from data.multi_sport_scheduler import",
        "run_multi_sport_bootstrap",
        "run_multi_sport_live_fetch",
        "get_multi_sport_scheduler_status",
        'id="multi_sport_live_fetch"',
    ]
    
    all_found = True
    for item in required_items:
        if item in content:
            print(f"   [OK] Found: {item[:50]}")
        else:
            print(f"   [XX] Missing: {item[:50]}")
            all_found = False
    
    if all_found:
        tests_passed += 1
    else:
        print("   [FAIL] Some dashboard items missing")
except Exception as e:
    print(f"   [FAIL] {e}")

# Summary
print("\n" + "="*70)
print(f"RESULTS: {tests_passed}/{tests_total} tests passed")
print("="*70 + "\n")

if tests_passed == tests_total:
    print("[OK] All modules verified successfully!")
    print("[OK] Ready to commit and deploy\n")
    sys.exit(0)
else:
    print(f"[FAIL] {tests_total - tests_passed} verification(s) failed\n")
    sys.exit(1)
