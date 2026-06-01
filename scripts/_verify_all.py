import os
import subprocess
import sys


ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PYTHON = sys.executable


CHECKS = [
    ("Syntax checks", ["scripts/_check_syntax.py"]),
    ("MLB time normalization regression", ["scripts/_test_mlb_time_normalization.py"]),
    ("Dashboard card bucket regression", ["scripts/_test_dashboard_card_buckets.py"]),
    ("WNBA tomorrow depth regression", ["scripts/_test_wnba_tomorrow_depth.py"]),
    ("Odds cooldown regression", ["scripts/_test_odds_cooldowns.py"]),
    ("Tennis source smoke test", ["scripts/_test_tennis_sources.py"]),
    ("Golf source smoke test", ["scripts/_test_golf_sources.py"]),
]


def _run_check(label: str, script_args: list[str]) -> int:
    cmd = [PYTHON, *script_args]
    print(f"\n== {label} ==")
    print("$", " ".join(cmd))
    proc = subprocess.run(cmd, cwd=ROOT)
    if proc.returncode != 0:
        print(f"FAIL: {label}")
    else:
        print(f"PASS: {label}")
    return proc.returncode


def main() -> int:
    failures = 0
    for label, args in CHECKS:
        rc = _run_check(label, args)
        if rc != 0:
            failures += 1

    print()
    if failures:
        print(f"Verification failed ({failures} failing check set(s)).")
        return 1

    print("Verification passed (all checks green).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
