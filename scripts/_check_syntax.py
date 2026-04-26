import ast, os, sys

files = [
    "src/data/sentiment.py",
    "src/models/mlb_predictor.py",
    "src/dashboard.py",
    "src/templates/dashboard.html",
]
root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
errors = 0

for rel in files:
    path = os.path.join(root, rel)
    with open(path, encoding="utf-8") as fh:
        src = fh.read()

    if rel.endswith(".py"):
        try:
            ast.parse(src)
            print(f"  OK  {rel}")
        except SyntaxError as e:
            print(f"  ERR {rel}: {e}")
            errors += 1
    else:
        # HTML: just check key strings
        checks = {
            "signal_rationale": "signal_rationale key present",
            "prop-rationale":   "prop-rationale CSS class",
            "Signal Rationale": "Signal Rationale column header",
        }
        for needle, desc in checks.items():
            if needle in src:
                print(f"  OK  {rel}: {desc}")
            else:
                print(f"  ERR {rel}: MISSING {desc}")
                errors += 1

# Check new function names in sentiment.py
with open(os.path.join(root, "src/data/sentiment.py"), encoding="utf-8") as fh:
    s = fh.read()
for name in ["get_player_prop_signal", "_over_prob_norm", "_extract_trend_avg", "_STAT_STD_FACTORS"]:
    status = "OK" if name in s else "ERR"
    print(f"  {status}  sentiment.py: {name}")
    if status == "ERR":
        errors += 1

# Check signal keys in predictor
with open(os.path.join(root, "src/models/mlb_predictor.py"), encoding="utf-8") as fh:
    s = fh.read()
for name in ["get_player_prop_signal", "signal_rationale", "signal_hist_prob", "signal_sentiment"]:
    status = "OK" if name in s else "ERR"
    print(f"  {status}  mlb_predictor.py: {name}")
    if status == "ERR":
        errors += 1

print()
if errors:
    print(f"FAILED — {errors} error(s)")
    sys.exit(1)
else:
    print("All checks passed!")
