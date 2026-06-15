"""Validate dashboard.html: Jinja template syntax + extracted JS syntax (via node)."""
import io
import os
import re
import subprocess
import tempfile

PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src", "templates", "dashboard.html"))

with io.open(PATH, "r", encoding="utf-8") as fh:
    html = fh.read()

# 1) Jinja syntax check
try:
    from jinja2 import Environment
    Environment().parse(html)
    print("Jinja parse: OK")
except Exception as e:
    print(f"Jinja parse: FAIL -> {e}")
    raise SystemExit(1)

# 2) Extract <script> blocks (ignore ones with src=)
scripts = re.findall(r"<script(?![^>]*\bsrc=)[^>]*>(.*?)</script>", html, re.DOTALL | re.IGNORECASE)
print(f"Found {len(scripts)} inline <script> block(s)")

# Neutralize Jinja expressions/statements/comments so it becomes valid-ish JS
def neutralize(js):
    js = re.sub(r"\{\{.*?\}\}", "0", js, flags=re.DOTALL)      # {{ expr }} -> 0
    js = re.sub(r"\{%.*?%\}", "", js, flags=re.DOTALL)          # {% stmt %} -> ''
    js = re.sub(r"\{#.*?#\}", "", js, flags=re.DOTALL)          # {# cmt #}  -> ''
    return js

ok = True
for i, js in enumerate(scripts):
    code = neutralize(js)
    with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False, encoding="utf-8") as tf:
        tf.write(code)
        tmp = tf.name
    try:
        res = subprocess.run(["node", "--check", tmp], capture_output=True, text=True)
        if res.returncode == 0:
            print(f"  script[{i}] node --check: OK ({len(code)} chars)")
        else:
            ok = False
            print(f"  script[{i}] node --check: FAIL\n{res.stderr}")
    finally:
        os.unlink(tmp)

raise SystemExit(0 if ok else 1)
