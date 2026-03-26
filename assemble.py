"""
Assembly script: splices new UI sections into app.py while preserving all backend logic.

Replacements:
1. build_theme_css function (line 128 to line 848 inclusive) → new_css.txt
2. Helpers block (line 853 to line 919 inclusive) → new_helpers.txt
3. render_callout_grid function (line 1044 to line 1064 inclusive) → new_callout.txt
4. Everything after FORECAST_MODE = "live_carbon" line (line 1910) to EOF → new_nav.txt
"""

import sys
import os

BASE = os.path.dirname(os.path.abspath(__file__))

def read(name):
    with open(os.path.join(BASE, name), "r", encoding="utf-8") as f:
        return f.read()

app_src = read("app.py")
lines = app_src.splitlines(keepends=True)

# ── Identify exact boundary lines (1-indexed → 0-indexed) ──────────────────

# Boundary 1: build_theme_css starts at line 128 (0-idx: 127)
# We want to keep everything before that line, then splice in new_css.txt,
# then resume at the line AFTER the closing `.format(**tokens)` which is line 849 (0-idx: 848).
# Line 849 is blank, line 850 is blank, line 851 is `st.markdown(build_theme_css...`
# We keep line 851 onward until helpers.

css_start = 127          # 0-indexed: "def build_theme_css..."
css_end   = 849          # 0-indexed exclusive: first line after `.format(**tokens)` closing block
                         # line 849 (1-indexed 850) is blank, line 850 (1-indexed 851) is st.markdown(...)

# Boundary 2: Helpers comment + functions (line 853..919, 0-idx 852..918)
# Line 851 (0-idx 850): st.markdown(build_theme_css(THEME_TOKENS), unsafe_allow_html=True)
# Line 852 (0-idx 851): blank
# Line 853 (0-idx 852): # ---------------------------------------------------
# We want to keep line 851 (st.markdown call), then splice new_helpers.txt,
# then resume at def build_workload_input which is line 921 (0-idx 920)

helpers_start = 851      # 0-indexed: blank line after st.markdown call
helpers_end   = 920      # 0-indexed exclusive: "def build_workload_input("

# Boundary 3: render_callout_grid (line 1044..1064, 0-idx 1043..1063)
# Keep lines up to 0-idx 1043 exclusive, splice new_callout.txt, resume at 0-idx 1065
callout_start = 1043     # 0-indexed: "def render_callout_grid..."
callout_end   = 1065     # 0-indexed exclusive: "def apply_estimator_value_to_optimizer"

# Boundary 4: After FORECAST_MODE line (line 1910, 0-idx 1909)
# Keep lines 0..1909 inclusive, then append new_nav.txt
nav_start = 1910         # 0-indexed exclusive (keep lines 0..1909)

# ── Load replacement content ────────────────────────────────────────────────
new_css      = read("new_css.txt").strip("\n") + "\n"
new_helpers  = read("new_helpers.txt").strip("\n") + "\n"
new_callout  = read("new_callout.txt").strip("\n") + "\n"
new_nav      = read("new_nav.txt").strip("\n") + "\n"

# ── Assemble ─────────────────────────────────────────────────────────────────
# We build the output in segments, applying replacements from bottom to top
# so earlier line numbers stay valid.

# Segment order (top to bottom of original file):
# [0..css_start)          → keep as-is
# new_css                 → replacement
# [css_end..helpers_start) → keep (includes st.markdown call on line 850)
# new_helpers             → replacement
# [helpers_end..callout_start) → keep (backend functions)
# new_callout             → replacement
# [callout_end..nav_start) → keep (more backend functions)
# new_nav                 → replacement (rest of file)

result_parts = []

result_parts.append("".join(lines[0:css_start]))
result_parts.append(new_css)
result_parts.append("".join(lines[css_end:helpers_start]))
result_parts.append(new_helpers)
result_parts.append("".join(lines[helpers_end:callout_start]))
result_parts.append(new_callout)
result_parts.append("".join(lines[callout_end:nav_start]))
result_parts.append(new_nav)

output = "".join(result_parts)

# ── Write backup then output ─────────────────────────────────────────────────
backup_path = os.path.join(BASE, "app.py.bak")
with open(backup_path, "w", encoding="utf-8") as f:
    f.write(app_src)
print(f"Backup written: {backup_path}")

out_path = os.path.join(BASE, "app.py")
with open(out_path, "w", encoding="utf-8") as f:
    f.write(output)
print(f"app.py written ({len(output.splitlines())} lines)")

# ── Syntax check ─────────────────────────────────────────────────────────────
import ast
try:
    ast.parse(output)
    print("Syntax check: OK")
except SyntaxError as e:
    print(f"Syntax check: FAILED — {e}")
    sys.exit(1)
