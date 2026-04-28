"""Fix the sortAndRenderProps function — restore missing badge + return template literal"""
path = r"c:\Users\lovingtracktor\bettor\src\templates\dashboard.html"

with open(path, encoding='utf-8') as f:
    content = f.read()

# The corrupted part: after conf, TD tags appear directly instead of badge + return
old_fragment = "      conf: p.confidence||50,\n      <td>${p.name||'\u2014'}</td>"
new_fragment = (
    "      conf: p.confidence||50,\n"
    "      badge: p.safety_label||'MODERATE',\n"
    "    }).replace(/\"/g,'&quot;');\n"
    "    return `<tr>\n"
    "      <td>${p.name||'\u2014'}</td>"
)

if old_fragment in content:
    content = content.replace(old_fragment, new_fragment)
    with open(path, 'w', encoding='utf-8') as f:
        f.write(content)
    print("FIXED")
else:
    idx = content.find("conf: p.confidence||50")
    if idx != -1:
        print("Found conf at index", idx)
        print(repr(content[idx:idx+120]))
    else:
        print("NOT FOUND AT ALL")
