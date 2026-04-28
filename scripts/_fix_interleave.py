"""
Fix the interleaved filterProps/autoGenParlaysFromProps mess.

The current broken state in the file is:
  function autoGenParlaysFromProps() {
    ...qualified filter setup...
    if (!qualified.length) {
      ...
      return;
    }
    return true;      <-- THIS IS LEFTOVER filterProps code
  });
  sortAndRenderProps();
}

  if (empty) empty.classList.add('hidden');  <-- CONTINUES autoGen
  ...rest of autoGen...

We need to remove the leftover filterProps closing from INSIDE autoGen
and put it back before autoGen.
"""

path = r"c:\Users\lovingtracktor\bettor\src\templates\dashboard.html"

with open(path, encoding='utf-8') as f:
    content = f.read()

# The broken sequence inside autoGenParlaysFromProps:
# "    return;\n  }\n    return true;\n  });\n  sortAndRenderProps();\n}\n\n  if (empty)"
broken = "    return;\n  }\n    return true;\n  });\n  sortAndRenderProps();\n}\n\n  if (empty)"
fixed  = "    return;\n  }\n\n  if (empty)"

if broken in content:
    content = content.replace(broken, fixed)
    # Now we need to make sure filterProps() has its closing BEFORE autoGenParlaysFromProps
    # Check if filterProps already has proper closing
    if 'return true;\n  });\n  sortAndRenderProps();\n}\n\nfunction autoGenParlaysFromProps' not in content:
        # The filterProps is missing its closing, add it
        content = content.replace(
            '}\n\nfunction autoGenParlaysFromProps',
            '    return true;\n  });\n  sortAndRenderProps();\n}\n\nfunction autoGenParlaysFromProps'
        )
        print("Added filterProps closing")
    with open(path, 'w', encoding='utf-8') as f:
        f.write(content)
    print("FIXED main issue")
else:
    print("Broken pattern not found")
    idx = content.find('return;\n  }\n    return true;')
    if idx != -1:
        print("Found at:", idx)
        print(repr(content[idx-100:idx+300]))
    else:
        # Check what's between autoGen qualified filter and if (empty) add hidden
        idx2 = content.find('if (empty) empty.classList.add')
        print("if(empty) add at:", idx2)
        print(repr(content[max(0,idx2-300):idx2+100]))
