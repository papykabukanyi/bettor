content = open('src/templates/dashboard.html', encoding='utf-8').read()
checks = [
    ('panel-parlays exists', 'panel-parlays'),
    ('props-tbody exists', 'props-tbody'),
    ('autoGenParlaysFromProps', 'function autoGenParlaysFromProps()'),
    ('renderParlays delegates', 'function renderParlays(parlays)'),
    ('switchTab calls autoGen', 'autoGenParlaysFromProps(); loadTrackedParlays()'),
    ('sortAndRenderProps return tr', 'return `<tr>'),
    ('filterProps closing', 'return true;'),
    ('NO rtAndRenderProps', 'rtAndRenderProps' not in content),
    ('NO refreshAutoPropsParlay', 'refreshAutoPropsParlay' not in content),
]
for name, check in checks:
    if isinstance(check, bool):
        print(f'{"PASS" if check else "FAIL"}: {name}')
    else:
        print(f'{"PASS" if check in content else "FAIL"}: {name}')
