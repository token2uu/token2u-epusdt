#!/usr/bin/env python3
"""Apply the RPC purpose compatibility update to the bundled frontend.

The frontend source is maintained separately. Fail if the expected bundle has
changed, so an upstream sync cannot silently produce a partial update.
"""
import hashlib
import re
from pathlib import Path

root = Path(__file__).resolve().parents[1]
assets = root / 'src/www/assets'
bundles = list(assets.glob('rpc-*.js'))
bundles = [path for path in bundles if path.name != 'rpc-purpose.js']
if len(bundles) != 1:
    raise SystemExit('Expected exactly one RPC page bundle')
bundle = bundles[0]
source = bundle.read_text()
if 'paidFallbackDescription' in source:
    print('RPC purpose UI is already updated')
    raise SystemExit(0)

replacements = [
    ('s as J,t as xt}from"./form-CQLgQ8B6.js"',
     'r as paidFallbackHelp,s as J,t as xt}from"./form-CQLgQ8B6.js"'),
    ('purpose:_t([`general`,`manual_verify`,`both`])',
     'purpose:_t([`general`,`manual_verify`,`both`,`paid_fallback`])'),
    ('(0,$.jsx)(R,{value:`both`,children:c()})',
     '(0,$.jsx)(R,{value:`both`,children:c()}),(0,$.jsx)(R,{value:`paid_fallback`,children:paidFallbackLabel()})'),
    ('t===`both`&&(n=c())',
     't===`both`?n=c():t===`paid_fallback`&&(n=paidFallbackLabel())'),
    ('{label:c(),value:`both`}',
     '{label:c(),value:`both`},{label:paidFallbackLabel(),value:`paid_fallback`}'),
    ('{columnId:`type`,searchKey:`type`,type:`array`},',
     '{columnId:`type`,searchKey:`type`,type:`array`},{columnId:`purpose`,searchKey:`purpose`,type:`array`},'),
    ('(0,$.jsx)(J,{})]})})]}),(0,$.jsx)(G,{control:f.control,name:`url`',
     'e.value===`paid_fallback`?(0,$.jsx)(paidFallbackHelp,{children:paidFallbackDescription()}):null,(0,$.jsx)(J,{})]})})]}),(0,$.jsx)(G,{control:f.control,name:`url`'),
]
for before, after in replacements:
    if source.count(before) != 1:
        raise SystemExit(f'Unexpected RPC bundle shape: {before}')
    source = source.replace(before, after)
source = ('import{paidFallbackLabel,paidFallbackDescription}from"./rpc-purpose.js";' + source)

# The filter is stored in the URL, so the route must accept the purpose field.
routers = list(assets.glob('router-*.js'))
if len(routers) != 1:
    raise SystemExit('Expected exactly one router bundle')
router = routers[0]
routing = router.read_text()
before = 'type:N.array(N.string()).optional().catch([]),status:N.array(N.enum([`ok`,`down`,`unknown`]))'
after = 'type:N.array(N.string()).optional().catch([]),purpose:N.array(N.enum([`general`,`manual_verify`,`both`,`paid_fallback`])).optional().catch([]),status:N.array(N.enum([`ok`,`down`,`unknown`]))'
if routing.count(before) != 1:
    raise SystemExit('Unexpected RPC route search schema')
routing = routing.replace(before, after)

# New names ensure browsers do not reuse an immutable cached bundle.
new_bundle = 'rpc-' + hashlib.sha256(source.encode()).hexdigest()[:8] + '.js'
routing = routing.replace(bundle.name, new_bundle)
new_router = 'router-' + hashlib.sha256(routing.encode()).hexdigest()[:8] + '.js'
changed_assets = []
for path in (root / 'src/www').rglob('*'):
    if path.is_file() and path.suffix in ('.js', '.html') and path not in (bundle, router):
        text = path.read_text()
        updated = text.replace(bundle.name, new_bundle).replace(router.name, new_router)
        if updated != text:
            path.write_text(updated)
            if path.parent == assets:
                changed_assets.append(path)
(assets / new_bundle).write_text(source.replace(router.name, new_router))
(assets / new_router).write_text(routing)
changed_assets.extend([assets / new_bundle, assets / new_router])
bundle.unlink()
router.unlink()
# Workbox otherwise retains modified importers under their original cache keys.
worker = root / 'src/www/sw.js'
manifest = worker.read_text()
for path in changed_assets:
    revision = hashlib.sha256(path.read_bytes()).hexdigest()[:32]
    pattern = r'(\{url:"assets/' + re.escape(path.name) + r'",revision:)null(\})'
    manifest, count = re.subn(pattern, lambda m: m[1] + '"' + revision + '"' + m[2], manifest)
    if count != 1:
        raise SystemExit(f'Unexpected precache entry: {path.name}')
helper = assets / 'rpc-purpose.js'
revision = hashlib.sha256(helper.read_bytes()).hexdigest()[:32]
manifest = manifest.replace('s.precacheAndRoute([', 's.precacheAndRoute([{url:"assets/rpc-purpose.js",revision:"' + revision + '"},', 1)
worker.write_text(manifest)
print(f'Updated {new_bundle} and {new_router}')
