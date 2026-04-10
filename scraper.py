#!/usr/bin/env python3
"""
P&G Scraper — standalone runner for GitHub Actions and local use.
Reads configs from configs/*.json, scrapes VTEX IO or Magento 2, saves to output/.
Usage:
  python scraper.py                        # scrape all configs
  python scraper.py farmacity_com          # scrape one config
  python scraper.py farmacity_com perfumeriaspigmento_com_ar
"""

import json, re, time, sys, argparse, urllib.request, urllib.error
from datetime import datetime
from pathlib import Path

BASE   = Path(__file__).parent
CFGDIR = BASE / "configs"
OUTDIR = BASE / "output"
OUTDIR.mkdir(exist_ok=True)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'application/json',
    'Accept-Language': 'es-AR,es;q=0.9',
}

# ── P&G EAN list (optional enrichment) ────────────────────────────────────────
def _load_pg_eans():
    p = CFGDIR / 'pg_ean_list.json'
    if p.exists():
        try:
            return json.loads(p.read_text(encoding='utf-8')).get('eans', {})
        except Exception:
            pass
    return {}

PG_EAN_DATA = _load_pg_eans()
PG_EANS     = set(PG_EAN_DATA.keys())

CAT_KEYWORDS = {
    'panales':      ['pañal', 'panal', 'pants', 'toallita', 'baby wipe'],
    'capilar':      ['shampoo', 'champú', 'acondicionador', 'tratamiento',
                     'crema de peinar', 'keratina', 'cabello', 'capilar', 'pelo'],
    'afeitado':     ['afeit', 'maquini', 'cartucho', 'hoja', 'navaja', 'foam',
                     'espuma', 'gel afeit', 'depilad', 'razor', 'gillette'],
    'desodorantes': ['desodor', 'antitranspir', 'deo ', 'deo-', 'body spray',
                     'roll-on', 'rollon', 'stick deo'],
    'dental':       ['dental', 'pasta dent', 'cepillo dent', 'enjuague',
                     'hilo dent', 'blanqueador', 'floss', 'bucal'],
    'femenino':     ['toalla', 'protector diario', 'tampón', 'tampon',
                     'copa menstrual', 'flujo'],
    'salud':        ['vick', 'vaporub', 'inhala', 'tos', 'resfr', 'gripe'],
    'suavizantes':  ['suaviz', 'downy', 'comfort', 'vivere', 'enjuague de ropa'],
}

def _in_cat(name, ean, cat_id, owner):
    if owner == 'pg' and ean and ean in PG_EAN_DATA:
        return PG_EAN_DATA[ean].get('categoria_id', '') == cat_id
    kws = CAT_KEYWORDS.get(cat_id, [])
    if not kws:
        return True
    nl = name.lower()
    return any(k in nl for k in kws)

def _norm(s):
    return re.sub(r'[^a-z0-9]', ' ', s.lower())

def _classify(name, brand, pg_list, comp_list):
    check = _norm(f"{name} {brand}")
    for m in pg_list:
        if _norm(m) in check:
            return m, 'pg'
    for m in comp_list:
        if _norm(m) in check:
            return m, 'comp'
    return brand or 'Otro', 'comp'

def _size_units(name):
    size, units = '', 1
    m = re.search(r'(\d+(?:[.,]\d+)?)\s*(ml|g|l|lt|kg|cm)\b', name, re.I)
    if m:
        size = f"{m.group(1)}{m.group(2).lower()}"
    t = re.search(r'\b(RN\+?|P\b|M\b|G\b|XG|XXG|XXXG)\b', name)
    if t and not size:
        size = t.group(1)
    u = re.search(r'x\s*(\d+)', name, re.I) or re.search(r'(\d+)\s*(?:un|unid)', name, re.I)
    if u:
        units = int(u.group(1))
    return size, units

def _slug(name):
    return re.sub(r'[^a-z0-9]+', '-', name.lower().replace('&', 'and')).strip('-')

def _get_brands(cat):
    if 'brands' in cat:
        return cat['brands']
    out = []
    for n in cat.get('marcas_pg', []):
        out.append({'name': n, 'slug': _slug(n), 'type': 'pg'})
    for n in cat.get('marcas_comp', []):
        out.append({'name': n, 'slug': _slug(n), 'type': 'comp'})
    return out

# ── VTEX IO Intelligent Search ─────────────────────────────────────────────────

def _fetch_vtex(base, account, slug):
    all_p, page, seen = [], 1, set()
    while True:
        url = (f"{base}/_v/api/intelligent-search/product_search/brand/{slug}"
               f"?count=50&page={page}&an={account}&order=OrderByBestDiscountDESC")
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=15) as r:
                data = json.loads(r.read())
        except Exception as e:
            print(f"      ⚠ {slug}: {e}", flush=True)
            break
        prods = data.get('products', [])
        if not prods:
            break
        for p in prods:
            pid = p.get('productId')
            if pid and pid not in seen:
                seen.add(pid)
                all_p.append(p)
        if len(all_p) >= data.get('recordsFiltered', 0):
            break
        page += 1
        time.sleep(0.3)
    return all_p

def _vtex_offer(item):
    sellers = item.get('sellers', [])
    if not sellers:
        return 0, 0, 0, ''
    offer = sellers[0].get('commertialOffer', {})
    price = offer.get('Price', 0) or 0
    lp    = offer.get('ListPrice', 0) or price
    if (offer.get('AvailableQuantity', 1) or 0) <= 0 or price <= 0:
        return 0, 0, 0, ''
    disc  = round((1 - price / lp) * 100) if lp > price > 0 else 0
    promo = ''
    for h in (offer.get('discountHighlights') or []):
        promo = h.get('name', '').split('#')[0].strip()
        if promo:
            break
    if not promo:
        for t in (offer.get('teasers') or []):
            if isinstance(t, dict):
                promo = t.get('name', '')
                if promo:
                    break
    return price, lp, disc, promo

def _vtex_ean(item):
    ean = item.get('ean') or ''
    if not ean:
        for ref in (item.get('referenceId') or []):
            if isinstance(ref, dict) and ref.get('Key') in ('EAN', 'Ean', 'ean'):
                ean = str(ref.get('Value', ''))
                break
    return ean.strip() or None

def _process_vtex(raw, pg_list, comp_list, cat_id):
    marcas = {}
    for prod in raw:
        pname = prod.get('productName', '')
        brand = prod.get('brand', '')
        mname, owner = _classify(pname, brand, pg_list, comp_list)
        if mname not in marcas:
            marcas[mname] = {'owner': owner, 'skus': []}
        for item in prod.get('items', []):
            price, lp, disc, promo = _vtex_offer(item)
            if price <= 0:
                continue
            iname = item.get('name') or item.get('nameComplete') or ''
            sku   = f"{pname} {iname}".strip() if iname and iname != pname else pname
            ean   = _vtex_ean(item)
            if not _in_cat(sku, ean, cat_id, owner):
                continue
            if any(s['sku'] == sku for s in marcas[mname]['skus']):
                continue
            size, units = _size_units(sku)
            pg_of = (ean in PG_EANS) if (owner == 'pg' and ean) else None
            marcas[mname]['skus'].append({
                'sku': sku, 'ean': ean, 'size': size, 'units': units,
                'precio': lp, 'descuento': disc, 'promo': promo or None,
                'pg_oficial': pg_of,
            })
    return [{'nombre': k, 'owner': v['owner'], 'skus': v['skus']}
            for k, v in marcas.items() if v['skus']]

# ── Magento 2 GraphQL ──────────────────────────────────────────────────────────

def _fetch_magento(base, brand_name):
    all_p, page = [], 1
    safe = brand_name.replace('"', '').replace('\\', '')
    while True:
        gql = json.dumps({"query": f"""{{
          products(search: "{safe}", pageSize: 48, currentPage: {page},
                   sort: {{relevance: DESC}}) {{
            total_count
            items {{
              name sku barcode
              price_range {{
                minimum_price {{
                  regular_price {{ value }}
                  final_price   {{ value }}
                  discount      {{ percent_off amount_off }}
                }}
              }}
            }}
          }}
        }}"""}).encode('utf-8')
        try:
            req = urllib.request.Request(
                base + '/graphql', data=gql,
                headers={**HEADERS, 'Content-Type': 'application/json'}
            )
            with urllib.request.urlopen(req, timeout=15) as r:
                resp = json.loads(r.read())
        except Exception as e:
            print(f"      ⚠ GraphQL: {e}", flush=True)
            break
        prods = resp.get('data', {}).get('products', {})
        total = prods.get('total_count', 0)
        items = prods.get('items', [])
        if not items:
            break
        for item in items:
            if brand_name.lower() in item.get('name', '').lower():
                all_p.append(item)
        if page * 48 >= total:
            break
        page += 1
        time.sleep(0.3)
    return all_p

def _process_magento(raw, pg_list, comp_list, cat_id):
    marcas = {}
    for item in raw:
        pname = item.get('name', '')
        mname, owner = _classify(pname, '', pg_list, comp_list)
        if mname not in marcas:
            marcas[mname] = {'owner': owner, 'skus': []}
        mp   = item.get('price_range', {}).get('minimum_price', {})
        lp   = mp.get('regular_price', {}).get('value', 0) or 0
        fp   = mp.get('final_price', {}).get('value', 0) or 0
        disc = round(mp.get('discount', {}).get('percent_off', 0) or 0)
        if fp <= 0:
            continue
        ean = str(item.get('barcode', '') or '').strip() or None
        if not _in_cat(pname, ean, cat_id, owner):
            continue
        if any(s['sku'] == pname for s in marcas[mname]['skus']):
            continue
        size, units = _size_units(pname)
        pg_of = (ean in PG_EANS) if (owner == 'pg' and ean) else None
        marcas[mname]['skus'].append({
            'sku': pname, 'ean': ean, 'size': size, 'units': units,
            'precio': lp or fp, 'descuento': disc, 'promo': None,
            'pg_oficial': pg_of,
        })
    return [{'nombre': k, 'owner': v['owner'], 'skus': v['skus']}
            for k, v in marcas.items() if v['skus']]

# ── Scrape one config ──────────────────────────────────────────────────────────

def scrape_config(cfg_path):
    cfg      = json.loads(Path(cfg_path).read_text(encoding='utf-8'))
    base     = cfg['base_url'].rstrip('/')
    account  = cfg.get('account', '')
    retailer = cfg.get('retailer', base)
    platform = cfg.get('platform', 'vtex_io')
    today    = datetime.now().strftime('%Y-%m-%d')
    week     = datetime.now().isocalendar()[1]

    print(f"\n{'='*60}", flush=True)
    print(f"  {retailer}  |  {platform.upper().replace('_', ' ')}  |  {today} Sem {week}", flush=True)
    print(f"{'='*60}", flush=True)

    all_cats = []
    for cat in cfg.get('categorias', []):
        print(f"\n{cat['icon']}  {cat['nombre']}", flush=True)
        brands    = _get_brands(cat)
        pg_list   = [b['name'] for b in brands if b.get('type') == 'pg']
        comp_list = [b['name'] for b in brands if b.get('type') == 'comp']
        raw, seen = [], set()

        for brand in brands:
            bname = brand['name']
            bslug = brand.get('slug', _slug(bname))
            if platform == 'magento2':
                prods = _fetch_magento(base, bname)
                raw.extend(prods)
                print(f"   {bname}: {len(prods)} productos", flush=True)
            else:
                prods = _fetch_vtex(base, account, bslug)
                added = 0
                for p in prods:
                    pid = p.get('productId')
                    if pid and pid not in seen:
                        seen.add(pid)
                        raw.append(p)
                        added += 1
                print(f"   {bname}: {added} productos", flush=True)
            time.sleep(0.3)

        cat_id = cat.get('id', '')
        if platform == 'magento2':
            marcas = _process_magento(raw, pg_list, comp_list, cat_id)
        else:
            marcas = _process_vtex(raw, pg_list, comp_list, cat_id)

        if marcas:
            total = sum(len(m['skus']) for m in marcas)
            all_cats.append({
                'id': cat_id, 'nombre': cat['nombre'],
                'icon': cat['icon'], 'marcas': marcas,
            })
            print(f"   ✅ {len(marcas)} marcas · {total} SKUs", flush=True)
        else:
            print(f"   ⚠ Sin productos", flush=True)

    result = {
        'label': f"Sem {week} — {today}",
        'fecha': today,
        'fuente': retailer,
        'nota': '',
        'categorias': all_cats,
    }

    out_slug = re.sub(r'[^a-z0-9]', '_', retailer.lower().replace('www.', ''))[:30].strip('_')
    out_path = OUTDIR / f"{out_slug}_{today}.json"
    out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')

    total_skus = sum(len(s['skus']) for c in all_cats for s in c['marcas'])
    total_dto  = sum(1 for c in all_cats for m in c['marcas'] for s in m['skus'] if s['descuento'] > 0)
    print(f"\n✅ {out_path.name}  ({total_skus} SKUs · {total_dto} con descuento)", flush=True)

# ── Manifest ───────────────────────────────────────────────────────────────────

def update_manifest():
    files = []
    for f in sorted(OUTDIR.glob('*.json'), reverse=True):
        if f.name == 'manifest.json':
            continue
        try:
            d = json.loads(f.read_text(encoding='utf-8'))
            files.append({
                'filename': f.name,
                'retailer': d.get('fuente', f.stem),
                'fecha':    d.get('fecha', ''),
                'label':    d.get('label', f.stem),
            })
        except Exception:
            pass

    retailers = {}
    for f in files:
        rid = re.sub(r'[^a-z0-9]', '_', f['retailer'].lower().replace('www.', ''))[:30].strip('_')
        if rid not in retailers:
            retailers[rid] = {'id': rid, 'name': f['retailer'], 'files': []}
        retailers[rid]['files'].append({
            'filename': f['filename'],
            'fecha':    f['fecha'],
            'label':    f['label'],
        })

    manifest = {
        'updated': datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),
        'retailers': list(retailers.values()),
    }
    (OUTDIR / 'manifest.json').write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2), encoding='utf-8'
    )
    print(f"\n📋 manifest.json actualizado  ({len(retailers)} retailer/s)", flush=True)

# ── Entry point ────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='P&G Scraper')
    parser.add_argument('retailers', nargs='*',
                        help='Config IDs to run (default: all). E.g. farmacity_com')
    args = parser.parse_args()

    cfg_files = [f for f in sorted(CFGDIR.glob('*.json'))
                 if f.name != 'pg_ean_list.json']

    if not cfg_files:
        print("ERROR: No se encontraron configs en configs/", file=sys.stderr)
        sys.exit(1)

    if args.retailers:
        cfg_files = [f for f in cfg_files if f.stem in args.retailers]
        if not cfg_files:
            print(f"ERROR: No configs para: {args.retailers}", file=sys.stderr)
            sys.exit(1)

    errors = 0
    for cfg_path in cfg_files:
        try:
            scrape_config(cfg_path)
        except Exception as e:
            print(f"\n❌ Error en {cfg_path.name}: {e}", file=sys.stderr)
            errors += 1

    update_manifest()
    print(f"\n{'='*60}", flush=True)
    print(f"  Listo.  Errores: {errors}", flush=True)
    print(f"{'='*60}\n", flush=True)

    if errors:
        sys.exit(1)

if __name__ == '__main__':
    main()
