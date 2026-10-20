#!/usr/bin/env python3
"""每晚计算全市场 ETF + 主要指数的日线 MACD 金叉/死叉名单，输出 macd-cross.json。
在 GitHub Actions 中运行（macd.yml，工作日 16:05 北京时间）。
口径：MACD(12,26,9)，前复权日 K；金叉 = 昨日 DIF<=DEA 且今日 DIF>DEA（死叉相反）。
只统计最后一根 K 线日期 == 全局最新交易日的标的（排除停牌/未更新）。
"""
import json, re, sys, time, urllib.request
from concurrent.futures import ThreadPoolExecutor

UA = {'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15',
      'Referer': 'https://fund.eastmoney.com/'}
INDEXES = {"1.000300": "沪深300", "1.000905": "中证500", "1.000852": "中证1000", "1.000016": "上证50", "1.000906": "中证800", "1.000010": "上证180", "0.399330": "深证100", "0.399006": "创业板指", "0.399673": "创业板50", "1.000688": "科创50", "1.000698": "科创100", "0.399986": "中证银行", "0.399997": "中证白酒", "0.399998": "中证煤炭", "0.399989": "中证医疗", "1.000933": "中证医药", "0.399967": "中证军工", "0.399971": "中证传媒", "0.399975": "证券公司", "1.000932": "中证消费", "0.399976": "中证新能源汽车", "0.980017": "国证芯片", "1.000922": "中证红利", "100.HSI": "恒生指数", "124.HSTECH": "恒生科技指数", "100.SPX": "标普500", "2.931230": "中证汽车零部件", "2.930721": "中证智能汽车", "2.931008": "中证全指汽车", "2.931239": "中证港股通汽车", "2.930820": "中证高端制造", "2.931865": "中证半导体产业", "2.930713": "中证人工智能", "2.931151": "中证光伏产业", "2.931152": "中证创新药产业", "2.930901": "中证动漫游戏", "2.930707": "中证畜牧养殖", "2.930955": "中证红利低波动100", "2.H30269": "中证红利低波动", "1.000815": "中证细分食品饮料", "2.930653": "中证食品饮料", "1.000685": "上证科创板芯片", "2.931743": "中证半导体材料设备", "100.HSCEI": "恒生国企指数", "1.000510": "中证A500", "2.932000": "中证2000", "1.000985": "中证全指", "0.899050": "北证50"}
ETF_FS = 'b:MK0021,b:MK0022,b:MK0023,b:MK0024'
QUOTE_HOSTS = ['push2.eastmoney.com', 'push2delay.eastmoney.com']

def get_json(url, timeout=15):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode('utf-8'))

def get_quote(path):
    last = None
    for h in QUOTE_HOSTS:
        try:
            return get_json('https://' + h + path)
        except Exception as e:
            last = e
    raise last

def fetch_etf_list():
    out, pn, total = [], 1, None
    while total is None or len(out) < total:
        j = get_quote('/api/qt/clist/get?pn=%d&pz=100&po=1&np=1&fltt=2&invt=2&fid=f3&fs=%s&fields=f12,f14' % (pn, ETF_FS))
        d = j.get('data') or {}
        total = d.get('total') or 0
        diff = d.get('diff') or []
        if not diff:
            break
        for x in diff:
            out.append((x['f12'], x['f14']))
        pn += 1
        if pn > 30:
            break
    return out


def fetch_board_list():
    out, pn, total = [], 1, None
    while total is None or len(out) < total:
        j = get_quote('/api/qt/clist/get?pn=%d&pz=100&po=1&np=1&fltt=2&invt=2&fid=f3&fs=m:90+t:2&fields=f12,f14' % pn)
        d = j.get('data') or {}
        total = d.get('total') or 0
        diff = d.get('diff') or []
        if not diff:
            break
        for x in diff:
            out.append((x['f12'], x['f14']))
        pn += 1
        if pn > 10:
            break
    return out


def fetch_etf_candidates():
    """从天天基金全量代码表筛 ETF 候选（名含 ETF、非联接、非FOF）。"""
    req = urllib.request.Request('https://fund.eastmoney.com/js/fundcode_search.js', headers=UA)
    with urllib.request.urlopen(req, timeout=30) as r:
        text = r.read().decode('utf-8', errors='ignore')
    m = re.search(r'var r = (\[.*?\]);', text, re.S)
    arr = json.loads(m.group(1))
    out = []
    for x in arr:
        code, name = x[0], x[2]
        if 'ETF' in name and '联接' not in name and 'FOF' not in name:
            out.append((code, name))
    return out

def ulist_alive(codes):
    """用 ulist 批量验活：只保留有实时行情返回的代码。"""
    alive = {}
    secids = ['%d.%s' % (market_of(c), c) for c in codes]
    for i in range(0, len(secids), 150):
        chunk = secids[i:i + 150]
        try:
            j = get_quote('/api/qt/ulist.np/get?fltt=2&invt=2&fields=f12,f14&secids=' + ','.join(chunk))
            for x in ((j.get('data') or {}).get('diff') or []):
                alive[x['f12']] = x.get('f14')
        except Exception as e:
            print('ulist alive chunk failed:', e, flush=True)
    return alive

def fetch_etf_list_fallback():
    cand = fetch_etf_candidates()
    alive = ulist_alive([c for c, _ in cand])
    name_of = dict(cand)
    return [(c, alive.get(c) or name_of[c]) for c, _ in cand if c in alive]

def fetch_boards_bruteforce():
    """兜底：扫 BK 代码段，用 ulist 验活出板块名单（名称+主力净流入齐全才留）。"""
    secids = ['90.BK%04d' % i for i in range(1, 1401)]
    out = []
    for i in range(0, len(secids), 150):
        chunk = secids[i:i + 150]
        try:
            j = get_quote('/api/qt/ulist.np/get?fltt=2&invt=2&fields=f12,f14,f62&secids=' + ','.join(chunk))
            for x in ((j.get('data') or {}).get('diff') or []):
                if x.get('f14') and x.get('f62') is not None:
                    out.append((x['f12'], x['f14']))
        except Exception as e:
            print('board sweep chunk failed:', e, flush=True)
    return out

def market_of(code):
    return 1 if code[0] in '569' else 0

def fetch_closes(secid, fqt):
    beg = time.strftime('%Y%m%d', time.localtime(time.time() - 500 * 86400))
    url = ('https://push2his.eastmoney.com/api/qt/stock/kline/get?secid=%s'
           '&fields1=f1,f2&fields2=f51,f52,f53,f54,f55&klt=101&fqt=%d&beg=%s&end=20500101' % (secid, fqt, beg))
    last = None
    for attempt in range(4):
        try:
            j = get_json(url)
            kl = (j.get('data') or {}).get('klines') or []
            if kl:
                bars = []
                for s in kl:
                    p = s.split(',')
                    bars.append((p[0], float(p[2])))
                return bars
            last = ValueError('empty klines')
        except Exception as e:
            last = e
        time.sleep(1.5 * (attempt + 1))
    raise last

def ema(vals, n):
    k = 2.0 / (n + 1)
    out, prev = [], None
    for v in vals:
        prev = v if prev is None else v * k + prev * (1 - k)
        out.append(prev)
    return out

def macd_cross(closes):
    if len(closes) < 40:
        return None
    e12, e26 = ema(closes, 12), ema(closes, 26)
    dif = [a - b for a, b in zip(e12, e26)]
    dea = ema(dif, 9)
    if dif[-2] <= dea[-2] and dif[-1] > dea[-1]:
        kind = 'golden'
    elif dif[-2] >= dea[-2] and dif[-1] < dea[-1]:
        kind = 'death'
    else:
        return None
    pct = (closes[-1] / closes[-2] - 1) * 100 if closes[-2] else 0.0
    return kind, round(dif[-1], 4), round(dea[-1], 4), round(pct, 2)

def process(item):
    secid, name, kind_label, fqt = item
    try:
        bars = fetch_closes(secid, fqt)
    except Exception:
        return None
    if len(bars) < 40:
        return None
    closes = [c for _, c in bars]
    r = macd_cross(closes)
    code = secid.split('.', 1)[1] if '.' in secid else secid
    return {'secid': secid, 'code': code, 'name': name, 'kind': kind_label,
            'last_date': bars[-1][0], 'cross': r}

def load_old_ref():
    try:
        with open('etf-boards.json', encoding='utf-8') as f:
            d = json.load(f)
        return [tuple(x) for x in d.get('etfs', [])], [tuple(x) for x in d.get('boards', [])]
    except Exception:
        return [], []

def fetch_etf_list_robust():
    for attempt in range(3):
        try:
            out = fetch_etf_list()
            if len(out) >= 1000:
                return out
            print('etf list too short:', len(out), flush=True)
        except Exception as e:
            print('etf list (clist) attempt', attempt, 'failed:', e, flush=True)
        time.sleep(15)
    try:
        out = fetch_etf_list_fallback()
        print('etf list via fundcode_search + ulist alive:', len(out), flush=True)
        if len(out) >= 1000:
            return out
    except Exception as e:
        print('etf list fallback failed:', e, flush=True)
    return None

def fetch_board_list_robust():
    for attempt in range(3):
        try:
            out = fetch_board_list()
            if len(out) >= 100:
                return out
        except Exception as e:
            print('board list attempt', attempt, 'failed:', e, flush=True)
        time.sleep(15)
    try:
        out = fetch_boards_bruteforce()
        print('boards via brute-force sweep:', len(out), flush=True)
        if len(out) >= 100:
            return out
    except Exception as e:
        print('board sweep failed:', e, flush=True)
    return None

def main():
    old_etfs, old_boards = load_old_ref()
    fresh_etfs = fetch_etf_list_robust()
    etf_list = fresh_etfs if fresh_etfs else old_etfs
    if not etf_list:
        print('no ETF list available (fresh failed and no fallback file) — abort')
        sys.exit(1)
    print('etf list source:', 'fresh' if fresh_etfs else 'fallback', len(etf_list), flush=True)
    fresh_boards = fetch_board_list_robust()
    board_list = fresh_boards if fresh_boards else old_boards
    ref = {'etfs': [[c, n] for c, n in etf_list], 'boards': [[c, n] for c, n in board_list]}
    with open('etf-boards.json', 'w', encoding='utf-8') as f:
        json.dump(ref, f, ensure_ascii=False)
    print('ref lists written: etfs', len(ref['etfs']), 'boards', len(ref['boards']), '(boards source:', 'fresh' if fresh_boards else 'fallback', ')', flush=True)
    items = []
    for code, name in etf_list:
        items.append(('%d.%s' % (market_of(code), code), name, 'ETF', 1))
    for secid, name in INDEXES.items():
        items.append((secid, name, '指数', 0))
    print('targets:', len(items), flush=True)
    results = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        for r in ex.map(process, items):
            if r:
                results.append(r)
    if len(results) < 800:
        print('too few kline results:', len(results), '— keeping previous macd-cross.json, skip write', flush=True)
        return
    latest = max(r['last_date'] for r in results)
    golden, death = [], []
    for r in results:
        if r['last_date'] != latest or not r['cross']:
            continue
        kind, dif, dea, pct = r['cross']
        entry = {'secid': r['secid'], 'code': r['code'], 'name': r['name'], 'kind': r['kind'],
                 'pct': pct, 'dif': dif, 'dea': dea}
        (golden if kind == 'golden' else death).append(entry)
    golden.sort(key=lambda e: -e['pct'])
    death.sort(key=lambda e: e['pct'])
    out = {'date': latest, 'generated_at': time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(time.time() + 8 * 3600)),
           'scanned': len(results), 'golden': golden, 'death': death}
    with open('macd-cross.json', 'w', encoding='utf-8') as f:
        json.dump(out, f, ensure_ascii=False)
    print('date:', latest, '| scanned:', len(results), '| golden:', len(golden), '| death:', len(death))

if __name__ == '__main__':
    main()
