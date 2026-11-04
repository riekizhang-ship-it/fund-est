#!/usr/bin/env python3
"""每晚计算全市场 ETF + 板块 + 主要指数的 MACD/KDJ 交叉名单，输出 macd-cross.json。
在 GitHub Actions 中运行（macd.yml，工作日 16:05 北京时间）。
口径：MACD(12,26,9) 与 KDJ(9,3,3)，日线（101）/周线（102）/月线（103）三周期；
ETF 前复权、板块/指数不复权；金叉 = 上期快线<=慢线 且 本期快线>慢线（死叉相反）。
日线另算即将金叉 = 尚未金叉（DIF<DEA）、差距较昨日收窄、且差距 ≤ 收盘价的 0.5%。
只统计最后一根 K 线日期 == 全局最新日期的标的（排除停牌/未更新）。
周线/月线以最近一根 K 线计算：当周/当月未走完时为盘中形态，周五/月末收盘后才定格。
货币类 ETF（名称特征或价格冻结）不参与任何名单计算。
"""
import collections, json, math, re, sys, time, urllib.request
from concurrent.futures import ThreadPoolExecutor

UA = {'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15',
      'Referer': 'https://fund.eastmoney.com/'}
INDEXES = {"1.000300": "沪深300", "1.000905": "中证500", "1.000852": "中证1000", "1.000016": "上证50", "1.000906": "中证800", "1.000010": "上证180", "0.399330": "深证100", "0.399006": "创业板指", "0.399673": "创业板50", "1.000688": "科创50", "1.000698": "科创100", "0.399986": "中证银行", "0.399997": "中证白酒", "0.399998": "中证煤炭", "0.399989": "中证医疗", "1.000933": "中证医药", "0.399967": "中证军工", "0.399971": "中证传媒", "0.399975": "证券公司", "1.000932": "中证消费", "0.399976": "中证新能源汽车", "0.980017": "国证芯片", "1.000922": "中证红利", "100.HSI": "恒生指数", "124.HSTECH": "恒生科技指数", "100.SPX": "标普500", "2.931230": "中证汽车零部件", "2.930721": "中证智能汽车", "2.931008": "中证全指汽车", "2.931239": "中证港股通汽车", "2.930820": "中证高端制造", "2.931865": "中证半导体产业", "2.930713": "中证人工智能", "2.931151": "中证光伏产业", "2.931152": "中证创新药产业", "2.930901": "中证动漫游戏", "2.930707": "中证畜牧养殖", "2.930955": "中证红利低波动100", "2.H30269": "中证红利低波动", "1.000815": "中证细分食品饮料", "2.930653": "中证食品饮料", "1.000685": "上证科创板芯片", "2.931743": "中证半导体材料设备", "100.HSCEI": "恒生国企指数", "1.000510": "中证A500", "2.932000": "中证2000", "1.000985": "中证全指", "0.899050": "北证50"}
ETF_FS = 'b:MK0021,b:MK0022,b:MK0023,b:MK0024'
QUOTE_HOSTS = ['push2.eastmoney.com', 'push2delay.eastmoney.com']

def get_json(url, timeout=10):
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
    """全量板块：地域(t:1) + 行业(t:2) + 概念(t:3) 三类合并去重。"""
    out, seen = [], set()
    for t in (1, 2, 3):
        pn, total, got = 1, None, 0
        while total is None or got < total:
            j = get_quote('/api/qt/clist/get?pn=%d&pz=100&po=1&np=1&fltt=2&invt=2&fid=f3&fs=m:90+t:%d&fields=f12,f14' % (pn, t))
            d = j.get('data') or {}
            total = d.get('total') or 0
            diff = d.get('diff') or []
            if not diff:
                break
            got += len(diff)
            for x in diff:
                if x['f12'] not in seen:
                    seen.add(x['f12'])
                    out.append((x['f12'], x['f14']))
            pn += 1
            if pn > 15:
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
    secids = ['90.BK%04d' % i for i in range(1, 2001)]
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

def get_text(url, timeout=10):
    req = urllib.request.Request(url, headers={'User-Agent': UA['User-Agent'],
                                                'Referer': 'https://www.10jqka.com.cn/'})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode('utf-8')

def fetch_bars_ths(code, period):
    """同花顺备选 K 线（quotebridge v6 line，JSONP 文本）：period 01=日 11=周 21=月。
    返回 [(YYYY-MM-DD, close, high, low)]（同花顺字段序：日期,开,高,低,收,…）。"""
    url = 'https://d.10jqka.com.cn/v6/line/hs_%s/%s/last800.js' % (code, period)
    last = None
    for attempt in range(2):
        try:
            t = get_text(url)
            j = json.loads(t[t.index('(') + 1:t.rindex(')')])
            bars = []
            for s in (j.get('data') or '').split(';'):
                if not s:
                    continue
                p = s.split(',')
                d = p[0]
                bars.append(('%s-%s-%s' % (d[:4], d[4:6], d[6:8]), float(p[4]), float(p[2]), float(p[3])))
            if bars:
                return bars
            last = ValueError('empty ths data')
        except Exception as e:
            last = e
        time.sleep(1.5)
    raise last

def fetch_bars(secid, fqt, klt, beg_days):
    """东财 K 线：klt 101=日 102=周 103=月。返回 [(date, close, high, low)]。"""
    beg = time.strftime('%Y%m%d', time.localtime(time.time() - beg_days * 86400))
    url = ('https://push2his.eastmoney.com/api/qt/stock/kline/get?secid=%s'
           '&fields1=f1,f2&fields2=f51,f52,f53,f54,f55&klt=%d&fqt=%d&beg=%s&end=20500101' % (secid, klt, fqt, beg))
    last = None
    for attempt in range(2):
        try:
            j = get_json(url)
            kl = (j.get('data') or {}).get('klines') or []
            if kl:
                bars = []
                for s in kl:
                    p = s.split(',')
                    bars.append((p[0], float(p[2]), float(p[3]), float(p[4])))
                return bars
            last = ValueError('empty klines')
        except Exception as e:
            last = e
        time.sleep(1.0 * (attempt + 1))
    raise last

def ema(vals, n):
    k = 2.0 / (n + 1)
    out, prev = [], None
    for v in vals:
        prev = v if prev is None else v * k + prev * (1 - k)
        out.append(prev)
    return out

NEAR_GAP_PCT_MAX = 0.5  # 即将金叉门槛：(DEA-DIF) 占收盘价的百分比上限
MONEY_NAME_RE = re.compile(r'货币|现金|添益|添利|快线|日利|活期|保证金|收益宝')

def is_flat(closes, n=20):
    """近 n 日平均单日涨跌幅 <0.02% 视为价格冻结（货币类），MACD 对其无意义。"""
    if len(closes) < n + 1:
        return False
    seg = closes[-(n + 1):]
    moves = [abs(seg[i] / seg[i - 1] - 1) for i in range(1, len(seg)) if seg[i - 1]]
    return bool(moves) and sum(moves) / len(moves) < 0.0002

def analyze(closes):
    """返回 (cross, near)：cross = (kind, dif, dea, pct) 或 None；
    near = (dif, dea, pct, gap_pct, days_est) 或 None。"""
    if len(closes) < 40:
        return None, None
    e12, e26 = ema(closes, 12), ema(closes, 26)
    dif = [a - b for a, b in zip(e12, e26)]
    dea = ema(dif, 9)
    pct = (closes[-1] / closes[-2] - 1) * 100 if closes[-2] else 0.0
    pct = round(pct, 2)
    if dif[-2] <= dea[-2] and dif[-1] > dea[-1]:
        return ('golden', round(dif[-1], 4), round(dea[-1], 4), pct), None
    if dif[-2] >= dea[-2] and dif[-1] < dea[-1]:
        return ('death', round(dif[-1], 4), round(dea[-1], 4), pct), None
    near = None
    if dif[-1] < dea[-1] and closes[-1]:
        gap = dea[-1] - dif[-1]
        gap_prev = dea[-2] - dif[-2]
        gap_pct = gap / closes[-1] * 100
        if gap < gap_prev and gap_pct <= NEAR_GAP_PCT_MAX:
            shrink = gap_prev - gap
            days = min(30, max(1, math.ceil(gap / shrink))) if shrink > 0 else None
            near = (round(dif[-1], 4), round(dea[-1], 4), pct, round(gap_pct, 3), days)
    return None, near

def analyze_kdj(bars):
    """KDJ(9,3,3)：返回 (kind, K, D, J, pct) 或 None。bars = [(date, close, high, low)]。"""
    if len(bars) < 40:
        return None
    closes = [b[1] for b in bars]
    highs = [b[2] for b in bars]
    lows = [b[3] for b in bars]
    k = d = 50.0
    ks, ds = [], []
    for i in range(len(bars)):
        lo = min(lows[max(0, i - 8):i + 1])
        hi = max(highs[max(0, i - 8):i + 1])
        rsv = 50.0 if hi == lo else (closes[i] - lo) / (hi - lo) * 100.0
        k = 2.0 / 3.0 * k + 1.0 / 3.0 * rsv
        d = 2.0 / 3.0 * d + 1.0 / 3.0 * k
        ks.append(k)
        ds.append(d)
    pct = (closes[-1] / closes[-2] - 1) * 100 if closes[-2] else 0.0
    pct = round(pct, 2)
    if ks[-2] <= ds[-2] and ks[-1] > ds[-1]:
        kind = 'golden'
    elif ks[-2] >= ds[-2] and ks[-1] < ds[-1]:
        kind = 'death'
    else:
        return None
    j = 3.0 * ks[-1] - 2.0 * ds[-1]
    return kind, round(ks[-1], 2), round(ds[-1], 2), round(j, 2), pct

PERIODS = {'day': (101, 500, '01'), 'week': (102, 1700, '11'), 'month': (103, 3300, '21')}

def process(args):
    item, cfg = args
    secid, name, kind_label, fqt = item
    klt, beg_days, ths_period = cfg
    bars = None
    try:
        bars = fetch_bars(secid, fqt, klt, beg_days)
    except Exception:
        bars = None
    if (not bars or len(bars) < 40) and kind_label == 'ETF':
        code = secid.split('.', 1)[1] if '.' in secid else secid
        try:
            bars = fetch_bars_ths(code, ths_period)
        except Exception:
            bars = None
    if not bars or len(bars) < 40:
        return None
    closes = [b[1] for b in bars]
    if kind_label == 'ETF' and (MONEY_NAME_RE.search(name) or is_flat(closes)):
        return {'skip': True}
    cross, near = analyze(closes)
    kdj = analyze_kdj(bars)
    code = secid.split('.', 1)[1] if '.' in secid else secid
    return {'secid': secid, 'code': code, 'name': name, 'kind': kind_label,
            'last_date': bars[-1][0], 'cross': cross, 'near': near, 'kdj': kdj}

def run_period(items, label):
    cfg = PERIODS[label]
    args = [(it, cfg) for it in items]
    results, failed = [], []
    with ThreadPoolExecutor(max_workers=16) as ex:
        for a, r in zip(args, ex.map(process, args)):
            if r and not r.get('skip'):
                results.append(r)
            elif not r:
                failed.append(a)
    print(label, 'first pass ok:', len(results), '| failed:', len(failed), flush=True)
    if failed:
        with ThreadPoolExecutor(max_workers=6) as ex:
            for r in ex.map(process, failed):
                if r and not r.get('skip'):
                    results.append(r)
        print(label, 'after retry pass:', len(results), flush=True)
    print(label, 'results by kind:', dict(collections.Counter(r['kind'] for r in results)), flush=True)
    return results

def build_lists(results):
    latest = max(r['last_date'] for r in results)
    golden, death, near, kdj_g, kdj_d = [], [], [], [], []
    for r in results:
        if r['last_date'] != latest:
            continue
        base = {'secid': r['secid'], 'code': r['code'], 'name': r['name'], 'kind': r['kind']}
        if r['cross']:
            kind, dif, dea, pct = r['cross']
            e = dict(base, pct=pct, dif=dif, dea=dea)
            (golden if kind == 'golden' else death).append(e)
        elif r['near']:
            dif, dea, pct, gap_pct, days = r['near']
            near.append(dict(base, pct=pct, dif=dif, dea=dea, gap_pct=gap_pct, days=days))
        if r['kdj']:
            kind, kv, dv, jv, pct = r['kdj']
            e = dict(base, pct=pct, k=kv, d=dv, j=jv)
            (kdj_g if kind == 'golden' else kdj_d).append(e)
    golden.sort(key=lambda e: -e['pct'])
    death.sort(key=lambda e: e['pct'])
    near.sort(key=lambda e: e['gap_pct'])
    kdj_g.sort(key=lambda e: -e['pct'])
    kdj_d.sort(key=lambda e: e['pct'])
    return {'date': latest, 'scanned': len(results), 'golden': golden, 'death': death,
            'near': near, 'kdj_golden': kdj_g, 'kdj_death': kdj_d}

def load_old_cross():
    try:
        with open('macd-cross.json', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return None

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
    for code, name in board_list:
        items.append(('90.%s' % code, name, '板块', 0))
    for secid, name in INDEXES.items():
        items.append((secid, name, '指数', 0))
    print('targets:', len(items), flush=True)
    old_cross = load_old_cross()
    day_res = run_period(items, 'day')
    if len(day_res) < 800:
        print('too few daily kline results:', len(day_res), '— keeping previous macd-cross.json, skip write', flush=True)
        return
    day = build_lists(day_res)
    sections = {}
    for label in ('week', 'month'):
        res = run_period(items, label)
        if len(res) >= 800:
            s = build_lists(res)
            sections[label] = {'date': s['date'], 'scanned': s['scanned'],
                               'macd_golden': s['golden'], 'macd_death': s['death'],
                               'kdj_golden': s['kdj_golden'], 'kdj_death': s['kdj_death']}
        elif old_cross and (old_cross.get('periods') or {}).get(label):
            sections[label] = old_cross['periods'][label]
            print(label, 'too few results:', len(res), '— carried over previous section', flush=True)
        else:
            sections[label] = {'date': None, 'scanned': len(res), 'macd_golden': [], 'macd_death': [],
                               'kdj_golden': [], 'kdj_death': []}
            print(label, 'too few results:', len(res), '— empty section (no previous to carry)', flush=True)
    out = {'date': day['date'], 'generated_at': time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(time.time() + 8 * 3600)),
           'scanned': day['scanned'], 'golden': day['golden'], 'death': day['death'], 'near': day['near'],
           'kdj_golden': day['kdj_golden'], 'kdj_death': day['kdj_death'], 'periods': sections}
    with open('macd-cross.json', 'w', encoding='utf-8') as f:
        json.dump(out, f, ensure_ascii=False)
    print('date:', day['date'], '| day scanned:', day['scanned'], '| macd golden:', len(day['golden']),
          '| death:', len(day['death']), '| near:', len(day['near']),
          '| kdj golden:', len(day['kdj_golden']), '| kdj death:', len(day['kdj_death']),
          '| week scanned:', sections['week']['scanned'], '| month scanned:', sections['month']['scanned'])

if __name__ == '__main__':
    main()
