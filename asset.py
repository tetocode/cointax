import re
from collections import defaultdict
from pprint import pprint
from typing import Dict

import gevent
import gevent.monkey
import gevent.pool
import lxml.html
import requests

import coinapi
from coinapi.clientbase import ClientBase

gevent.monkey.patch_all()

C_MAP = {
    'QSH': 'QASH',
}

TICKER_MAP = {
    'BCH': ('BCH/JPY', 'bitbankcc'),
    'BTC': ('BTC/JPY', 'bitflyer'),
    'ERC20.CMS': ('ERC20.CMS/JPY', 'zaif'),
    'ETH': ('ETH/JPY', 'zaif'),
    'JPYZ': ('JPYZ/JPY', 'zaif'),
    'MONA': ('MONA/JPY', 'zaif'),
    'PEPECASH': ('PEPECASH/JPY', 'zaif'),
    'QASH': ('QASH/JPY', 'quoinex'),
    'XRP': ('XRP/JPY', 'bitbankcc'),
    'XEM': ('XEM/JPY', 'zaif'),
    'ZAIF': ('ZAIF/JPY', 'zaif'),
}


def main():
    exchanges = [
        'bitbankcc',
        'bitfinex',
        'bitflyer',
        'bitmex',
        'coincheck',
        'kraken',
        'quoinex',
        'zaif',
    ]

    clients = {}  # type: Dict[str, ClientBase]
    pool = gevent.pool.Pool()
    for ex in exchanges:
        clients[ex] = getattr(coinapi, ex).Client()  # type: ClientBase

    def get_balance(key: str):
        balances = {}
        for k, v in clients[key].balance().items():
            balances[C_MAP.get(k, k)] = v
        return key, balances

    def get_tick(tick: str):
        instrument, ex = TICKER_MAP[tick]
        return instrument, clients[ex].tick(instrument)

    ticks = dict(pool.map(get_tick, list(TICKER_MAP)))
    balances = dict(pool.map(get_balance, exchanges))
    fx_rates = get_fxrates()

    currency_totals = defaultdict(lambda: dict(qty=.0, jpy=.0))
    totals = {}
    for ex, balance in sorted(balances.items()):
        ex_totals = defaultdict(lambda: dict(qty=.0, jpy=.0))
        for k, v in sorted(balance.items()):
            q = v['total']
            if not q:
                continue
            if k in TICKER_MAP:
                rate = ticks['{}/JPY'.format(k)]['bid']
            else:
                rate = fx_rates['{}/JPY'.format(k)]
            jpy = q * rate
            ex_totals[k]['qty'] += q
            ex_totals[k]['jpy'] += jpy
            ex_totals['_total']['jpy'] += jpy
            currency_totals[k]['qty'] += q
            currency_totals[k]['jpy'] += jpy
        print('# {}'.format(ex))
        pprint(dict(ex_totals))
        totals[ex] = ex_totals['_total']['jpy']

    print('# currency')
    pprint(dict(currency_totals))
    totals['_total'] = sum(totals.values())
    print('# jpy')
    pprint(dict(totals))


def get_fxrates():
    URL = 'https://info.finance.yahoo.co.jp/fx/convert/'
    res = requests.get(URL)
    root = lxml.html.fromstring(res.text)
    doms = root.cssselect('.fxRateTbl.fxList')
    usd = doms[0].text_content()
    jpy = doms[1].text_content()

    names = {
        'アメリカ　ドル.*': 'USD',
        '欧州　ユーロ.*': 'EUR',
        # 'ブラジル　レアル.*' : '',
        'オーストラリア　ドル.*': 'AUD',
        '中国　元.*': 'CNY',
        # 'イギリス　ポンド.*' : '',
        # '韓国　ウォン.*' : '',
        # 'ニュージーランド　ドル.*' : '',
        'シンガポール　ドル.*': 'SGD',
        # 'タイ　バーツ.*' : '',
        # '台湾　ドル.*' : '',
        # '南アフリカ　ランド.*' : '',
        # 'カナダ　ドル.*' : '',
        # 'トルコ　リラ.*' : '',
        '香港　ドル.*': 'HKD',
        # 'スイス　フラン.*' : '',
        # 'マレーシア　リンギット.*' : '',
        # 'メキシコ　ペソ.*' : '',
        'フィリピン　ペソ.*': 'PHP',
        'インド　ルピー.*': 'INR',
        'インドネシア　ルピア.*': 'IDR',
        # 'ロシア　ルーブル.*' : '',
        # 'スウェーデン　クローナ.*' : '',
        # 'ノルウェー　クローネ.*' : '',
        # 'デンマーク　クローネ.*' : '',
        # 'UAE　ディルハム.*' : '',
        # 'チリ　ペソ.*' : '',
        # 'ベネズエラ　ボリバル・フエルテ.*' : '',
        # 'クウェート　ディナール.*' : '',
        # 'サウジアラビア　リヤル.*' : '',
        # 'ルーマニア　レウ.*' : '',
        # 'パラグアイ　グァラニ.*' : '',
        # 'エジプト　ポンド.*' : '',
        # 'コロンビア　ペソ.*' : '',
        # 'ヨルダン　ディナール.*' : '',
        # 'ペルー　ソル.*' : '',
        # 'レバノン　ポンド.*' : '',
    }
    rates = {
        'JPY/JPY': 1.0
    }
    for m in re.finditer('(?P<name>[^\d]+?)(?P<rate>[\d.]+)', jpy):
        d = m.groupdict()
        for k, v in names.items():
            if re.search(k, d['name']):
                rates['{}/JPY'.format(v)] = float(d['rate'])
                break
    if not (100 <= rates.get('USD/JPY', 0) < 125):
        for k in rates:
            rates[k] = 0
    return rates


if __name__ == '__main__':
    main()
