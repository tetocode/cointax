from collections import defaultdict, OrderedDict

import python_bitbankcc

from .clientbase import ClientBase


class Client(ClientBase):
    NAME = 'bitbankcc'
    COLLECTIONS = ('daily_report',)
    INSTRUMENTS = [
        'BTC/JPY', 'XRP/JPY', 'LTC/BTC', 'ETH/BTC',
        'MONA/JPY', 'MONA/BTC', 'BCH/JPY', 'BCH/BTC',
    ]
    CURRENCY_MAP = {'BCH': 'BCC'}

    def __init__(self, api_key: str = None, api_secret: str = None, timeout: float = None, **__):
        super().__init__(api_key, api_secret, timeout)
        self.public = python_bitbankcc.public()
        self.private = python_bitbankcc.private(self.api_key, self.api_secret)

    def get_currencies(self):
        currencies = {}
        for k, v in self.instruments.items():
            base, quote = k.split('/')
            currencies[base] = dict(name=base, id=self.CURRENCY_MAP.get(base, base))
            currencies[quote] = dict(name=quote, id=self.CURRENCY_MAP.get(quote, quote))
        currencies = OrderedDict((k, currencies[k]) for k in sorted(currencies))
        return currencies

    def get_instruments(self):
        results = OrderedDict()

        def map_currency(_c: str):
            return self.CURRENCY_MAP.get(_c, _c)

        for instrument in sorted(self.INSTRUMENTS):
            base, quote = instrument.split('/')
            results[instrument] = {
                'id': '{}_{}'.format(map_currency(base), map_currency(quote)).lower(),
                'base': base,
                'quote': quote
            }
        return results

    def tick(self, instrument: str):
        pair = self.instruments[instrument]['id']
        res = self.public.get_ticker(pair)
        res.update(bid=float(res['buy']), ask=float(res['sell']))
        return res

    def balance(self):
        balances = defaultdict(lambda: dict(total=.0, used=.0, free=.0))
        res = self.private.get_asset()
        for asset in res['assets']:
            total = float(asset['onhand_amount'])
            free = float(asset['free_amount'])
            used = float(asset['locked_amount'])
            currency = self.rcurrencies[asset['asset'].upper()]
            d = balances[currency]
            d['total'] = total
            d['free'] = free
            d['used'] = used
        return balances

    @property
    def import_data_methods(self):
        name_methods = defaultdict(list)

        # RPT016-CUSTOMER-DAILY-REPORT-みんなのビットコイン株式会社-54397-20180128.json
        for path in self.crypto_path.glob('*CUSTOMER-DAILY-REPORT-みんなのビットコイン株式会社*.json'):
            name_methods['daily_report'].append((self.load_reports_all_json_file, [str(path)]))

        return name_methods

    def convert_data(self, name: str):
        def to_float(s: str):
            if s == '-':
                return .0
            return float(s.replace(',', ''))

        def convert_one(doc: dict):
            data = doc['data']
            currency = data['currency']
            kind = data['kind']

            if kind == 'fiat_balance':
                if data['摘要'] in ('売', '買'):
                    return
                if data['摘要'] in ('ロールオーバー',):
                    qty = to_float(data['ポジション料'])
                    assert qty < 0
                    return dict(kind='margin_fee',
                                pnl=(currency, qty, 'position_fee'))
                if data['摘要'] in ('新規', '決済',):
                    pnl = []
                    pnl_qty = to_float(data['金額'])
                    fee_qty = to_float(data['手数料(内税)'])
                    position_fee_qty = to_float(data['ポジション料'])
                    assert fee_qty <= 0 and position_fee_qty <= 0
                    if pnl_qty != 0:
                        pnl.append((currency, pnl_qty, ''))
                    if fee_qty < 0:
                        pnl.append((currency, fee_qty, 'fee'))
                    if position_fee_qty:
                        pnl.append((currency, position_fee_qty, 'position_fee'))
                    return dict(kind='margin',
                                pnl=pnl)
                if data['摘要'] == '出金':
                    qty = to_float(data['入出金'])
                    fee_qty = to_float(data['手数料(内税)'])
                    assert qty < 0 and fee_qty < 0
                    return dict(kind='withdrawal',
                                pnl=[
                                    (currency, qty, ''),
                                    (currency, fee_qty, 'fee'),
                                ])

            if kind == 'spot':
                base, quote = data['仮想通貨名'], currency
                instrument = '{}/{}'.format(base, quote)
                base_qty = abs(to_float(data['約定数量']))
                quote_qty = abs(to_float(data['約定金額']))
                fee_qty = to_float(data['手数料(内税)'])
                assert base_qty > 0 and quote_qty > 0 >= fee_qty
                if data['区分'] == '買':
                    side = 'BUY'
                    pnl = [
                        (base, base_qty, 'in'),
                        (quote, -quote_qty, 'out'),
                        (quote, fee_qty, 'fee'),
                    ]
                else:
                    assert data['区分'] == '売'
                    side = 'SELL'
                    pnl = [
                        (quote, quote_qty, 'in'),
                        (base, -base_qty, 'out'),
                        (quote, fee_qty, 'fee'),
                    ]
                return dict(kind='spot',
                            instrument=instrument,
                            side=side,
                            pnl=pnl)

            if kind == 'crypto_balance':
                if data['種別'] in ('買', '売', '交換(買)', '交換(売)'):
                    return
                if data['種別'] == '入金':
                    qty = to_float(data['入出金'])
                    assert qty > 0
                    return dict(kind='deposit',
                                pnl=(currency, qty, ''))
                if data['種別'] == '出金':
                    qty = to_float(data['入出金'])
                    fee_qty = to_float(data['手数料(内税)'])
                    assert qty < 0 and fee_qty <= 0
                    return dict(kind='withdrawal',
                                pnl=[
                                    (currency, qty, ''),
                                    (currency, fee_qty, 'fee'),
                                ])

            if kind == 'margin':
                return

            if kind == 'position':
                return

            assert False

        result = {}
        while True:
            _doc = yield result
            result = convert_one(_doc)
