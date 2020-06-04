import csv
import sys
from collections import defaultdict
from typing import Generator, TextIO

import ccxt

from .ccxtclient import CCXTClient
from .ratelimiter import RateLimiter


class Client(CCXTClient):
    NAME = 'coincheck'
    CCXT_CLASS = ccxt.coincheck
    LIMIT = 500
    COLLECTIONS = ('report', 'crypto_deposit', 'crypto_withdrawal', 'btc_execution', 'btc_position')

    def describe(self):
        desc = super().describe()
        desc['api']['private']['get'].extend([
            'deposits',
        ])
        return desc

    def get_page_items(self, fn, parse, page_key: str, rps_limit: float, **params):
        """
         limit 1ページあたりの取得件数を指定できます。
        order "desc", "asc" を指定できます。
        starting_after IDを指定すると絞り込みの開始位置を設定できます。
        ending_before IDを指定すると絞り込みの終了位置を設定できます。
        """
        fn = RateLimiter(rps_limit, fn)
        limit = self.LIMIT
        last_id = sys.maxsize
        while True:
            params.update(limit=limit, order='desc', starting_after=last_id)
            res = fn(params)
            data = res[page_key]
            for x in data:
                x = parse(x)
                yield x
                last_id = x['id']
            if len(data) < limit:
                break

    def btc_executions(self) -> Generator[dict, None, None]:
        def parse(data: dict):
            assert data['pair'] == 'btc_jpy', data
            return dict(time=self.parse_time(data['created_at']),
                        id=data['id'],
                        data=data)

        # exchange/orders/transactions_pagination
        yield from self.get_page_items(self.privateGetExchangeOrdersTransactionsPagination,
                                       parse, 'data', 60 / 60)

    def btc_positions(self) -> Generator[dict, None, None]:
        def parse(data: dict):
            assert data['pair'] == 'btc_jpy', data
            return dict(time=self.parse_time(data['closed_at']),
                        id=data['id'],
                        data=data)

        # GET /api/exchange/leverage/positions
        yield from self.get_page_items(self.privateGetExchangeLeveragePositions,
                                       parse, 'data', 60 / 60, status='closed')

    def crypto_deposits(self, currency: str) -> Generator[dict, None, None]:
        def parse(data: dict):
            return dict(time=self.parse_time(data['created_at']),
                        id='{}_{}'.format(currency, data['id']),
                        data=data)

        # deposit_money
        yield from self.get_page_items(self.privateGetDepositMoney, parse, 'deposits', 60 / 60,
                                       currency=currency)

    def crypto_withdrawals(self, currency: str) -> Generator[dict, None, None]:
        def parse(data: dict):
            return dict(time=self.parse_time(data['created_at']),
                        id='{}_{}'.format(currency, data['id']),
                        data=data)

        # deposit_money
        yield from self.get_page_items(self.privateGetSendMoney, parse, 'sends', 60 / 60,
                                       currency=currency)

    def fiat_deposits_all(self) -> Generator[dict, None, None]:
        def parse(data: dict):
            return dict(time=self.parse_time(data['created_at']),
                        id=data['id'],
                        data=data)

        # GET /api/deposits?
        yield from self.get_page_items(self.privateGetDeposits, parse, 'data', 60 / 60)

    def fiat_withdrawals_all(self) -> Generator[dict, None, None]:
        def parse(data: dict):
            return dict(time=self.parse_time(data['created_at']),
                        id=data['id'],
                        data=data)

        # GET /api/withdraws
        yield from self.get_page_items(self.privateGetWithdraws, parse, 'data', 60 / 60)

    def load_reports_all_csv(self, file: TextIO) -> Generator[dict, None, None]:
        """
        コインチェック株式会社
        ID,日付,操作内容,金額,通貨,JPY,BTC,ETH,ETC,DAO,LSK,FCT,XMR,REP,XRP,ZEC,XEM,LTC,DASH,BCH
        """
        title = file.readline()
        assert title and ('コインチェック株式会社' in title), title
        reader = csv.DictReader(file)
        fieldnames = ['ID', '日付', '操作内容', '金額', '通貨',
                      'JPY', 'BTC', 'ETH', 'ETC', 'DAO', 'LSK',
                      'FCT', 'XMR', 'REP', 'XRP', 'ZEC', 'XEM',
                      'LTC', 'DASH', 'BCH']
        assert reader.fieldnames == fieldnames, reader.fieldnames
        for row in reader:
            if row and row['ID']:
                yield dict(time=self.parse_time(row['日付']),
                           id='report_{}'.format(row['ID']),
                           data=row)

    def load_reports_all_csv_file(self, path: str) -> Generator[dict, None, None]:
        with open(path, 'r') as file:
            yield from self.load_reports_all_csv(file)

    @property
    def import_data_methods(self):
        name_methods = defaultdict(list)
        for path in self.crypto_path.glob('*.csv'):
            name_methods['report'].append((self.load_reports_all_csv_file,
                                           [str(path)]))
        print('#', self.currencies)
        for currency in ['BTC', 'XRP']:
            name_methods['crypto_deposit'].append((self.crypto_deposits, [currency]))
            name_methods['crypto_withdrawal'].append((self.crypto_withdrawals, [currency]))
        name_methods['btc_execution'].append((self.btc_executions, ()))
        name_methods['btc_position'].append((self.btc_positions, ()))

        return name_methods

    def convert_data(self, name: str):
        trades = []

        def convert_one(doc: dict):
            data = doc['data']
            pair = data.get('pair')
            currency = data.get('currency')
            instrument = pair and self.rinstruments[pair]
            side = data.get('side', '').upper()

            if name == 'report':
                nonlocal trades
                kind = data['操作内容']
                currency = data['通貨']
                qty = float(data['金額'])
                if kind in ('購入', '売却'):
                    trades.append(data)
                    if len(trades) < 2:
                        return
                    if kind == '購入':
                        # BUY
                        side = 'BUY'
                        base, quote = trades[1]['通貨'], trades[0]['通貨']
                        base_qty, quote_qty = trades[1]['金額'], trades[0]['金額']
                    else:
                        # SELL
                        assert kind == '売却'
                        side = 'SELL'
                        base, quote = trades[0]['通貨'], trades[1]['通貨']
                        base_qty, quote_qty = trades[0]['金額'], trades[1]['金額']

                    instrument = '{}/{}'.format(base, quote)
                    trades = []
                    return dict(kind='spot',
                                instrument=instrument,
                                side=side,
                                pnl=[
                                    (base, float(base_qty), ''),
                                    (quote, float(quote_qty), ''),
                                ])
                if kind in ('入金',
                            '指値注文', '指値注文をキャンセル', '取引が成約',
                            '購入', '売却',):
                    return
                if kind in ('送金',):
                    if qty > 0:
                        # キャンセル
                        return dict(kind='cancel_withdrawal',
                                    pnl=(currency, qty, ''))
                    return
                if kind in ('振替',):
                    return  # dict(kind='margin_transfer', pnl=(currency, qty, ''))
            elif name == 'btc_execution':
                pnl = []
                for c, qty in data['funds'].items():
                    pnl.append((c.upper(), float(qty), ''))
                fee_qty = -float(data['fee'])
                assert fee_qty <= 0
                if data['fee_currency']:
                    pnl.append((data['fee_currency'], fee_qty, 'fee'))
                return dict(kind='spot',
                            instrument=instrument,
                            side=side,
                            pnl=pnl)

            elif name == 'btc_position':
                pl = float(data['pl'])
                return dict(kind='margin',
                            pnl=('JPY', pl, ''))
            if name in ('crypto_deposit',):
                qty = float(data['amount'])
                fast_fee = -float(data['fast_fee'] or 0)
                assert qty > 0
                assert fast_fee <= 0
                pnl = [(currency, qty, '')]
                if fast_fee:
                    pnl.append((currency, fast_fee, 'fast_fee'))
                return dict(kind='deposit',
                            pnl=pnl)
            if name in ('crypto_withdrawal',):
                qty = -float(data['amount'])
                fee_qty = -float(data['fee'])
                assert qty < 0
                assert fee_qty < 0
                return dict(kind='withdrawal',
                            pnl=[
                                (currency, qty, ''),
                                (currency, fee_qty, 'fee'),
                            ])
            assert False

        result = {}
        while True:
            _doc = yield result
            result = convert_one(_doc)
