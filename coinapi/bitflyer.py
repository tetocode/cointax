import csv
import io
import os
from collections import defaultdict
from typing import Generator

import ccxt
import pandas

from .ccxtclient import CCXTClient
from .ratelimiter import RateLimiter


class Client(CCXTClient):
    NAME = 'bitflyer'
    LIMIT = 500
    CACHE_LIMIT = 10000
    CCXT_CLASS = ccxt.bitflyer
    COLLECTIONS = ('report',
                   'crypto_deposit', 'crypto_withdrawal',
                   'fiat_deposit', 'fiat_withdrawal')

    def describe(self):
        desc = super().describe()
        desc['api']['private']['get'].extend([
            'getcollateralhistory',
        ])
        return desc

    def get_page_items(self, fn, parse, rps_limit: float, **params):
        # count, before, after
        last_id = None
        fn = RateLimiter(rps_limit, fn)
        limit = params.get('count', self.LIMIT)
        while True:
            params.update(count=limit)
            if last_id is not None:
                params.update(before=last_id)
            res = fn(params)
            for x in res:
                x = parse(x)
                yield x
                last_id = x['id']
            if len(res) < limit:
                break

    def public_executions(self, instrument: str, **params) -> Generator[dict, None, None]:
        def parse(data: dict):
            return dict(time=self.parse_time(data['exec_date'], self.UTC),
                        id=data['id'],
                        price=data['price'],
                        qty=data['size'],
                        data=data)

        product_code = self.instruments[instrument]['id']
        rps_limit = 500 / 60
        if os.environ.get('https_proxy'):
            rps_limit *= 5
            self.warning('rps_limit * 5')
        yield from self.get_page_items(self.publicGetGetexecutions, parse, rps_limit,
                                       product_code=product_code, **params)

    def executions(self, instrument: str) -> Generator[dict, None, None]:
        def parse(data: dict):
            return dict(time=self.parse_time(data['exec_date']),
                        id=data['id'],
                        data=data)

        product_code = self.instruments[instrument]['id']
        yield from self.get_page_items(self.privateGetExecutions, parse, 200 / 60,
                                       product_code=product_code)

    def crypto_deposits_all(self) -> Generator[dict, None, None]:
        def parse(data: dict):
            return dict(time=self.parse_time(data['event_date'], self.UTC),
                        id=data['id'],
                        data=data)

        yield from filter(lambda data: data['data']['status'] == 'COMPLETED',
                          self.get_page_items(self.privateGetGetcoinins, parse, 200 / 60))

    def crypto_withdrawals_all(self) -> Generator[dict, None, None]:
        def parse(data: dict):
            return dict(time=self.parse_time(data['event_date'], self.UTC),
                        id=data['id'],
                        data=data)

        yield from filter(lambda data: data['data']['status'] == 'COMPLETED',
                          self.get_page_items(self.privateGetGetcoinouts, parse, 200 / 60))

    def fiat_deposits_all(self) -> Generator[dict, None, None]:
        def parse(data: dict):
            return dict(time=self.parse_time(data['event_date'], self.UTC),
                        id=data['id'],
                        data=data)

        yield from filter(lambda data: data['data']['status'] == 'COMPLETED',
                          self.get_page_items(self.privateGetDeposits, parse, 200 / 60))

    def fiat_withdrawals_all(self) -> Generator[dict, None, None]:
        def parse(data: dict):
            return dict(time=self.parse_time(data['event_date'], self.UTC),
                        id=data['id'],
                        data=data)

        yield from filter(lambda data: data['data']['status'] == 'COMPLETED',
                          self.get_page_items(self.privateGetWithdrawals, parse, 200 / 60))

    @classmethod
    def load_reports_all_csv(cls, file_io):
        """
        取引日時,通貨,取引種別,価格,BTC,手数料(BTC),残高(BTC),JPY,残高(JPY),ETH,手数料(ETH),残高(ETH),LTC,手数料(LTC),残高(LTC),
        ETC,手数料(ETC),残高(ETC),BCH,手数料(BCH),残高(BCH),MONA,手数料(MONA),残高(MONA),注文 ID
        """
        types = {'売り', '出金', '外部送付', '証拠金預入', '入金', '買い', '証拠金引出', '手数料', '預入', '受取'}
        reader = csv.DictReader(file_io)
        fieldnames = ['取引日時', '通貨', '取引種別', '価格',
                      'BTC', '手数料(BTC)', '残高(BTC)',
                      'JPY', '残高(JPY)',
                      'ETH', '手数料(ETH)', '残高(ETH)',
                      'LTC', '手数料(LTC)', '残高(LTC)',
                      'ETC', '手数料(ETC)', '残高(ETC)',
                      'BCH', '手数料(BCH)', '残高(BCH)',
                      'MONA', '手数料(MONA)', '残高(MONA)',
                      'LSK', '手数料(LSK)', '残高(LSK)',
                      '注文 ID']
        assert reader.fieldnames == fieldnames, reader.fieldnames
        for i, data in enumerate(reader, 1):
            assert data['取引種別'] in types, 'unknown type {}'.format(data)
            yield dict(time=cls.parse_time(data['取引日時'], cls.JST),
                       id='report_#{}'.format(i),
                       data=data)

    def load_reports_all_csv_file(self, path: str):
        assert path.endswith('.csv'), path
        with open(path, 'r') as file:
            yield from self.load_reports_all_csv(file)

    def load_reports_all_xls_file(self, path: str):
        assert path.endswith('.xls'), path
        file = pandas.ExcelFile(path)
        assert len(file.sheet_names) == 1, file.sheet_names
        df = file.parse(file.sheet_names[0])
        yield from self.load_reports_all_csv(io.StringIO(df.to_csv(index=False)))

    @property
    def import_data_methods(self):
        name_methods = defaultdict(list)
        path_list = list(sorted(self.crypto_path.glob('*.xls')))
        assert path_list
        name_methods['report'].append((self.load_reports_all_xls_file, [str(path_list[-1])]))
        name_methods['crypto_deposit'].append((self.crypto_deposits_all, ()))
        name_methods['crypto_withdrawal'].append((self.crypto_withdrawals_all, ()))

        return name_methods

    def convert_data(self, name: str):
        def convert_one(doc: dict):
            data = doc['data']
            if name == 'report':
                kind = data['取引種別']
                currency = data['通貨']
                qty = ('/' not in currency) and float(data[currency])
                if kind in ('買い', '売り'):
                    instrument = currency
                    base, quote = instrument.split('/')
                    base_qty = float(data[base])
                    quote_qty = float(data[quote])
                    base_fee_qty = float(data.get('手数料({})'.format(base), 0))
                    quote_fee_qty = float(data.get('手数料({})'.format(quote), 0))
                    if kind == '買い':
                        assert base_qty > 0
                        assert quote_qty < 0
                        assert base_fee_qty <= 0
                        assert quote_fee_qty == 0
                        return dict(kind='spot',
                                    instrument=instrument,
                                    side='BUY',
                                    pnl=[
                                        (base, base_qty, 'in'),
                                        (quote, quote_qty, 'out'),
                                        (base, base_fee_qty, 'fee'),
                                    ])
                    else:
                        assert kind == '売り'
                        assert base_qty < 0
                        assert quote_qty > 0
                        assert base_fee_qty <= 0
                        assert quote_fee_qty == 0
                        return dict(kind='spot',
                                    instrument=instrument,
                                    side='SELL',
                                    pnl=[
                                        (quote, quote_qty, 'in'),
                                        (base, base_qty, 'out'),
                                        (base, base_fee_qty, 'fee'),
                                    ])
                elif kind == '預入':
                    assert qty > 0 and currency != 'JPY'
                    return  # dict(kind='deposit', pnl=(currency, qty, ''))
                elif kind == '外部送付':
                    assert qty < 0 and currency != 'JPY'
                    return  # dict(kind='withdrawal', pnl=(currency, qty, ''))
                elif kind == '入金':
                    assert qty > 0 and currency == 'JPY'
                    return dict(kind='deposit', pnl=(currency, qty, ''))
                elif kind == '出金':
                    assert qty < 0 and currency == 'JPY'
                    return dict(kind='withdrawal', pnl=(currency, qty, ''))
                elif kind in ('手数料',):
                    # 出金, 送金手数料
                    assert qty < 0
                    if currency != 'JPY':
                        return
                    return dict(kind='withdrawal_fee',
                                pnl=(currency, qty, 'fee'))
                elif kind in ('証拠金預入',):
                    assert qty < 0
                    return dict(kind='margin_deposit',
                                pnl=(currency, qty, ''))
                elif kind in ('証拠金引出',):
                    assert qty > 0
                    return dict(kind='margin_withdrawal',
                                pnl=(currency, qty, ''))
                elif kind in ('受取',):
                    assert qty > 0
                    return dict(kind='campaign',
                                pnl=(currency, qty, ''))
            if name == 'crypto_deposit':
                return dict(kind='deposit',
                            pnl=(data['currency_code'], float(data['amount']), ''))
            if name == 'crypto_withdrawal':
                qty = -float(data['amount'])
                assert qty < 0
                fee_qty = -data['fee']
                assert fee_qty < 0
                additional_fee = -data.get('additional_fee', 0)
                assert additional_fee <= 0
                qty -= additional_fee
                fee_qty += additional_fee
                return dict(kind='withdrawal',
                            pnl=[
                                (data['currency_code'], qty, ''),
                                (data['currency_code'], fee_qty, 'fee'),
                            ])
            assert False

        result = {}
        while True:
            _doc = yield result
            result = convert_one(_doc)
