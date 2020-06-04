import csv
import tempfile
import time
from collections import OrderedDict, defaultdict
from datetime import datetime
from typing import Generator, TextIO

import ccxt
import requests
from dateutil.relativedelta import relativedelta
from requests import HTTPError

from .ccxtclient import CCXTClient
from .ratelimiter import RateLimiter


class Client(CCXTClient):
    NAME = 'zaif'
    CCXT_CLASS = ccxt.zaif
    LIMIT = 500
    CACHE_LIMIT = 10000
    FIAT_CURRENCIES = {'JPY', }
    COLLECTIONS = ('tip_send_report', 'tip_receive_report', 'bonus_report',
                   'execution', 'position',
                   'deposit', 'withdrawal',
                   'deposit_report', 'withdrawal_report')
    COLLATERAL_CURRENCIES = ('JPY', 'BTC', 'MONA', 'XEM')

    @classmethod
    def make_nonce(cls):
        return time.time()

    def _handle_error(self, e: Exception):
        s = str(e)
        if 'Bad Gateway' in s:
            self.info('Bad Gateway')
            return
        else:
            self.warning(str(e))
            return self.RATE_LIMIT_INTERVAL

    def get_instruments(self):
        instruments = super().get_instruments()
        futures = {}
        for future in self.fapiGetGroupsGroupId(dict(group_id='all')):
            pair = future['currency_pair']
            base, quote = pair.upper().split('_')
            future['base'] = base
            future['quote'] = quote
            future['future'] = True
            future['group_id'] = future['id']
            for v in instruments.values():
                if future['currency_pair'] == v['info']['currency_pair']:
                    future['info'] = v['info'].copy()
                    future['info']['is_future'] = True
                    break
            if future['id'] == 1 and future['use_swap']:
                instrument = 'AIR_FX'
                futures[instrument] = future
            else:
                instrument = '{}_FUTURE_{}'.format(pair.upper(), future['id'])
            future['instrument'] = instrument
            futures[instrument] = future
        instruments.update(futures)
        for x in instruments.values():
            x['decimals'] = x['info']['aux_unit_point']
            x['currency_pair'] = x['info']['currency_pair']
            x['future'] = x.get('future', False)
        return OrderedDict((k, instruments[k]) for k in sorted(instruments))

    def get_page_items(self, fn, parse, rps_limit: float, **params):
        limit = self.LIMIT
        from_i = 0
        params = params.copy()
        params.update(limit=limit)
        fn = RateLimiter(rps_limit, fn)
        cache = OrderedDict()
        while True:
            params['from'] = from_i
            res = fn(params)['return']
            if not len(res):
                break
            for k, v in sorted(res.items(), key=lambda _x: int(_x[0]), reverse=True):
                k = int(k)
                if k not in cache:
                    cache[k] = True
                    x = parse(k, v)
                    if len(cache) > self.CACHE_LIMIT:
                        cache.popitem(last=False)
                    yield x
            from_i += len(res)

    def executions(self, instrument: str) -> Generator[dict, None, None]:
        def parse(k: int, v: dict):
            return dict(time=self.utc_from_timestamp(float(v['timestamp'])),
                        id='{}_{}'.format(instrument, k),
                        data=v)

        info = self.instruments[instrument]
        if info['future']:
            return
        currency_pair = info['id']
        yield from self.get_page_items(self.privatePostTradeHistory, parse, 60 / 60,
                                       currency_pair=currency_pair)

    def positions(self, instrument: str) -> Generator[dict, None, None]:
        def parse(k: int, v: dict):
            return dict(time=self.utc_from_timestamp(float(v['timestamp_closed'])),
                        id='{}_{}'.format(instrument, k),
                        data=v)

        info = self.instruments[instrument]
        if info['future']:
            yield from self.get_page_items(self.tlapiPostGetPositions, parse, 60 / 60,
                                           type='futures', group_id=info['id'])
        else:
            yield from self.get_page_items(self.tlapiPostGetPositions, parse, 60 / 60,
                                           type='margin', currency_pair=info['id'])

    def deposits(self, currency: str) -> Generator[dict, None, None]:
        def parse(k: int, v: dict):
            return dict(time=self.utc_from_timestamp(float(v['timestamp'])),
                        id='{}_{}'.format(currency, k),
                        data=v)

        currency = self.currencies[currency]['id']
        yield from self.get_page_items(self.privatePostDepositHistory, parse, 60 / 60,
                                       currency=currency.lower())

    def withdrawals(self, currency: str) -> Generator[dict, None, None]:
        def parse(k: int, v: dict):
            return dict(time=self.utc_from_timestamp(float(v['timestamp'])),
                        id='{}_{}'.format(currency, k),
                        data=v)

        currency = self.currencies[currency]['id']
        yield from self.get_page_items(self.privatePostWithdrawHistory, parse, 60 / 60,
                                       currency=currency.lower())

    def load_crypto_deposits_csv(self, currency: str, file: TextIO):
        if currency in self.FIAT_CURRENCIES:
            return
        reader = csv.DictReader(file)
        assert reader.fieldnames == ['日時', '金額', 'TX'], reader.fieldnames
        for i, data in enumerate(reader, 1):
            yield dict(time=self.parse_time(data['日時'], self.JST),
                       id='{}_#{}'.format(currency, i),
                       data=data)

    def load_crypto_deposits_csv_file(self, currency: str, path: str):
        with open(path, 'r') as file:
            yield from self.load_crypto_deposits_csv(currency, file)

    def load_crypto_withdrawals_csv(self, currency: str, file: TextIO):
        if currency in self.FIAT_CURRENCIES:
            return
        reader = csv.DictReader(file)
        assert reader.fieldnames == ['日時', '金額', '手数料', 'TX', 'アドレス'], reader.fieldnames
        for i, data in enumerate(reader, 1):
            yield dict(time=self.parse_time(data['日時'], self.JST),
                       id='{}_#{}'.format(currency, i),
                       data=data)

    def load_crypto_withdrawals_csv_file(self, currency: str, path: str):
        with open(path, 'r') as file:
            yield from self.load_crypto_withdrawals_csv(currency, file)

    def load_fiat_deposits_csv(self, currency: str, file: TextIO):
        if currency not in self.FIAT_CURRENCIES:
            return
        reader = csv.DictReader(file)
        assert reader.fieldnames == ['日時', '金額'], reader.fieldnames
        for i, data in enumerate(reader, 1):
            yield dict(time=self.parse_time(data['日時'], self.JST),
                       id='{}_#{}'.format(currency, i),
                       data=data)

    def load_fiat_deposits_csv_file(self, currency: str, path: str):
        with open(path, 'r') as file:
            yield from self.load_fiat_deposits_csv(currency, file)

    def load_fiat_withdrawals_csv(self, currency: str, file: TextIO):
        if currency not in self.FIAT_CURRENCIES:
            return
        reader = csv.DictReader(file)
        assert reader.fieldnames == ['日時', '銀行', '支店',
                                     '口座種別', '口座番号', '口座名義',
                                     '金額', '手数料'], reader.fieldnames
        for i, data in enumerate(reader, 1):
            yield dict(time=self.parse_time(data['日時'], self.JST),
                       id='{}_#{}'.format(currency, i),
                       data=data)

    def load_fiat_withdrawals_csv_file(self, currency: str, path: str):
        with open(path, 'r') as file:
            yield from self.load_fiat_withdrawals_csv(currency, file)

    def load_bonus_all_csv(self, file: TextIO):
        reader = csv.DictReader(file)
        fieldnames = ['支払日時', '日付', '支払ボーナス', '支払通貨']
        assert reader.fieldnames == fieldnames, reader.fieldnames
        for i, data in enumerate(reader, 1):
            yield dict(time=self.parse_time(data['支払日時'], self.JST),
                       id='bonus_#{}'.format(i),
                       data=data)

    def load_bonus_all_csv_file(self, path: str):
        with open(path, 'r') as file:
            yield from self.load_bonus_all_csv(file)

    def load_tip_receive_all_csv(self, file: TextIO):
        reader = csv.DictReader(file)
        fieldnames = ['日時', 'タイプ', '送信者', 'BTC', 'MONA', 'XEM', 'トークン', '状態', '更新日時']
        assert reader.fieldnames == fieldnames, reader.fieldnames
        for i, data in enumerate(reader, 1):
            yield dict(time=self.parse_time(data['更新日時'], self.JST),
                       id='tip_receive_#{}'.format(i),
                       data=data)

    def load_tip_receive_all_csv_file(self, path: str):
        with open(path, 'r') as file:
            yield from self.load_tip_receive_all_csv(file)

    def load_tip_send_all_csv(self, file: TextIO):
        reader = csv.DictReader(file)
        fieldnames = ['日時', 'タイプ', '宛先', 'BTC', 'MONA', 'XEM', 'トークン', '状態', '更新日時']
        assert reader.fieldnames == fieldnames, reader.fieldnames
        for i, data in enumerate(reader, 1):
            yield dict(time=self.parse_time(data['更新日時'], self.JST),
                       id='tip_send_#{}'.format(i),
                       data=data)

    def load_tip_send_all_csv_file(self, path: str):
        with open(path, 'r') as file:
            yield from self.load_tip_send_all_csv(file)

    @property
    def import_data_methods(self):
        crypto_path = self.crypto_path
        name_methods = defaultdict(list)
        name_methods['tip_send_report'].append((self.load_tip_send_all_csv_file,
                                                [str(crypto_path / 'tip_send.csv')]))
        name_methods['tip_receive_report'].append((self.load_tip_receive_all_csv_file,
                                                   [str(crypto_path / 'tip_receive.csv')]))
        name_methods['bonus_report'].append((self.load_bonus_all_csv_file,
                                             [str(crypto_path / 'obtain_bonus.csv')]))
        for instrument in self.instruments:
            name_methods['execution'].append((self.executions, [instrument]))
            name_methods['position'].append((self.positions, [instrument]))
        api_support_currencies = ['JPY', 'BTC']
        for currency in api_support_currencies:
            name_methods['deposit'].append((self.deposits, [currency]))
            name_methods['withdrawal'].append((self.withdrawals, [currency]))

        for currency in self.currencies:
            if currency in api_support_currencies:
                continue
            path = crypto_path / '{}_deposit.csv'.format(currency.lower())
            if path.exists():
                name_methods['deposit_report'].append((self.load_crypto_deposits_csv_file,
                                                       [currency, str(path)]))
            path = crypto_path / '{}_withdraw.csv'.format(currency.lower())
            if path.exists():
                name_methods['withdrawal_report'].append((self.load_crypto_withdrawals_csv_file,
                                                          [currency, str(path)]))

        return name_methods

    def convert_data(self, name: str):
        def convert_one(doc: dict):
            data = doc['data']
            currency = doc['id'].split('_')[0]

            if name == 'deposit_report':
                qty = float(data['金額'])
                assert qty > 0
                return dict(kind='deposit',
                            pnl=(currency, qty, ''))
            if name == 'deposit':
                qty = float(data['amount'])
                assert qty > 0
                return dict(kind='deposit',
                            pnl=(currency, qty, ''))
            if name == 'withdrawal_report':
                qty = -float(data['金額'])
                fee_qty = -float(data['手数料'])
                assert qty < 0 and fee_qty < 0
                return dict(kind='withdrawal',
                            pnl=[
                                (currency, qty, ''),
                                (currency, fee_qty, 'fee')
                            ])
            if name == 'withdrawal':
                qty = -float(data['amount'])
                fee_qty = -float(data['fee'])
                assert qty < 0 and fee_qty < 0
                return dict(kind='withdrawal',
                            pnl=[
                                (currency, qty, ''),
                                (currency, fee_qty, 'fee')
                            ])
            if name == 'bonus_report':
                currency = data['支払通貨'].upper()
                qty = float(data['支払ボーナス'])
                assert qty > 0
                return dict(kind='bonus',
                            pnl=(currency, qty, ''))
            if name in ('tip_receive_report', 'tip_send_report'):
                is_send = 'send' in name
                if not is_send and data['状態'] != '受取済':
                    return
                pnl = []
                for k in ['BTC', 'MONA', 'XEM']:
                    qty = float(data[k])
                    if qty >= 0:
                        if is_send:
                            qty = -qty
                        pnl.append((k, qty, 'tip'))
                token_data = data['トークン'].split(',')
                assert len(token_data) <= 2
                # [iterable[x:x + n] for x in range(0, len(iterable), n)]
                for token, amount in zip(token_data[::2], token_data[1::2]):
                    qty = float(amount)
                    if qty >= 0:
                        if is_send:
                            qty = -qty
                        pnl.append((token, qty, 'tip'))
                return dict(kind='tip_send' if is_send else 'tip_receive',
                            pnl=pnl)
            if name == 'execution':
                instrument = currency
                base, quote = instrument.split('/')
                base_qty = float(data['amount'])
                quote_qty = float(data['price']) * base_qty
                fee_qty = float(data['fee_amount'])
                assert base_qty > 0 and quote_qty > 0 and fee_qty >= 0
                assert float(data.get('fee', 0)) == 0

                def get_spot(side: str, with_fee: bool = True):
                    if side == 'BUY':
                        _pnl = [
                            (base, base_qty, 'in'),
                            (quote, -quote_qty, 'out'),
                        ]
                        if with_fee:
                            _pnl.append((base, -fee_qty, 'fee'))
                    else:
                        assert side == 'SELL'
                        _pnl = [
                            (quote, quote_qty, 'in'),
                            (base, -base_qty, 'out'),
                        ]
                        if with_fee:
                            _pnl.append((quote, -fee_qty, 'fee'))
                    return dict(kind='spot',
                                instrument=instrument,
                                side=side,
                                pnl=_pnl)

                if data['your_action'] == 'bid':
                    return get_spot('BUY')
                if data['your_action'] == 'ask':
                    return get_spot('SELL')
                assert data['your_action'] == 'both'
                if data['action'] == 'ask':
                    # first action is bid
                    spots = [get_spot('BUY', False), get_spot('SELL')]
                else:
                    assert data['action'] == 'bid'
                    spots = [get_spot('SELL', False), get_spot('BUY')]
                for i, spot in enumerate(spots):
                    spot.update(id='{}_{}'.format(doc['id'], i))
                return spots

            if name == 'position':
                if 'timestamp_closed' not in data:
                    return None
                pnl = []
                for c in self.COLLATERAL_CURRENCIES:
                    key = c.lower()
                    fund = data.get('deposit_{}'.format(key), 0)
                    refund = data.get('refunded_{}'.format(key))
                    if fund:
                        assert refund
                    if refund is None:
                        continue
                    pnl.append((c, refund - fund, ''))
                return dict(kind='margin',
                            pnl=pnl)
            assert False

        result = {}
        while True:
            _doc = yield result
            result = convert_one(_doc)

    def public_executions_asc(self, instrument: str, start: datetime, stop: datetime, **params):
        dt = start.astimezone(self.JST)
        while dt < stop:
            try:
                csv_text = self.download_execution_csv(instrument, dt.year, dt.month)
                with tempfile.TemporaryFile('w+') as file:
                    file.write(csv_text)
                    file.seek(0)
                    for i, data in enumerate(csv.DictReader(file)):
                        t = self.parse_time(data['timestamp'], self.JST)
                        if start <= t < stop:
                            yield dict(time=t,
                                       id='#{}'.format(i),
                                       price=float(data['price']),
                                       qty=float(data['amount']),
                                       data=data)
            except HTTPError as e:
                if e.response.status_code != 404:
                    raise
                self.warning('csv not found {} {:04d}/{:02d}'.format(instrument, dt.year, dt.month))
            finally:
                dt += relativedelta(months=1)

    def download_execution_csv(self, instrument: str, year: int, month: int):
        url_format = 'https://zaif.jp/more_data/{pair}/csv/{year:04d}/{pair}_{year:04d}_{month:02d}.csv'
        pair = self.instruments[instrument]['id']
        with requests.Session() as s:
            res = s.get(url_format.format(pair=pair, year=year, month=month))
            if not res.ok:
                res.raise_for_status()
            return res.text
