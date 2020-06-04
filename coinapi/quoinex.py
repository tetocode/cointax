import sys
from collections import OrderedDict, defaultdict
from datetime import datetime
from pprint import pformat
from typing import Generator, Optional, Dict

import ccxt
import os

from .ccxtclient import CCXTClient
from .ratelimiter import RateLimiter


class _Quoinex(ccxt.quoinex):
    def describe(self):
        desc = super().describe()
        desc['api']['private']['get'].extend([
            'accounts',
            'fund_infos',  # fiat
            'transactions',  # fiat, crypto
            'withdrawals',  # fiat
            'crypto_withdrawals',  # crypto
        ])
        return desc


class Client(CCXTClient):
    NAME = 'quoinex'
    CCXT_CLASS = _Quoinex
    LIMIT = 500
    CACHE_LIMIT = 10000
    FIAT_CURRENCIES = {'AUD', 'CNY', 'EUR', 'HKD', 'IDR', 'INR', 'JPY', 'PHP', 'SGD', 'USD'}
    CRYPTO_CURRENCIES = {'BCH', 'BTC', 'ETH', 'QASH', 'XRP'}
    SUPPORTED_CURRENCIES = FIAT_CURRENCIES | CRYPTO_CURRENCIES
    UNSUPPORTED_CURRENCIES = {'DASH', 'NEO', 'QTUM', 'UBTC'}
    COLLECTIONS = ('order', 'transaction')

    def get_instruments(self):
        instruments = super().get_instruments()
        supported = self.SUPPORTED_CURRENCIES
        unsupported = self.UNSUPPORTED_CURRENCIES
        all_currencies = supported | unsupported
        for k, v in tuple(instruments.items()):
            subset = {v['base'], v['quote']}
            if not subset.issubset(all_currencies):
                raise Exception('unsupported currency in {}'.format(k))
            if not subset.issubset(supported):
                del instruments[k]
        return instruments

    @property
    def rinstruments(self, *_):
        if not self._rinstruments:
            # convert str -> int
            self._rinstruments = OrderedDict((int(v['id']), k) for k, v in self.instruments.items())
        return self._rinstruments

    def get_page_items(self, fn, parse, rps_limit: float, **params):
        limit = self.LIMIT
        params = params.copy()
        page = 1
        cache = OrderedDict()
        fn = RateLimiter(rps_limit, fn)
        while True:
            params.update(page=page, limit=limit)
            res = fn(params)
            for model in res['models']:
                item = parse(model)
                if item['id'] not in cache:
                    cache[item['id']] = True
                    if len(cache) > self.CACHE_LIMIT:
                        cache.popitem(last=False)
                    yield item
                else:
                    self.warning('#duplicate entry {}'.format(pformat(model)))
            if page >= res.get('total_pages', sys.maxsize):
                break
            page += 1

    def public_executions_asc(self, instrument: str,
                              start: datetime, stop: datetime, **params):
        """
        Parameters 	Optional? 	Description
        currency_pair_code 		e.g. BTCJPY
        timestamp 		Only show executions at or after this timestamp (Unix timestamps in seconds)
        limit 	yes 	How many executions should be returned. Must be <= 1000. Default is 20
        """
        timestamp = int(start.timestamp())
        rps_limit = 300 / 300
        if os.environ.get('https_proxy'):
            rps_limit *= 5
            self.warning('rps_limit * 5')
        fn = RateLimiter(rps_limit, self.publicGetExecutions)
        limit = 1000
        params.update(dict(product_id=self.instruments[instrument]['id'], limit=limit))
        while True:
            params.update(timestamp=timestamp)
            res = fn(params)
            for x in res:
                dt = self.utc_from_timestamp(float(x['created_at']))
                if stop <= dt:
                    return
                yield dict(time=dt,
                           id=x['id'],
                           price=float(x['price']),
                           qty=float(x['quantity']),
                           data=x)
                timestamp = x['created_at'] + 1
            if len(res) < limit:
                break

    def executions(self, instrument: str = None) -> Generator[dict, None, None]:
        def parse(data: dict):
            items = []
            _instrument = self.rinstruments[data['product_id']]
            order_fee = float(data['order_fee'])
            filled_qty = float(data['filled_quantity'])

            for execution in data['executions']:
                side = data['side'].upper()
                assert side in ('BUY', 'SELL')
                qty = float(execution['quantity'])
                items.append(self.make_data(self.utc_from_timestamp(float(execution['created_at'])),
                                            'spot_execution',
                                            '{}_{}'.format(data['id'], execution['id']),
                                            target=data['target'],
                                            instrument=_instrument,
                                            side=side,
                                            source_action=data.get('source_action', ''),
                                            order_id=data['id'],
                                            order_fee=order_fee * qty / filled_qty,
                                            data=execution))
            return items

        params = {}
        if instrument:
            params.update(product_id=self.instruments[instrument]['id'])
        yield from self.get_page_items(self.privateGetOrders, parse,
                                       with_details=1, **params)

    def orders_all(self) -> Generator[dict, None, None]:
        yield from self.orders(None)

    def orders(self, instrument: Optional[str]) -> Generator[dict, None, None]:
        def parse(data: dict):
            return dict(time=self.utc_from_timestamp(float(data['created_at'])),
                        id=data['id'],
                        data=data)

        params = {}
        if instrument:
            params.update(product_id=self.instruments[instrument]['id'])
        yield from self.get_page_items(self.privateGetOrders, parse, 300 / 300,
                                       with_details=1, **params)

    def positions_all(self) -> Generator[dict, None, None]:
        yield from self.positions(None)

    def positions(self, currency: Optional[str]) -> Generator[dict, None, None]:
        def parse(data: dict):
            return dict(time=self.utc_from_timestamp(float(data['updated_at'])),
                        id=data['id'],
                        data=data)

        params = {}
        if currency:
            params.update(funding_currency=currency)
        yield from self.get_page_items(self.privateGetTrades, parse, 300 / 300, **params)

    def transactions(self, currency: str) -> Generator[dict, None, None]:
        def parse(data: dict):
            return dict(time=self.utc_from_timestamp(float(data['created_at'])),
                        id='{}_{}'.format(currency, data['id']),
                        data=data)

        yield from self.get_page_items(self.privateGetTransactions, parse, 300 / 300,
                                       currency=currency, with_details=1)

    def fiat_deposits(self, currency: str) -> Generator[dict, None, None]:
        if currency not in self.FIAT_CURRENCIES:
            return

        def parse(data: dict):
            return dict(time=self.utc_from_timestamp(float(data['created_at'])),
                        id='{}_{}'.format(currency, data['id']),
                        data=data)

        yield from self.get_page_items(self.privateGetFundInfos, parse, 300 / 300,
                                       currency=currency)

    def fiat_withdrawals(self, currency: str) -> Generator[dict, None, None]:
        if currency not in self.FIAT_CURRENCIES:
            return

        def parse(data: dict):
            return dict(time=self.utc_from_timestamp(float(data['created_at'])),
                        id='{}_{}'.format(currency, data['id']),
                        data=data)

        yield from self.get_page_items(self.privateGetWithdrawals, parse, 300 / 300,
                                       currency=currency)

    def crypto_deposits(self, currency: str) -> Generator[dict, None, None]:
        if currency not in self.CRYPTO_CURRENCIES:
            return

        def parse(data: dict):
            return dict(time=self.utc_from_timestamp(float(data['created_at'])),
                        id='{}_{}'.format(currency, data['id']),
                        data=data)

        yield from self.get_page_items(self.privateGetTransactions, parse, 300 / 300,
                                       currency=currency, transaction_type='funding')

    def crypto_withdrawals(self, currency: str) -> Generator[dict, None, None]:
        if currency not in self.CRYPTO_CURRENCIES:
            return

        def parse(data: dict):
            return dict(time=self.utc_from_timestamp(float(data['created_at'])),
                        id='{}_{}'.format(currency, data['id']),
                        data=data)

        yield from self.get_page_items(self.privateGetCryptoWithdrawals, parse, 300 / 300,
                                       currency=currency)

    @property
    def import_data_methods(self):
        name_methods = defaultdict(list)
        name_methods['order'].append((self.orders_all, []))
        for currency in sorted(self.FIAT_CURRENCIES | self.CRYPTO_CURRENCIES):
            name_methods['transaction'].append((self.transactions, [currency]))

        return name_methods

    def accounts(self) -> Dict[str, dict]:
        res = self.privateGetAccounts()
        accounts = res['fiat_accounts'] + res['crypto_accounts']
        accounts = {account['currency']: account for account in accounts}
        return OrderedDict({k: accounts[k] for k in sorted(accounts)})

    def convert_data(self, name: str):
        account_ids = {k: v['id'] for k, v in self.accounts().items()}

        def convert_one(doc: dict):
            data = doc['data']

            if name == 'transaction':
                transaction_type = data['transaction_type']
                assert data['gross_amount'] == data['net_amount']
                qty = float(data['gross_amount'])
                currency = doc['id'].split('_')[0]
                assert currency in self.SUPPORTED_CURRENCIES

                if transaction_type == 'trade':
                    # skip, use order data
                    return
                if transaction_type == 'trade_fee':
                    # skip, use order data
                    return

                from_id = data.get('from_fiat_account_id')
                from_crypto_id = data.get('from_account_id')
                to_id = data.get('to_fiat_account_id')
                to_crypto_id = data.get('to_account_id')
                from_me = account_ids[currency] in (from_id, from_crypto_id)
                to_me = account_ids[currency] in (to_id, to_crypto_id)
                if transaction_type in ('funding', 'withdrawal'):
                    assert float(data.get('fee') or .0) == 0
                    assert float(data.get('network_fee') or .0) == 0
                    assert qty > 0
                    assert from_me != to_me
                    if from_me:
                        return dict(kind='withdrawal',
                                    pnl=(currency, -qty, ''))
                    assert to_me
                    return dict(kind='deposit',
                                pnl=(currency, qty, ''))

                if transaction_type in ('bank_fee', 'withdrawal_fee'):
                    assert qty >= 0
                    assert float(data.get('fee') or .0) == 0
                    assert float(data.get('network_fee') or .0) == 0
                    return dict(kind='withdrawal_fee',
                                pnl=(currency, -qty, 'fee'))

                if transaction_type == 'campaign_credit':
                    assert qty > 0
                    return dict(kind='campaign',
                                pnl=(currency, qty, ''))
                if transaction_type == 'cfd_fee':
                    assert qty > 0
                    return dict(kind='margin_fee',
                                pnl=(currency, -qty, 'position_fee'))
                if transaction_type == 'cfd_pnl':
                    assert from_me != to_me
                    if from_me:
                        qty = -qty
                    return dict(kind='margin',
                                pnl=(currency, qty, ''))
            if name == 'order':
                if data['target'] != 'spot':
                    return
                if not data.get('executions'):
                    return
                results = []
                instrument = self.rinstruments[data['product_id']]
                base, quote = instrument.split('/')
                side = data['side'].upper()
                total_fee = float(data['order_fee'])
                assert total_fee >= 0
                executions = data['executions']
                total_qty = float(data['filled_quantity'])
                for execution in executions:
                    base_qty = float(execution['quantity'])
                    quote_qty = float(execution['price']) * base_qty
                    assert base_qty >= 0 and quote_qty >= 0
                    fee_qty = -(base_qty * total_fee / total_qty)
                    assert fee_qty <= 0
                    if side == 'BUY':
                        pnl = [
                            (base, base_qty, 'in'),
                            (quote, -quote_qty, 'out'),
                        ]
                    else:
                        assert side == 'SELL'
                        pnl = [
                            (quote, quote_qty, 'in'),
                            (base, -base_qty, 'out'),
                        ]
                    pnl.append((quote, fee_qty, 'fee'))
                    results.append(dict(kind='spot',
                                        time=self.utc_from_timestamp(float(execution['created_at'])),
                                        id='{}_{}'.format(doc['id'], execution['id']),
                                        instrument=instrument,
                                        side=side,
                                        pnl=pnl,
                                        data=execution))
                return results
            assert False

        result = {}
        while True:
            _doc = yield result
            result = convert_one(_doc)
