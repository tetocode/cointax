from collections import OrderedDict, defaultdict
from typing import Generator

import ccxt

from .ccxtclient import CCXTClient
from .ratelimiter import RateLimiter


class Client(CCXTClient):
    NAME = 'kraken'
    CCXT_CLASS = ccxt.kraken
    LIMIT = 500
    CACHE_LIMIT = 10000
    COLLECTIONS = ('execution', 'deposit', 'withdrawal')

    def get_page_items(self, fn, parse, page_key: str, rps_limit: float, **params):
        fn = RateLimiter(rps_limit, fn)
        timestamp = int(self.utc_now().timestamp())
        offset = 0
        cache = OrderedDict()
        while True:
            params.update(end=timestamp, ofs=offset)
            res = fn(params)
            result = res['result']
            items = result[page_key]
            if len(items) <= 0:
                break
            for x in sorted(map(lambda kv: parse(*kv), items.items()),
                            key=lambda _: _['time'], reverse=True):
                if x['id'] not in cache:
                    cache[x['id']] = True
                    if len(cache) > self.CACHE_LIMIT:
                        cache.popitem(last=False)
                    yield x
            offset += len(items)

    def executions_all(self) -> Generator[dict, None, None]:
        def parse(k: str, v: dict):
            instrument = self.rinstruments[v['pair']]
            return dict(time=self.utc_from_timestamp(float(v['time'])),
                        id='{}_{}'.format(instrument, k),
                        data=v)

        return self.get_page_items(self.privatePostTradesHistory, parse, 'trades', 10 / 60,
                                   type='no position')

    def deposits_all(self) -> Generator[dict, None, None]:
        def parse(k: str, v: dict):
            currency = v['asset']
            return dict(time=self.utc_from_timestamp(v['time']),
                        id='{}_{}'.format(currency, k),
                        data=v)

        return self.get_page_items(self.privatePostLedgers, parse, 'ledger', 10 / 60,
                                   type='deposit')

    def withdrawals_all(self) -> Generator[dict, None, None]:
        def parse(k: str, v: dict):
            currency = v['asset']
            return dict(time=self.utc_from_timestamp(float(v['time'])),
                        id='{}_{}'.format(currency, k),
                        data=v)

        return self.get_page_items(self.privatePostLedgers, parse, 'ledger', 10 / 60,
                                   type='withdrawal')

    @property
    def import_data_methods(self):
        name_methods = defaultdict(list)
        name_methods['execution'].append((self.executions_all, []))
        name_methods['deposit'].append((self.deposits_all, []))
        name_methods['withdrawal'].append((self.withdrawals_all, []))

        return name_methods

    def convert_data(self, name: str):
        def get_currency(asset: str):
            for v in self.instruments.values():
                if v['info']['base'] == asset:
                    return v['base']
                if v['info']['quote'] == asset:
                    return v['quote']
            assert False

        def convert_one(doc: dict):
            data = doc['data']
            amount = float(data.get('amount', 0))
            fee_qty = -float(data.get('fee', 0))
            assert fee_qty <= 0
            if name == 'deposit':
                assert amount > 0
                assert fee_qty == 0
                return dict(kind='deposit',
                            pnl=(get_currency(data['asset']), amount, ''))
            elif name == 'withdrawal':
                assert amount < 0
                return dict(kind='withdrawal',
                            pnl=[
                                (get_currency(data['asset']), amount, ''),
                                (get_currency(data['asset']), fee_qty, 'fee'),
                            ])
            elif name == 'execution':
                instrument = self.rinstruments[data['pair']]
                base, quote = instrument.split('/')
                side = data['type'].upper()
                vol = float(data['vol'])
                cost = float(data['cost'])
                assert vol > 0
                assert cost > 0
                if side == 'BUY':
                    return dict(kind='spot',
                                instrument=instrument,
                                side=side,
                                pnl=[
                                    (base, vol, 'in'),
                                    (quote, -cost, 'out'),
                                    (quote, fee_qty, 'fee'),
                                ])
                else:
                    assert side == 'SELL'
                    return dict(kind='spot',
                                instrument=instrument,
                                side=side,
                                pnl=[
                                    (quote, cost, 'in'),
                                    (base, -vol, 'out'),
                                    (quote, fee_qty, 'fee'),
                                ])
            assert False

        result = {}
        while True:
            _doc = yield result
            result = convert_one(_doc)
