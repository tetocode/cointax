import os
import re
import time
from collections import OrderedDict, defaultdict
from datetime import datetime
from typing import Generator

import ccxt
from ccxt import DDoSProtection, ExchangeNotAvailable

from .ccxtclient import CCXTClient
from .ratelimiter import RateLimiter


class Client(CCXTClient):
    NAME = 'bitfinex'
    LIMIT = 500
    CACHE_LIMIT = 10000
    CCXT_CLASS = ccxt.bitfinex
    COLLECTIONS = ('balance_history',)

    def balance(self):
        balances = defaultdict(lambda: dict(total=.0, used=.0, free=.0))
        for x in self.privatePostBalances():
            """
            [{
              "type":"deposit",
              "currency":"btc",
              "amount":"0.0",
              "available":"0.0"
            },
            """
            currency = x['currency'].upper()
            total = float(x['amount'])
            free = float(x['available'])
            used = total - free
            balances[currency]['total'] += total
            balances[currency]['free'] += free
            balances[currency]['used'] += used
        return balances

    def get_page_items(self, fn, parse, rps_limit: float, **params):
        fn = RateLimiter(rps_limit, fn)
        limit = self.LIMIT
        timestamp = None
        cache = OrderedDict()
        while True:
            if timestamp:
                params.update(until=timestamp)
            res = fn(params)
            processed_n = 0
            for x in res:
                ts = x['timestamp']
                x = parse(x)
                _id = x['id']
                if _id not in cache:
                    cache[_id] = True
                    if len(cache) > self.CACHE_LIMIT:
                        cache.popitem(last=False)
                    yield x
                timestamp = ts
                processed_n += 1
            if len(res) < limit:
                break
            if processed_n == 0:
                raise Exception('page items exceed limit')

    def balance_history(self, currency: str) -> Generator[dict, None, None]:
        """
        currency        string        required
            The currency to look for.
        since        date-time
            Return only the history after this timestamp.
        until        date-time
            Return only the history before this timestamp.
        limit        int32
            Limit the number of entries to return.
        wallet        string
            Return only entries that took place in this wallet. Accepted inputs are: “trading”, “exchange”, “deposit”.
        """

        # rate limit: 20 req/min
        def parse(data: dict):
            ts = data['timestamp']
            digest = self.json_hash(data)
            return dict(time=self.utc_from_timestamp(float(ts)),
                        id='{}_{}'.format(currency, digest),
                        data=data)

        yield from self.get_page_items(self.privatePostHistory, parse, 20 / 60,
                                       currency=currency, limit=self.LIMIT)

    def transfers(self, currency: str) -> Generator[dict, None, None]:
        # history/movements
        """
        currency        string        required
            The currency to look for.
        method        string
            The method of the deposit/withdrawal (can be “bitcoin”, “litecoin”, “darkcoin”, “wire”).
        since        date-time
            Return only the history after this timestamp.
        until        date-time
            Return only the history before this timestamp.
        limit        int32
            Limit the number of entries to return.
        """

        # rate limit: 20 req/min
        def parse(data: dict):
            ts = data['timestamp']
            digest = self.json_hash(data)
            return dict(time=self.utc_from_timestamp(float(ts)),
                        id='{}_{}'.format(currency, digest),
                        data=data)

        yield from self.get_page_items(self.privatePostHistoryMovements, parse, 20 / 60,
                                       currency=currency, limit=self.LIMIT)

    def executions(self, instrument: str) -> Generator[dict, None, None]:
        # mytrades
        """
        symbol        string        required
            The pair traded (BTCUSD, …).
        timestamp        date-time
            Trades made before this timestamp won’t be returned.
        until        date-time
            Trades made after this timestamp won’t be returned.
        limit_trades        int32
            Limit the number of trades returned.
        reverse        int32
            Return trades in reverse order (the oldest comes first). Default is returning newest trades first.
        """

        # rate limit: 45 req/min
        def parse(data: dict):
            ts = data['timestamp']
            digest = self.json_hash(data)
            return dict(time=self.utc_from_timestamp(float(ts)),
                        id='{}_{}'.format(instrument, digest),
                        data=data)

        symbol = self.instruments[instrument]['id']
        yield from self.get_page_items(self.privatePostMytrades, parse, 45 / 60,
                                       symbol=symbol, limit_trades=self.LIMIT)

    @property
    def import_data_methods(self):
        name_methods = defaultdict(list)
        for currency in sorted(self.currencies):
            name_methods['balance_history'].append((self.balance_history, [currency]))
            # name_methods['transfer'].append((self.transfers, [currency]))
        # for instrument in sorted(self.instruments):
        #    name_methods['execution'].append((self.executions, [instrument]))
        return name_methods

    def convert_data(self, name: str):
        def convert_one(doc: dict):
            data = doc['data']

            if name == 'balance_history':
                desc = data['description']
                currency = data['currency']
                qty = float(data['amount'])

                if re.search('^Deposit Fee \(\w+\) \d+ on wallet (?:deposit|exchange)', desc, re.IGNORECASE):
                    assert qty < 0
                    return dict(kind='deposit_fee',
                                pnl=(currency, qty, 'fee'))
                if re.search('^Deposit \(\w+\) #\d+ on wallet (?:deposit|Exchange)', desc, re.IGNORECASE):
                    assert qty > 0
                    return dict(kind='deposit',
                                pnl=(currency, qty, ''))
                if re.search('^Transfer of', desc, re.IGNORECASE):
                    return
                if re.search('^Margin Funding Payment on wallet Deposit', desc, re.IGNORECASE):
                    assert qty > 0
                    return dict(kind='interest',
                                pnl=(currency, qty, ''))
                m = re.search(
                    '^Trading fees for \S+ \S+ \((?P<symbol>\S+)\) @ \S+ on BFX \(\S+\) on wallet (?P<wallet>\S+)',
                    desc, re.IGNORECASE)
                if m:
                    d = m.groupdict()
                    symbol = d['symbol']
                    wallet = d['wallet'].lower()
                    assert len(symbol) == 6
                    assert qty < 0
                    base, quote = symbol[:3], symbol[3:]
                    instrument = '{}/{}'.format(base, quote)
                    if wallet == 'trading':
                        return dict(kind='margin_fee',
                                    pnl=(currency, qty, 'fee'))
                    else:
                        assert wallet == 'exchange'
                        if currency != quote:
                            side = 'BUY'
                        else:
                            assert currency == quote
                            side = 'SELL'
                        return dict(kind='spot_fee',
                                    instrument=instrument,
                                    side=side,
                                    pnl=(currency, qty, 'fee'))
                m = re.search('^Exchange \S+ (?P<base>\S+) for (?P<quote>\S+) @ \S+ on wallet Exchange',
                              desc, re.IGNORECASE)
                if m:
                    d = m.groupdict()
                    base, quote = d['base'], d['quote']
                    instrument = '{}/{}'.format(base, quote)
                    if currency == base:
                        if qty > 0:
                            side = 'BUY'
                        else:
                            assert qty < 0
                            side = 'SELL'
                    else:
                        assert currency == quote
                        if qty > 0:
                            side = 'SELL'
                        else:
                            assert qty < 0
                            side = 'BUY'
                    return dict(kind='spot',
                                instrument=instrument,
                                side=side,
                                pnl=(currency, qty, ''))
                m = re.search('^Position closed @ \S+ on wallet Trading', desc, re.IGNORECASE)
                if m:
                    return dict(kind='margin',
                                pnl=(currency, qty, ''))
                m = re.search('^Unused Margin Funding Fee Loan #\d+ on wallet Trading', desc, re.IGNORECASE)
                if m:
                    assert qty < 0
                    return dict(kind='margin_fee',
                                pnl=(currency, qty, 'unused margin funding fee'))
                m = re.search('^Unused Margin Funding Fee on wallet Trading', desc, re.IGNORECASE)
                if m:
                    assert qty < 0
                    return dict(kind='margin_fee',
                                pnl=(currency, qty, 'unused margin funding fee'))
                m = re.search('^Used Margin Funding Charge on wallet trading', desc, re.IGNORECASE)
                if m:
                    assert qty < 0
                    return dict(kind='margin_fee',
                                pnl=(currency, qty, 'used margin funding charge fee'))
                m = re.search('^Settlement @ \S+ on wallet (?:Trading|Exchange)', desc, re.IGNORECASE)
                if m:
                    return dict(kind='adjustment',
                                pnl=(currency, qty, 'settlement'))
                m = re.search('^\w+ Withdrawal #\d+ on wallet (?:Deposit|trading)', desc, re.IGNORECASE)
                if m:
                    assert qty < 0
                    return dict(kind='withdrawal',
                                pnl=(currency, qty, ''))
                m = re.search('^\w+ Withdrawal #\d+ on wallet Exchange', desc, re.IGNORECASE)
                if m:
                    assert qty < 0
                    return dict(kind='withdrawal',
                                pnl=(currency, qty, ''))
                m = re.search('^Crypto Withdrawal fee on wallet (?:Deposit|Exchange|trading)', desc, re.IGNORECASE)
                if m:
                    assert qty < 0
                    return dict(kind='withdrawal_fee',
                                pnl=(currency, qty, 'fee'))
                m = re.search('^Position claimed (?P<symbol>\w+) @ \S+ on wallet Trading', desc, re.IGNORECASE)
                if m:
                    d = m.groupdict()
                    symbol = d['symbol']
                    assert len(symbol) == 6
                    base, quote = symbol[:3], symbol[3:]
                    instrument = '{}/{}'.format(base, quote)
                    return dict(kind='claim',
                                instrument=instrument,
                                pnl=(currency, qty, 'claim'))
                m = re.search('^Claiming fee for Position claimed \w+ @ \S+ on wallet Trading', desc,
                              re.IGNORECASE)
                if m:
                    return dict(kind='claim_fee',
                                pnl=(currency, qty, 'fee'))
                m = re.search('^Position funding cost on wallet Trading', desc, re.IGNORECASE)
                if m:
                    assert qty < 0
                    return dict(kind='margin_fee',
                                pnl=(currency, qty, 'position funding cost fee'))
                m = re.search('^Position #\d+ funding cost on wallet Trading', desc, re.IGNORECASE)
                if m:
                    assert qty < 0
                    return dict(kind='margin_fee',
                                pnl=(currency, qty, 'position funding cost fee'))
                m = re.search('^Margin Funding Payment \(adj \S+\) on wallet Deposit', desc,
                              re.IGNORECASE)
                if m:
                    return dict(kind='adjustment',
                                pnl=(currency, qty, 'adjustment'))
                m = re.search('^Adjustment Margin Funding Payment on wallet Deposit', desc, re.IGNORECASE)
                if m:
                    assert qty < 0
                    return dict(kind='adjustment',
                                pnl=(currency, qty, 'adjustment'))
                m = re.search('^Canceled withdrawal fee #\d+ on wallet Exchange', desc, re.IGNORECASE)
                if m:
                    assert qty > 0
                    return dict(kind='cancel_withdrawal_fee', pnl=(currency, qty, ''))
                m = re.search('^Canceled withdrawal request #\d+ on wallet Exchange', desc, re.IGNORECASE)
                if m:
                    assert qty > 0
                    return dict(kind='cancel_withdrawal', pnl=(currency, qty, ''))
            elif name == 'transfer':
                if True:
                    self.error('currently transfer not used')
                    return
                if data['status'] != 'COMPLETED':
                    return
                currency = data['currency']
                qty = float(data['amount'])
                fee_qty = float(data['fee'])
                assert qty > 0
                assert fee_qty <= 0
                if data['type'] == 'DEPOSIT':
                    assert fee_qty == 0
                    return dict(kind='deposit',
                                pnl=[
                                    (currency, qty, ''),
                                ])
                else:
                    qty = -qty
                    assert data['type'] == 'WITHDRAWAL'
                    print('withdrawal,{},{}'.format(currency, qty))
                    print('withdrawal_fee,{},{}'.format(currency, fee_qty))
                    return dict(kind='withdrawal',
                                pnl=[
                                    (currency, qty, ''),
                                    (currency, fee_qty, 'fee'),
                                ])
            elif name == 'execution':
                if True:
                    self.error('currently execution not used')
                    return
                instrument = doc['id'].split('_')[0]
                base, quote = instrument.split('/')
                base_qty = float(data['amount'])
                quote_qty = float(data['price']) * base_qty
                fee_currency = data['fee_currency']
                fee_qty = float(data['fee_amount'])
                assert base_qty > 0 and quote_qty > 0
                assert fee_qty < 0
                side = data['type'].upper()
                if side == 'BUY':
                    pnl = [
                        (base, base_qty, 'in'),
                        (quote, -quote_qty, 'out'),
                        (fee_currency, fee_qty, 'fee'),
                    ]
                else:
                    assert side == 'SELL'
                    pnl = [
                        (quote, quote_qty, 'in'),
                        (base, -base_qty, 'out'),
                        (fee_currency, fee_qty, 'fee'),
                    ]
                return dict(kind='execution',
                            instrument=instrument,
                            side=side,
                            pnl=pnl)
            assert False

        result = {}
        while True:
            _doc = yield result
            result = convert_one(_doc)

    def public_executions_asc(self, instrument: str, start: datetime, stop: datetime, **params):
        """
        https://api.bitfinex.com/v2/trades/Symbol/hist
        Path Params
        Symbol        string        required
            The symbol you want information about.
        Query Params
        limit        int32
            Number of records
        start        int32
            Millisecond start time
        end        int32
            Millisecond end time
        sort        int32
            if = 1 it sorts results returned with old > new

        // on trading pairs (ex. tBTCUSD)
        [
          [
            ID,
            MTS,
            AMOUNT,
            PRICE
          ]
        ]
        // on funding currencies (ex. fUSD)
        [
          [
            ID,
            MTS,
            AMOUNT,
            RATE,
            PERIOD
          ]
        ]
        """
        """
            params = dict(symbol=symbol, limit=limit)
            if timestamp:
                params.update(end=timestamp)
            res = v2api.publicGetTradesSymbolHist(params)
"""
        api = ccxt.bitfinex2()
        rps_limit = 60 / 60
        if os.environ.get('https_proxy'):
            rps_limit *= 5
        fn = RateLimiter(rps_limit, getattr(api, 'publicGetTradesSymbolHist'))
        limit = self.LIMIT
        params = dict(symbol='t{}'.format(self.instruments[instrument]['id']), limit=limit, sort=1)
        cache = OrderedDict()
        start = int(start.timestamp() * 1000)
        stop = int(stop.timestamp() * 1000)
        mts = start
        while mts < stop:
            try:
                params.update(start=mts)
                res = fn(params)
                processed_n = 0
                for data in res:
                    _id, mts, amount, price = data
                    if start <= mts < stop:
                        processed_n += 1
                        if _id not in cache:
                            cache[_id] = True
                            if len(cache) > self.CACHE_LIMIT:
                                cache.popitem(last=False)
                        yield dict(time=self.utc_from_timestamp(mts / 1000),
                                   id=_id,
                                   price=price,
                                   qty=abs(amount),
                                   data=data)
                if len(res) < limit:
                    break
                if not processed_n:
                    self.error('no processed item {}'.format(params))
                    break
            except DDoSProtection:
                self.warning('DDoSProtection. sleep {}.'.format(self.RATE_LIMIT_INTERVAL))
                time.sleep(self.RATE_LIMIT_INTERVAL)
            except ExchangeNotAvailable as e:
                self.warning(str(e))
