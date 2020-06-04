import copy
import json
import logging
import pathlib
import sys
from collections import defaultdict, deque
from datetime import datetime, timedelta
from pprint import pprint, pformat
from typing import Sequence, Dict, Tuple, Optional, Union

import pymongo
from dateutil.relativedelta import relativedelta
from docopt import docopt

from coinapi.clientbase import ClientBase
from coindb.bulkop import BulkOp

UTC = ClientBase.UTC
JST = ClientBase.JST
utc_now = ClientBase.utc_now
parse_time = ClientBase.parse_time

RATE_KEY = ''


def main():
    logging.basicConfig(level=logging.INFO)
    args = docopt("""
    Usage:
        {f} [options] balance [EXCHANGE]
        {f} [options] gather
        {f} [options] move [EXCHANGE]
        {f} [options] gross [EXCHANGE]
    
    Options:
        --db DB  [default: tax]
        --collection COLLECTION  [default: pl]
        --start START  [default: 2017-01-01T00:00+09:00]
        --stop STOP  [default: 2018-01-01T00:00+09:00]
        --key KEY  [default: vwap]

    """.format(f=pathlib.Path(sys.argv[0]).name,
               start=JST.localize(datetime(2016, 12, 29)),
               now=utc_now().astimezone(JST)))
    global RATE_KEY
    RATE_KEY = args['--key']
    pprint(args)
    db = args['--db']
    collection = args['--collection']
    start_after = parse_time(args['--start']) - timedelta(microseconds=1000)
    stop = parse_time(args['--stop'])

    exchanges = ['bitfinex', 'bitflyer', 'bitmex',
                 'coincheck', 'kraken', 'minbtc',
                 'quoinex', 'xmr', 'zaif']
    exchange = args['EXCHANGE']
    if exchange:
        assert exchange in exchanges, (exchange, exchanges)
        exchanges = [exchange]

    if args['balance']:
        return check_balance(exchanges, start_after, stop)
    if args['gather']:
        return gather(db, collection, exchanges, start_after, stop)
    if args['move']:
        return calculate('move', db, collection, exchanges, start_after, stop)
    if args['gross']:
        return calculate('gross', db, collection, exchanges, start_after, stop)


def gen_doc(db: str, collection: str, start_after: datetime, stop: datetime):
    db_client = pymongo.MongoClient()
    for doc in db_client[db][collection].find({'time': {'$gt': start_after, '$lt': stop}},
                                              {'_id': 0}).sort('time', 1):
        assert '_id' not in doc
        if doc.get('skip'):
            print('#skip')
            continue
        yield doc


def gen_pnl(db: str, collection: str, start_after: datetime, stop: datetime):
    for doc in gen_doc(db, collection, start_after, stop):
        if isinstance(doc['pnl'][0], str):
            pnl_list = [doc['pnl']]
        else:
            pnl_list = doc['pnl']
        for currency, qty, note in pnl_list:
            yield (doc, CURRENCY_MAP.get(currency, currency), qty, note)


def print_balances(balances):
    balances = copy.deepcopy(balances)
    for k, v in balances.items():
        if k in {'BTC', 'ETH', 'BCH', 'QASH', 'XRP'}:
            v = round(v * 1e12) / 1e12
        else:
            v = round(v * 1e9) / 1e9
        balances[k] = v
    pprint(balances)


def check_balance(exchanges: Sequence[str],
                  start_after: datetime, stop: datetime):
    print('#', exchanges, start_after, stop)
    balances = defaultdict(float)
    for exchange in exchanges:
        x_balances = defaultdict(float)
        debt_flags = defaultdict(lambda: False)
        for doc, currency, qty, note in gen_pnl(exchange, 'converted', start_after, stop):
            x_balances[currency] += qty
            if debt_flags[currency] and x_balances[currency] >= 0:
                # print('#recover', doc['time'], exchange, currency, qty, note, x_balances[currency])
                debt_flags[currency] = False
            if x_balances[currency] < 0:
                # print('#', doc['time'], exchange, currency, qty, note, x_balances[currency])
                debt_flags[currency] = True
            balances[currency] += qty
        print('#', exchange)
        print_balances(x_balances)
    for k, v in balances.items():
        if k in CRYPTO_CURRENCIES:
            v = round(v * 1e12) / 1e12
        else:
            v = round(v * 1e9) / 1e9
        balances[k] = v
    if len(exchanges) >= 2:
        print('# total')
        print_balances(balances)


def gather(db: str, collection: str,
           exchanges: Sequence[str],
           start_after: datetime, stop: datetime):
    print('#', exchanges, start_after, stop)
    collection = pymongo.MongoClient()[db][collection]
    collection.drop()
    collection.create_index([
        ('time', 1), ('exchange', 1), ('kind', 1), ('id', 1),
    ], unique=True)
    with BulkOp(collection) as bulk_op:
        for exchange in exchanges:
            for doc in gen_doc(exchange, 'converted', start_after, stop):
                doc.update(exchange=exchange)
                assert '_id' not in doc
                bulk_op.insert(doc)
    return


def calculate(calc_type: str, db: str, collection: str,
              exchanges: Sequence[str],
              start_after: datetime, stop: datetime):
    print('#', exchanges, start_after, stop)
    calculator = Calculator(RATE_KEY)

    for doc in gen_doc(db, collection, start_after, stop):
        try:
            if doc['exchange'] not in exchanges:
                continue

            calculator.add(doc)
        except Exception:
            logging.error('#{}'.format(pformat(doc)))
            raise

    if calc_type == 'move':
        calculator.export_move()
    elif calc_type == 'gross':
        calculator.export_gross()


FIAT_CURRENCIES = {'AUD', 'CNY', 'EUR', 'HKD', 'IDR', 'INR', 'JPY', 'PHP', 'SGD', 'USD'}
CRYPTO_CURRENCIES = {
    'BCH': ('quoinex', 'JPY'),
    'BTC': ('quoinex', 'JPY'),
    'ETH': ('quoinex', 'JPY'),
    'QASH': ('quoinex', 'JPY'),
    'XMR': ('bitfinex', 'USD'),
    'XRP': ('bitfinex', 'USD'),
    'JPYZ': ('zaif', 'JPY'),
    'MONA': ('zaif', 'JPY'),
    'PEPECASH': ('zaif', 'JPY'),
    'XEM': ('zaif', 'JPY'),
    'ZAIF': ('zaif', 'JPY'),
    'ERC20.CMS': ('zaif', 'JPY'),
}
ALL_CURRENCIES = FIAT_CURRENCIES | set(CRYPTO_CURRENCIES)
CURRENCY_MAP = {
    'QSH': 'QASH',
}


class Calculator:
    def __init__(self, rate_key: str):
        self.rate_key = rate_key
        self.db_client = pymongo.MongoClient()
        self.collections = {}
        self.cache = {}
        self.q = deque()
        self.kinds = set()

    def get_collection(self, db: str, collection: str):
        key = (db, collection)
        if key not in self.collections:
            self.collections[key] = self.db_client[db][collection]
        return self.collections[key]

    def _get_instrument_rate(self, exchange: str, instrument: str, dt: datetime):
        #        dt_base = (dt - relativedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        dt_base = (dt - relativedelta(minutes=1)).replace(second=0, microsecond=0)
        key = (exchange, instrument, dt_base)
        if key not in self.cache:
            #            collection = self.get_collection(exchange, '{}'.format(instrument))
            collection = self.get_collection(exchange, '{}_M1'.format(instrument))
            #            collection = self.get_collection(exchange, '{}_D'.format(instrument))
            target = None
            #            for target in collection.find({'time': {'$lt': dt}}).sort('time', -1):
            for target in collection.find({'time': {'$gt': dt_base}}).sort('time', 1):
                #                return target['price']
                assert target and self.rate_key in target, (exchange, instrument, dt, target)
                self.cache[key] = target[self.rate_key]
                break
            else:
                assert False, (exchange, instrument, dt, dt_base, target)
        return self.cache[key]

    def get_fiat_rate(self, currency: str, dt: datetime):
        btc_jpy_rate = self._get_instrument_rate('quoinex', 'BTC/JPY', dt)
        target_rate = self._get_instrument_rate('quoinex', 'BTC/{}'.format(currency), dt)
        return btc_jpy_rate / target_rate

    def get_crypto_rate(self, currency: str, dt: datetime):
        exchange, quote = CRYPTO_CURRENCIES[currency]
        target_rate = self._get_instrument_rate(exchange, '{}/{}'.format(currency, quote), dt)
        quote_rate = self.get_fiat_rate(quote, dt)
        return target_rate * quote_rate

    def get_rate(self, currency: str, dt: datetime):
        if currency in ('bank', 'JPY'):
            return 1.0
        if currency in FIAT_CURRENCIES:
            return self.get_fiat_rate(currency, dt)
        assert currency in CRYPTO_CURRENCIES, (currency, CRYPTO_CURRENCIES)
        return self.get_crypto_rate(currency, dt)

    def append(self, *, kind: str, incomes=(), outcomes=(), fees=(), **kwargs):
        self.q.append(dict(kind=kind, incomes=incomes, outcomes=outcomes, fees=fees, **kwargs))

    def export_move(self):
        simple_balances = defaultdict(float)
        balances = Balances()

        results = []
        for doc in self.q:
            try:
                for currency, qty in doc['incomes'] + doc['outcomes'] + doc['fees']:
                    currency = CURRENCY_MAP.get(currency, currency)
                    simple_balances[currency] += qty
                doc_time = doc['time']
                doc_kind = doc['kind']
                instrument = ''
                side = ''
                incomes = []
                outcomes = []
                fees = []
                pprint(doc)
                if doc_kind in ('spot', 'spot_fee'):
                    instrument = doc['instrument']
                    base, quote = instrument.split('/')
                    base = CURRENCY_MAP.get(base, base)
                    quote = CURRENCY_MAP.get(quote, quote)
                    side = doc['side']
                    assert base in CRYPTO_CURRENCIES
                    if quote in FIAT_CURRENCIES:
                        if side == 'BUY':
                            for currency, income in doc['incomes']:
                                assert currency == base
                                incomes.append(balances[currency].calculate(doc_time, income, 0.))
                            for currency, outcome in doc['outcomes']:
                                assert currency == quote
                                d = balances[currency].calculate(doc_time, outcome, None)
                                outcomes.append(d)
                                incomes.append(balances[base].calculate(doc_time, 0., abs(d['value'])))
                            for currency, fee in doc['fees']:
                                d = balances[currency].calculate(doc_time, fee, None)
                                fees.append(d)
                                balances[base].calculate(doc_time, 0., abs(d['value']))
                        else:
                            for currency, income in doc['incomes']:
                                incomes.append(balances[currency].calculate(doc_time, income, None))
                            for currency, outcome in doc['outcomes']:
                                outcomes.append(balances[currency].calculate(doc_time, outcome, None))
                            for currency, fee in doc['fees']:
                                d = balances[currency].calculate(doc_time, fee, None)
                                fees.append(d)
                                balances[quote].calculate(doc_time, 0., abs(d['value']))
                    else:
                        for currency, income in doc['incomes']:
                            incomes.append(balances[currency].calculate(doc_time, income, None))
                        for currency, outcome in doc['outcomes']:
                            outcomes.append(balances[currency].calculate(doc_time, outcome, None))
                        if side == 'BUY':
                            for currency, fee in doc['fees']:
                                d = balances[currency].calculate(doc_time, fee, None)
                                fees.append(d)
                                balances[base].calculate(doc_time, 0., abs(d['value']))
                        else:
                            for currency, fee in doc['fees']:
                                d = balances[currency].calculate(doc_time, fee, None)
                                fees.append(d)
                                balances[quote].calculate(doc_time, 0., abs(d['value']))
                else:
                    for currency, income in doc['incomes']:
                        incomes.append(balances[currency].calculate(doc_time, income, None))
                    for currency, outcome in doc['outcomes']:
                        outcomes.append(balances[currency].calculate(doc_time, outcome, None))
                    for currency, fee in doc['fees']:
                        fees.append(balances[currency].calculate(doc_time, fee, None))
                print('#after')
                pprint(incomes)
                pprint(outcomes)
                pprint(fees)
                results.append(dict(time=doc_time,
                                    exchange=doc['exchange'],
                                    kind=doc_kind,
                                    instrument=instrument,
                                    side=side,
                                    _incomes=incomes,
                                    _outcomes=outcomes,
                                    _fees=fees,
                                    balances=copy.deepcopy(balances.as_dict())
                                    ))
            except Exception:
                pprint(doc)
                pprint(balances)
                raise

        pl_value = 0
        for i, result in enumerate(results, 1):
            negative = False
            for c, bs in result['balances'].items():
                if bs['qty'] < 0:
                    negative = True
            print('#{}{}'.format(i, ' negative' if negative else ''))
            for x in result['_incomes'] + result['_outcomes'] + result['_fees']:
                pl_value += x['value']
            print('#pnl={:,}'.format(pl_value))
            pprint(result)

        print('# balances')
        pprint(balances)
        print('# pl_value={:,}'.format(pl_value))
        print('# simple balances')
        print_balances(simple_balances)
        print('# kinds = {}'.format(list(sorted(self.kinds))))

    def export_gross(self):
        simple_balances = defaultdict(float)  # type: Dict[str, float]
        balances_buy = defaultdict(lambda: (.0, .0))  # type: Dict[str, Tuple[float, float]]
        balances_sell = defaultdict(lambda: (.0, .0))  # type: Dict[str, Tuple[float, float]]

        costs = defaultdict(lambda: (.0, .0))
        for doc in self.q:
            try:
                for currency, qty in doc['incomes'] + doc['outcomes'] + doc['fees']:
                    simple_balances[currency] += qty
                dt = doc['time']
                kind = doc['kind']
                if kind in ('spot', 'spot_fee'):
                    instrument = doc['instrument']
                    base, quote = instrument.split('/')
                    base = CURRENCY_MAP.get(base, base)
                    quote = CURRENCY_MAP.get(quote, quote)
                    side = doc['side']
                    assert base in CRYPTO_CURRENCIES
                    if quote in FIAT_CURRENCIES and side == 'BUY':
                        for currency, income in doc['incomes']:
                            assert currency == base
                            q, v = costs[currency]
                            costs[currency] = (q + income, v)
                        for currency, outcome in doc['outcomes']:
                            assert currency == quote
                            q, v = costs[base]
                            costs[base] = (q, v + abs(outcome) * self.get_rate(quote, dt))
                    else:
                        for currency, income in doc['incomes']:
                            q, v = costs[currency]
                            costs[currency] = (q + income, v + abs(income) * self.get_rate(currency, dt))
                else:
                    for currency, income in doc['incomes']:
                        q, v = costs[currency]
                        costs[currency] = (q + income, v + abs(income) * self.get_rate(currency, dt))
            except Exception:
                pprint(doc)
                raise

        unit_prices = {}
        for k, (a, b) in costs.items():
            unit_prices[k] = b / a

        balances = defaultdict(float)

        for doc in self.q:
            try:
                dt = doc['time']
                kind = doc['kind']
                for currency, outcome in doc['outcomes']:
                    balances[currency] += abs(outcome) * (self.get_rate(currency, dt) - unit_prices[currency])
                for currency, fee in doc['fees']:
                    if fee:
                        balances[currency] += abs(fee) * (self.get_rate(currency, dt) - unit_prices[currency])
            except Exception:
                pprint(doc)
                raise

        print('# unit_price')
        pprint(unit_prices)

        print('# pl')
        print_balances(balances)

        pl = sum(balances.values())
        print('# pl={:,}'.format(pl))

    def add(self, doc: dict):
        def append(*, kind: str = None, **_kwargs):
            nonlocal doc_time, doc_exchange, doc
            assert doc_time and doc_exchange
            doc = copy.deepcopy(doc)
            doc.update(kind=kind or doc_kind,
                       incomes=incomes, outcomes=outcomes, fees=fees)
            self.q.append(doc)

        doc_time = doc['time']
        doc_exchange = doc['exchange']
        doc_kind = doc['kind']
        assert len(doc['pnl']) > 0
        if isinstance(doc['pnl'][0], str):
            pnl_list = [doc['pnl']]
        else:
            pnl_list = doc['pnl']

        doc_currencies = list(set([CURRENCY_MAP.get(_[0], _[0]) for _ in pnl_list]))
        for _ in doc_currencies:
            assert _ in ALL_CURRENCIES, (_, ALL_CURRENCIES)
        incomes, outcomes, fees = [], [], []
        for currency, qty, note in pnl_list:
            if not qty:
                continue
            currency = CURRENCY_MAP.get(currency, currency)
            if 'fee' in note or 'cost' in note:
                fees.append((currency, qty))
            elif qty > 0:
                incomes.append((currency, qty))
            elif qty < 0:
                outcomes.append((currency, qty))

        self.kinds.add(doc_kind)
        if doc_kind in ('deposit', 'withdrawal'):
            if 'JPY' in doc_currencies:
                return
            if doc_kind == 'withdrawal':
                outcomes = []
                assert not incomes and not outcomes
                return append()
            return
        if doc_kind in ('withdrawal_fee',):
            assert not incomes and not outcomes
            if 'JPY' in doc_currencies:
                assert len(doc_currencies) == 1, doc_currencies
                return append(kind='jpy_withdrawal_fee')
            return append()
        if '_fee' in doc_kind and doc_kind != 'spot_fee':
            assert not incomes and not outcomes
            return append()
        return append()


class Balances(dict):
    def __missing__(self, key):
        self[key] = value = Balance(key)
        return value

    def json(self):
        return json.dumps(self)

    def as_dict(self):
        return {k: v.copy() for k, v in self.items()}


class Balance(dict):
    def __init__(self, currency: str):
        super().__init__()
        self.currency = CURRENCY_MAP.get(currency, currency)
        self.qty = 0
        self.value = 0

        self.db_client = pymongo.MongoClient()
        self.collections = {}
        self.cache = {}

    @property
    def qty(self):
        return self['qty']

    @qty.setter
    def qty(self, value):
        self['qty'] = value
        self['unit_price'] = float('nan')
        if self.qty:
            self['unit_price'] = self.value / self.qty

    @property
    def value(self):
        return self['value']

    @value.setter
    def value(self, value):
        self['value'] = value
        self['unit_price'] = float('nan')
        if self.qty:
            self['unit_price'] = self.value / self.qty

    def calculate(self, dt: datetime, qty: Union[int, float], value: Optional[float]):
        assert isinstance(qty, (float, int))
        pre_qty = self.qty
        pre_value = self.value
        note = ''

        if self.currency == 'JPY':
            value = qty
        if isinstance(value, (int, float)):
            self.value += value
        else:
            assert qty
            if qty > 0:
                if value is None:
                    rate = self.get_rate(self.currency, dt)
                    note = '{}/JPY={}'.format(self.currency, rate)
                    value = qty * rate
            else:
                assert qty < 0
                if self.qty <= 0:
                    rate = self.get_rate(self.currency, dt)
                    note = '{}/JPY={}'.format(self.currency, rate)
                    value = qty * rate
                else:
                    if self.value <= 0:
                        rate = self.get_rate(self.currency, dt)
                        note = '{}/JPY={}'.format(self.currency, rate)
                        value = qty * rate
                    else:
                        value = self.value * qty / self.qty
            self.value += value
        self.qty += qty
        qty_delta = self.qty - pre_qty
        value_delta = self.value - pre_value
        return dict(currency=self.currency, qty=qty_delta, value=value_delta, _note=note)

    def ret_data(self, qty, value: Optional[float], note: str):
        return dict(currency=self.currency, qty=qty, value=value, _note=note)

    def get_collection(self, db: str, collection: str):
        key = (db, collection)
        if key not in self.collections:
            self.collections[key] = self.db_client[db][collection]
        return self.collections[key]

    def _get_instrument_rate(self, exchange: str, instrument: str, dt: datetime):
        dt_base = (dt - relativedelta(days=2)).replace(hour=0, minute=0, second=0, microsecond=0)
        key = (exchange, instrument, dt_base)
        if key not in self.cache:
            collection = self.get_collection(exchange, '{}_D'.format(instrument))
            target = None
            for target in collection.find({'time': {'$gt': dt_base}}).sort('time', 1):
                assert target and RATE_KEY in target, (exchange, instrument, dt, target)
                self.cache[key] = target[RATE_KEY]
                break
            else:
                assert False, (exchange, instrument, dt, dt_base, target)
        return self.cache[key]

    def _get_fiat_instrument_rate(self, exchange: str, instrument: str, dt: datetime):
        if instrument == 'JPY/JPY':
            return 1.0
        collection = self.get_collection(exchange, instrument)
        for doc in collection.find({'time': {'$lt': dt - timedelta(days=1)}}).sort('time', -1):
            return doc['c']
        assert False

    def get_fiat_rate(self, currency: str, dt: datetime):
        if currency == 'JPY':
            return 1.0
        target_rate = self._get_fiat_instrument_rate('yahoo', '{}/JPY'.format(currency), dt)
        return target_rate

    def get_crypto_rate(self, currency: str, dt: datetime):
        db, quote = CRYPTO_CURRENCIES[currency]
        #   collection = self.get_collection(db, '{}/{}_M1'.format(currency, quote))
        collection = self.get_collection(db, '{}/{}_D'.format(currency, quote))
        dt_base = (dt - relativedelta(days=2)).replace(hour=0, minute=0, second=0, microsecond=0)
        for doc in collection.find({'time': {'$gt': dt_base}}).sort('time', 1):
            return doc['vwap'] * self.get_fiat_rate(quote, dt)
        assert False

    def get_rate(self, currency: str, dt: datetime):
        if currency in ('bank', 'JPY'):
            return 1.0
        if currency in FIAT_CURRENCIES:
            return self.get_fiat_rate(currency, dt)
        assert currency in CRYPTO_CURRENCIES, (currency, CRYPTO_CURRENCIES)
        return self.get_crypto_rate(currency, dt)


if __name__ == '__main__':
    main()
