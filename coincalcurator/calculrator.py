from collections import defaultdict
from datetime import datetime, timedelta
from typing import Sequence, Optional

import pymongo
from dateutil.relativedelta import relativedelta

from coinapi.clientbase import ClientBase

UTC = ClientBase.UTC
JST = ClientBase.JST
utc_now = ClientBase.utc_now
parse_time = ClientBase.parse_time

RATE_KEY = ''


class Calculator:
    FIAT_CURRENCIES = {
        'AUD': dict(db='yahoo'),
        'CNY': dict(db='yahoo'),
        'EUR': dict(db='yahoo'),
        'HKD': dict(db='yahoo'),
        'IDR': dict(db='yahoo'),
        'INR': dict(db='yahoo'),
        'JPY': dict(db=None),
        'PHP': dict(db='yahoo'),
        'SGD': dict(db='yahoo'),
        'USD': dict(db='yahoo'),
    }
    CRYPTO_CURRENCIES = {
        'BCH': dict(db='quoinex', quote='JPY'),
        'BTC': dict(db='quoinex', quote='JPY'),
        'ETH': dict(db='quoinex', quote='JPY'),
        'QASH': dict(db='quoinex', quote='JPY'),
        'XMR': dict(db='bitfinex', quote='USD'),
        'XRP': dict(db='bitfinex', quote='USD'),
        'JPYZ': dict(db='zaif', quote='JPY'),
        'MONA': dict(db='zaif', quote='JPY'),
        'PEPECASH': dict(db='zaif', quote='JPY'),
        'XEM': dict(db='zaif', quote='JPY'),
        'ZAIF': dict(db='zaif', quote='JPY'),
        'ERC20.CMS': dict(db='zaif', quote='JPY'),
    }
    SUPPORTED_CURRENCIES = set(FIAT_CURRENCIES) | set(CRYPTO_CURRENCIES)
    CURRENCY_MAP = {
        'QSH': 'QASH',
    }

    def __init__(self):
        self._collection_cache = {}
        self.rate_cache = {}
        self.pnl = .0
        self.balances = defaultdict(lambda: dict(qty=.0, jpy=.0, price=float('nan')))
        self.debt_balances = defaultdict(lambda: dict(qty=.0, jpy=.0, price=float('nan')))
        self.ga_costs = defaultdict(lambda: dict(qty=.0, jpy=.0, price=float('nan')))
        self.ga_outcomes = defaultdict(float)

    def reset(self):
        self.pnl = .0
        self.balances.clear()
        self.ga_costs.clear()
        self.ga_outcomes.clear()

    def load_balances(self, balances):
        for k, v in balances.items():
            self.balances[k].update(**v)
            self.ga_costs[k].update(**v)

    def reset_debt_balances(self):
        self.debt_balances = defaultdict(lambda: dict(qty=.0, jpy=.0, price=float('nan')))

    def get_current_value(self, dt: datetime):
        value = .0
        debt = .0
        if not dt.tzinfo:
            dt = UTC.localize(dt)
        balances = {}
        total = .0
        for k, v in self.balances.items():
            qty = v['qty'] + self.debt_balances[k]['qty']
            jpy = self.get_rate(k, dt) * qty
            balances[k] = dict(qty=qty, jpy=jpy)
            total += jpy
        balances['total'] = total
        return balances

    def get_collection(self, db: str, collection: str):
        key = (db, collection)
        if key not in self._collection_cache:
            self._collection_cache[key] = pymongo.MongoClient()[db][collection]
        return self._collection_cache[key]

    def get_fiat_rate(self, currency: str, dt: datetime):
        if currency in ('JPY',):
            return 1.0
        d = self.FIAT_CURRENCIES[currency]
        db = d['db']
        collection = self.get_collection(db, '{}/JPY'.format(currency))
        for doc in collection.find({'time': {'$lt': dt - timedelta(days=1)}}).sort('time', -1):
            return doc['c']
        assert False

    def get_crypto_rate(self, currency: str, dt: datetime):
        d = self.CRYPTO_CURRENCIES[currency]
        db, quote = d['db'], d['quote']
        #   collection = self.get_collection(db, '{}/{}_M1'.format(currency, quote))
        collection = self.get_collection(db, '{}/{}_D'.format(currency, quote))
        dt_base = (dt - relativedelta(days=2)).replace(hour=0, minute=0, second=0, microsecond=0)
        for doc in collection.find({'time': {'$gt': dt_base}}).sort('time', 1):
            #        for doc in collection.find({'time': {'$lt': dt - timedelta(minutes=1)}}).sort('time', -1):
            return doc['vwap'] * self.get_fiat_rate(quote, dt)
        assert False

    def get_rate(self, currency: str, dt: datetime):
        assert dt.tzinfo
        dt_base = dt.astimezone(JST).replace(hour=0, minute=0, second=0, microsecond=0)
        key = (currency, dt_base)
        if dt_base not in self.rate_cache:
            if currency in self.FIAT_CURRENCIES:
                rate = self.get_fiat_rate(currency, dt)
            else:
                assert currency in self.CRYPTO_CURRENCIES, (currency, self.CRYPTO_CURRENCIES)
                rate = self.get_crypto_rate(currency, dt)
            self.rate_cache[key] = rate
        return self.rate_cache[key]

    def import_data(self, exchanges: Sequence[str], start: datetime, stop: datetime):
        start_after = start
        stop = stop
        db_client = pymongo.MongoClient()
        for exchange in sorted(exchanges):
            collection = db_client[exchange]['converted']
            for doc in collection.find({'time': {'$gt': start_after, '$lt': stop}},
                                       {'_id': 0}).sort('time', 1):
                assert '_id' not in doc
                if doc.get('skip'):
                    continue
                kind = doc['kind']
                assert len(doc['pnl']) > 0
                if isinstance(doc['pnl'][0], str):
                    pnl_list = [doc['pnl']]
                else:
                    pnl_list = doc['pnl']
                currencies = list(set([self.CURRENCY_MAP.get(_[0], _[0]) for _ in pnl_list]))
                for _ in currencies:
                    assert _ in self.SUPPORTED_CURRENCIES, (_, self.SUPPORTED_CURRENCIES)
                incomes = defaultdict(float)
                outcomes = defaultdict(float)
                fees = defaultdict(float)
                for currency, qty, note in pnl_list:
                    if not qty:
                        continue
                    currency = self.CURRENCY_MAP.get(currency, currency)
                    if 'fee' in note:
                        assert qty < 0
                        fees[currency] += qty
                    elif qty > 0:
                        assert 'fee' not in note
                        incomes[currency] += qty
                    elif qty < 0:
                        assert 'fee' not in note
                        outcomes[currency] += qty
                    else:
                        assert False, doc
                if kind in ('deposit', 'withdrawal'):
                    if list(currencies) == ['JPY']:
                        kind = 'jpy_{}'.format(kind)
                incomes = list(sorted(incomes.items()))
                outcomes = list(sorted(outcomes.items()))
                fees = list(sorted(fees.items()))
                doc.update(kind=kind, exchange=exchange,
                           incomes=incomes, outcomes=outcomes, fees=fees)
                del doc['pnl']
                yield doc

    def calculate_ma_old(self, doc: dict):
        doc_time = doc['time']
        kind = doc['kind']
        instrument = ''
        side = ''
        incomes = defaultdict(lambda: dict(qty=.0, jpy=.0))
        outcomes = defaultdict(lambda: dict(qty=.0, jpy=.0))
        fees = defaultdict(lambda: dict(qty=.0, jpy=.0))

        def update_balance(_c: str, _q: float, _jpy: Optional[float]):
            return self.update_balance(_c, doc_time, _q, _jpy)

        pnl = .0
        if kind in ('jpy_deposit', 'jpy_withdrawal'):
            for currency, qty in doc['incomes']:
                assert currency not in incomes
                # incomes[currency] = update_balance(currency, qty, None)
            for currency, qty in doc['outcomes']:
                assert currency not in outcomes
                # outcomes[currency] = update_balance(currency, qty, None)
            for currency, qty in doc['fees']:
                assert currency not in fees
                v = fees[currency] = update_balance(currency, qty, None)
                pnl += v['jpy']
        elif kind in ('deposit', 'withdrawal'):
            for currency, qty in doc['incomes']:
                assert currency not in incomes
                # incomes[currency] = update_balance(currency, qty, None)
            for currency, qty in doc['outcomes']:
                assert currency not in outcomes
                # outcomes[currency] = update_balance(currency, qty, None)
            for currency, qty in doc['fees']:
                assert currency not in fees
                v = fees[currency] = update_balance(currency, qty, None)
                pnl += v['jpy']
        elif kind in ('margin_deposit', 'margin_withdrawal',):
            assert not fees
            for currency, qty in doc['incomes']:
                rate = self.get_rate(currency, doc_time)
                v = incomes[currency] = update_balance(currency, qty, qty * rate)
                pnl += v['jpy']
            for currency, qty in doc['outcomes']:
                v = outcomes[currency] = update_balance(currency, qty, None)
                pnl += v['jpy']
            for currency, qty in doc['fees']:
                assert currency not in fees
                v = fees[currency] = update_balance(currency, qty, None)
                pnl += v['jpy']
        elif kind == 'margin_transfer':
            assert False
        elif kind in ('spot', 'spot_fee'):
            instrument = doc['instrument']
            base, quote = instrument.split('/')
            base = self.CURRENCY_MAP.get(base, base)
            quote = self.CURRENCY_MAP.get(quote, quote)
            side = doc['side']
            if side == 'BUY':
                income_currency = base
                outcome_currency = quote
            else:
                income_currency = quote
                outcome_currency = base
            assert base in self.CRYPTO_CURRENCIES
            if quote in self.FIAT_CURRENCIES:
                if side == 'BUY':
                    for currency, qty in doc['incomes']:
                        assert currency == income_currency
                        assert currency not in incomes
                        incomes[currency] = update_balance(currency, qty, 0)
                    for currency, qty in doc['outcomes']:
                        assert currency == outcome_currency
                        assert currency not in outcomes
                        outcomes[currency] = v = update_balance(currency, qty, None)
                        v = self.update_balance(income_currency, doc_time, 0, abs(v['jpy']))
                        incomes[income_currency]['jpy'] += v['jpy']
                else:
                    for currency, qty in doc['incomes']:
                        assert currency == income_currency
                        assert currency not in incomes
                        v = incomes[currency] = update_balance(currency, qty, None)
                        pnl += v['jpy']
                    for currency, qty in doc['outcomes']:
                        assert currency == outcome_currency
                        assert currency not in outcomes
                        v = outcomes[currency] = update_balance(currency, qty, None)
                        pnl += v['jpy']
            else:
                for currency, qty in doc['incomes']:
                    assert currency == income_currency
                    assert currency not in incomes
                    v = incomes[currency] = update_balance(currency, qty, None)
                    pnl += v['jpy']
                for currency, qty in doc['outcomes']:
                    assert currency == outcome_currency
                    assert currency not in outcomes
                    v = outcomes[currency] = update_balance(currency, qty, None)
                    pnl += v['jpy']
            for currency, qty in doc['fees']:
                assert currency not in fees
                v = update_balance(currency, qty, None)
                fees[currency] = v
                update_balance(income_currency, 0, abs(v['jpy']))
        else:
            for currency, qty in doc['incomes']:
                assert currency not in incomes
                v = incomes[currency] = update_balance(currency, qty, None)
                pnl += v['jpy']
            for currency, qty in doc['outcomes']:
                assert currency not in outcomes
                v = outcomes[currency] = update_balance(currency, qty, None)
                pnl += v['jpy']
            for currency, qty in doc['fees']:
                assert currency not in fees
                v = fees[currency] = update_balance(currency, qty, None)
                pnl += v['jpy']
        # assert incomes or outcomes or fees
        for v in list(self.balances.values()) + list(self.debt_balances.values()):
            if v['qty']:
                v['price'] = v['jpy'] / v['qty']
        self.balances['pnl']['jpy'] += pnl
        return dict(time=doc_time,
                    exchange=doc['exchange'],
                    kind=kind,
                    id=doc['id'],
                    instrument=instrument,
                    side=side,
                    _incomes=incomes,
                    _outcomes=outcomes,
                    _fees=fees,
                    __incomes=doc['incomes'],
                    __outcomes=doc['outcomes'],
                    __fees=doc['fees'],
                    pnl=pnl,
                    balances=self.balances,
                    debt_balances=self.debt_balances)

    def update_balance(self, currency: str, doc_time: datetime, qty: float, jpy: Optional[float]):
        balance = self.balances[currency]
        pre_qty = balance['qty']
        pre_jpy = balance['jpy']
        debt_qty = 0
        debt_jpy = 0
        rate_note = None

        assert isinstance(qty, float) or isinstance(jpy, float)

        balance['qty'] += qty
        if isinstance(qty, (int, float)) and isinstance(jpy, (int, float)):
            balance['jpy'] += jpy
        elif balance['qty'] >= 0:
            if qty < 0:
                assert jpy is None, jpy
                rate = qty / pre_qty
                jpy = balance['jpy'] * rate
                rate_note = (currency, None, jpy / qty)
            else:
                if jpy is None:
                    rate = self.get_rate(currency, doc_time)
                    rate_note = (currency, doc_time, rate)
                    jpy = qty * rate
                assert jpy is not None
            balance['jpy'] += jpy
        else:
            debt = self.debt_balances[currency]
            debt_qty = balance['qty']
            rate = self.get_rate(currency, doc_time)
            rate_note = (currency, doc_time, rate)
            debt_jpy = balance['qty'] * rate
            debt['qty'] += debt_qty
            debt['jpy'] += debt_jpy
            balance['qty'] = balance['jpy'] = .0
        qty = balance['qty'] - pre_qty + debt_qty
        jpy = balance['jpy'] - pre_jpy + debt_jpy
        if balance['qty']:
            balance['price'] = balance['jpy'] / balance['qty']
        else:
            balance['price'] = float('nan')
        return dict(qty=qty, jpy=jpy, rates=rate_note)

    def calculate_ma(self, doc: dict):
        doc_time = doc['time']
        kind = doc['kind']
        instrument = ''
        side = ''
        incomes = defaultdict(lambda: dict(qty=.0, jpy=.0))
        outcomes = defaultdict(lambda: dict(qty=.0, jpy=.0))
        fees = defaultdict(lambda: dict(qty=.0, jpy=.0))

        def update_balance(_c: str, _q: float, _jpy: Optional[float]):
            if _c == 'JPY':
                return dict(qty=_q, jpy=_q)
            return self.update_balance(_c, doc_time, _q, _jpy)

        pnl = .0
        if kind in ('jpy_deposit', 'jpy_withdrawal'):
            if not doc['fees']:
                return
            for currency, qty in doc['incomes']:
                assert currency not in incomes
                # incomes[currency] = update_balance(currency, qty, None)
            for currency, qty in doc['outcomes']:
                assert currency not in outcomes
                # outcomes[currency] = update_balance(currency, qty, None)
            for currency, qty in doc['fees']:
                assert currency not in fees
                v = fees[currency] = update_balance(currency, qty, None)
                pnl += v['jpy']
        elif kind in ('deposit', 'withdrawal', 'withdrawal_fee'):
            if not doc['fees']:
                return
            for currency, qty in doc['incomes']:
                assert currency not in incomes
                # incomes[currency] = update_balance(currency, qty, None)
            for currency, qty in doc['outcomes']:
                assert currency not in outcomes
                # outcomes[currency] = update_balance(currency, qty, None)
            for currency, qty in doc['fees']:
                assert currency not in fees
                v = fees[currency] = update_balance(currency, qty, None)
                pnl += v['jpy']
        elif kind in ('margin_deposit', 'margin_withdrawal',):
            assert not fees
            for currency, qty in doc['incomes']:
                rate = self.get_rate(currency, doc_time)
                v = incomes[currency] = update_balance(currency, qty, qty * rate)
                pnl += v['jpy']
            for currency, qty in doc['outcomes']:
                v = outcomes[currency] = update_balance(currency, qty, None)
                pnl += v['jpy']
            for currency, qty in doc['fees']:
                assert currency not in fees
                v = fees[currency] = update_balance(currency, qty, None)
                pnl += v['jpy']
        elif kind == 'margin_transfer':
            assert False
        elif kind in ('spot', 'spot_fee'):
            instrument = doc['instrument']
            base, quote = instrument.split('/')
            base = self.CURRENCY_MAP.get(base, base)
            quote = self.CURRENCY_MAP.get(quote, quote)
            side = doc['side']
            if side == 'BUY':
                income_currency = base
                outcome_currency = quote
            else:
                income_currency = quote
                outcome_currency = base
            assert base in self.CRYPTO_CURRENCIES
            if quote in self.FIAT_CURRENCIES and quote == 'JPY':
                if side == 'BUY':
                    for currency, qty in doc['incomes']:
                        assert currency == income_currency
                        assert currency not in incomes
                        incomes[currency] = update_balance(currency, qty, 0)
                    for currency, qty in doc['outcomes']:
                        assert currency == outcome_currency
                        assert currency not in outcomes
                        outcomes[currency] = v = update_balance(currency, qty, None)
                        v = self.update_balance(income_currency, doc_time, 0, abs(v['jpy']))
                        incomes[income_currency]['jpy'] += v['jpy']
                    for currency, qty in doc['fees']:
                        assert currency not in fees
                        v = update_balance(currency, qty, None)
                        assert v['jpy'] <= 0
                        fees[currency] = v
                        update_balance(income_currency, 0, abs(v['jpy']))
                else:
                    for currency, qty in doc['incomes']:
                        assert currency == income_currency
                        assert currency not in incomes
                        v = incomes[currency] = update_balance(currency, qty, None)
                        pnl += v['jpy']
                    for currency, qty in doc['outcomes']:
                        assert currency == outcome_currency
                        assert currency not in outcomes
                        v = outcomes[currency] = update_balance(currency, qty, None)
                        pnl += v['jpy']
                    for currency, qty in doc['fees']:
                        assert currency not in fees
                        v = update_balance(currency, qty, None)
                        fees[currency] = v
                        assert v['jpy'] <= 0
                        pnl += v['jpy']
                        # update_balance(income_currency, 0, abs(v['jpy']))
            else:
                for currency, qty in doc['incomes']:
                    assert currency == income_currency
                    assert currency not in incomes
                    v = incomes[currency] = update_balance(currency, qty, None)
                    pnl += v['jpy']
                for currency, qty in doc['outcomes']:
                    assert currency == outcome_currency
                    assert currency not in outcomes
                    v = outcomes[currency] = update_balance(currency, qty, None)
                    pnl += v['jpy']
                for currency, qty in doc['fees']:
                    assert currency not in fees
                    v = update_balance(currency, qty, None)
                    fees[currency] = v
                    update_balance(income_currency, 0, abs(v['jpy']))
        else:
            for currency, qty in doc['incomes']:
                assert currency not in incomes
                v = incomes[currency] = update_balance(currency, qty, None)
                pnl += v['jpy']
            for currency, qty in doc['outcomes']:
                assert currency not in outcomes
                v = outcomes[currency] = update_balance(currency, qty, None)
                pnl += v['jpy']
            for currency, qty in doc['fees']:
                assert currency not in fees
                v = fees[currency] = update_balance(currency, qty, None)
                pnl += v['jpy']
        # assert incomes or outcomes or fees
        for v in list(self.balances.values()) + list(self.debt_balances.values()):
            if v['qty']:
                v['price'] = v['jpy'] / v['qty']
        self.pnl += pnl
        return dict(time=doc_time,
                    exchange=doc['exchange'],
                    kind=kind,
                    id=doc['id'],
                    instrument=instrument,
                    side=side,
                    _incomes=incomes,
                    _outcomes=outcomes,
                    _fees=fees,
                    pnl=self.pnl,
                    pnl_delta=pnl,
                    balances=self.balances,
                    debt_balances=self.debt_balances)

    def calculate_ma2(self, doc: dict):
        doc_time = doc['time']
        kind = doc['kind']
        instrument = ''
        side = ''
        incomes = defaultdict(lambda: dict(qty=.0, jpy=.0))
        outcomes = defaultdict(lambda: dict(qty=.0, jpy=.0))
        fees = defaultdict(lambda: dict(qty=.0, jpy=.0))

        def update_balance(_c: str, _q: float, _jpy: Optional[float]):
            if _c == 'JPY':
                self.update_balance(_c, doc_time, _q, _q)
                return dict(qty=_q, jpy=_q)
            return self.update_balance(_c, doc_time, _q, _jpy)

        pnl = .0
        if kind in ('jpy_deposit', 'jpy_withdrawal'):
            if not doc['fees']:
                return
            for currency, qty in doc['incomes']:
                assert currency not in incomes
                # incomes[currency] = update_balance(currency, qty, None)
            for currency, qty in doc['outcomes']:
                assert currency not in outcomes
                # outcomes[currency] = update_balance(currency, qty, None)
            for currency, qty in doc['fees']:
                assert currency not in fees
                v = fees[currency] = update_balance(currency, qty, None)
                pnl += v['jpy']
        elif kind in ('deposit', 'withdrawal', 'withdrawal_fee'):
            if not doc['fees']:
                return
            for currency, qty in doc['incomes']:
                assert currency not in incomes
                # incomes[currency] = update_balance(currency, qty, None)
            for currency, qty in doc['outcomes']:
                assert currency not in outcomes
                # outcomes[currency] = update_balance(currency, qty, None)
            for currency, qty in doc['fees']:
                assert currency not in fees
                v = fees[currency] = update_balance(currency, qty, None)
                pnl += v['jpy']
        elif kind in ('margin_deposit', 'margin_withdrawal',):
            assert not fees
            for currency, qty in doc['incomes']:
                rate = self.get_rate(currency, doc_time)
                v = incomes[currency] = update_balance(currency, qty, qty * rate)
                pnl += v['jpy']
            for currency, qty in doc['outcomes']:
                v = outcomes[currency] = update_balance(currency, qty, None)
                pnl += v['jpy']
            for currency, qty in doc['fees']:
                assert currency not in fees
                v = fees[currency] = update_balance(currency, qty, None)
                pnl += v['jpy']
        elif kind == 'margin_transfer':
            assert False
        elif kind in ('spot', 'spot_fee'):
            instrument = doc['instrument']
            base, quote = instrument.split('/')
            base = self.CURRENCY_MAP.get(base, base)
            quote = self.CURRENCY_MAP.get(quote, quote)
            side = doc['side']
            if side == 'BUY':
                income_currency = base
                outcome_currency = quote
            else:
                income_currency = quote
                outcome_currency = base
            assert base in self.CRYPTO_CURRENCIES
            if quote in self.FIAT_CURRENCIES and quote == 'JPY':
                if side == 'BUY':
                    for currency, qty in doc['incomes']:
                        assert currency == income_currency
                        assert currency not in incomes
                        incomes[currency] = update_balance(currency, qty, 0)
                    for currency, qty in doc['outcomes']:
                        assert currency == outcome_currency
                        assert currency not in outcomes
                        outcomes[currency] = v = update_balance(currency, qty, None)
                        v = self.update_balance(income_currency, doc_time, 0, abs(v['jpy']))
                        incomes[income_currency]['jpy'] += v['jpy']
                    for currency, qty in doc['fees']:
                        assert currency not in fees
                        v = update_balance(currency, qty, None)
                        assert v['jpy'] <= 0
                        fees[currency] = v
                        update_balance(income_currency, 0, abs(v['jpy']))
                else:
                    for currency, qty in doc['incomes']:
                        assert currency == income_currency
                        assert currency not in incomes
                        v = incomes[currency] = update_balance(currency, qty, None)
                        pnl += v['jpy']
                    for currency, qty in doc['outcomes']:
                        assert currency == outcome_currency
                        assert currency not in outcomes
                        v = outcomes[currency] = update_balance(currency, qty, None)
                        pnl += v['jpy']
                    for currency, qty in doc['fees']:
                        assert currency not in fees
                        v = update_balance(currency, qty, None)
                        fees[currency] = v
                        assert v['jpy'] <= 0
                        pnl += v['jpy']
                        # update_balance(income_currency, 0, abs(v['jpy']))
            else:
                for currency, qty in doc['incomes']:
                    assert currency == income_currency
                    assert currency not in incomes
                    v = incomes[currency] = update_balance(currency, qty, None)
                    pnl += v['jpy']
                for currency, qty in doc['outcomes']:
                    assert currency == outcome_currency
                    assert currency not in outcomes
                    v = outcomes[currency] = update_balance(currency, qty, None)
                    pnl += v['jpy']
                for currency, qty in doc['fees']:
                    assert currency not in fees
                    v = update_balance(currency, qty, None)
                    fees[currency] = v
                    update_balance(income_currency, 0, abs(v['jpy']))
        else:
            for currency, qty in doc['incomes']:
                assert currency not in incomes
                v = incomes[currency] = update_balance(currency, qty, None)
                pnl += v['jpy']
            for currency, qty in doc['outcomes']:
                assert currency not in outcomes
                v = outcomes[currency] = update_balance(currency, qty, None)
                pnl += v['jpy']
            for currency, qty in doc['fees']:
                assert currency not in fees
                v = fees[currency] = update_balance(currency, qty, None)
                pnl += v['jpy']
        # assert incomes or outcomes or fees
        for v in list(self.balances.values()) + list(self.debt_balances.values()):
            if v['qty']:
                v['price'] = v['jpy'] / v['qty']
        self.pnl += pnl
        return dict(time=doc_time,
                    exchange=doc['exchange'],
                    kind=kind,
                    id=doc['id'],
                    instrument=instrument,
                    side=side,
                    _incomes=incomes,
                    _outcomes=outcomes,
                    _fees=fees,
                    pnl=self.pnl,
                    pnl_delta=pnl,
                    balances=self.balances,
                    debt_balances=self.debt_balances)

    def ga(self, doc: dict):
        doc_time = doc['time']
        kind = doc['kind']
        instrument = ''
        side = ''
        incomes = defaultdict(lambda: dict(qty=.0, jpy=.0))
        outcomes = defaultdict(lambda: dict(qty=.0, jpy=.0))
        fees = defaultdict(lambda: dict(qty=.0, jpy=.0))

        def update_cost(_c: str, _q: float, _jpy: Optional[float]):
            _cost = self.ga_costs[_c]
            _cost['qty'] += _q
            _rate = None
            if _jpy is None:
                _rate = self.get_rate(_c, doc_time)
                _jpy = _q * _rate
            _cost['jpy'] += _jpy
            return dict(qty=_q, jpy=_jpy, rate_note=_rate)

        def sub_cost(_c: str, _q: float):
            self.ga_outcomes[_c] += _q
            return dict(qty=_q)

        pnl = .0
        if kind in ('jpy_deposit', 'jpy_withdrawal'):
            if not doc['fees']:
                return
            for currency, qty in doc['incomes']:
                assert currency not in incomes
                # incomes[currency] = update_balance(currency, qty, None)
            for currency, qty in doc['outcomes']:
                assert currency not in outcomes
                # outcomes[currency] = update_balance(currency, qty, None)
            for currency, qty in doc['fees']:
                assert currency not in fees
                v = fees[currency] = update_cost(currency, qty, None)
                pnl += v['jpy']
        elif kind in ('deposit', 'withdrawal', 'withdrawal_fee'):
            if not doc['fees']:
                return
            for currency, qty in doc['incomes']:
                assert currency not in incomes
                # incomes[currency] = update_balance(currency, qty, None)
            for currency, qty in doc['outcomes']:
                assert currency not in outcomes
                # outcomes[currency] = update_balance(currency, qty, None)
            for currency, qty in doc['fees']:
                assert currency not in fees
                v = fees[currency] = update_cost(currency, qty, None)
                pnl += v['jpy']
        elif kind in ('margin_deposit', 'margin_withdrawal',):
            assert not doc['fees']
            for currency, qty in doc['incomes']:
                incomes[currency] = update_cost(currency, qty, None)
            for currency, qty in doc['outcomes']:
                outcomes[currency] = sub_cost(currency, qty)
        elif kind == 'margin_transfer':
            assert False
        elif kind in ('spot', 'spot_fee'):
            instrument = doc['instrument']
            base, quote = instrument.split('/')
            base = self.CURRENCY_MAP.get(base, base)
            quote = self.CURRENCY_MAP.get(quote, quote)
            side = doc['side']
            if side == 'BUY':
                income_currency = base
                outcome_currency = quote
            else:
                income_currency = quote
                outcome_currency = base
            assert base in self.CRYPTO_CURRENCIES
            if quote in self.FIAT_CURRENCIES and quote == 'JPY':
                if side == 'BUY':
                    for currency, qty in doc['incomes']:
                        assert currency == income_currency
                        assert currency not in incomes
                        incomes[currency] = update_cost(currency, qty, 0)
                    for currency, qty in doc['outcomes']:
                        assert currency == outcome_currency
                        assert currency not in outcomes
                        # outcomes[currency] = v = update_balance(currency, qty, None)
                        v = update_cost(income_currency, .0, abs(qty))
                        incomes[income_currency]['jpy'] += v['jpy']
                    for currency, qty in doc['fees']:
                        assert currency not in fees
                        v = update_cost(currency, qty, None)
                        assert v['jpy'] <= 0
                        fees[currency] = v
                        update_cost(income_currency, 0, abs(v['jpy']))
                else:
                    for currency, qty in doc['incomes']:
                        assert currency == income_currency
                        assert currency not in incomes
                        rate = self.get_rate(currency, doc_time)
                        v = incomes[currency] = dict(qty=qty, jpy=qty * rate)  # update_balance(currency, qty, None)
                        pnl += v['jpy']
                    for currency, qty in doc['outcomes']:
                        assert currency == outcome_currency
                        assert currency not in outcomes
                        v = outcomes[currency] = sub_cost(currency, qty)
                        pnl += v['jpy']
                    for currency, qty in doc['fees']:
                        assert currency not in fees
                        v = fees[currency] = sub_cost(currency, qty)
                        assert v['jpy'] <= 0
                        pnl += v['jpy']
                        # update_balance(income_currency, 0, abs(v['jpy']))
            else:
                for currency, qty in doc['incomes']:
                    assert currency == income_currency
                    assert currency not in incomes
                    v = incomes[currency] = update_cost(currency, qty, None)
                    pnl += v['jpy']
                for currency, qty in doc['outcomes']:
                    assert currency == outcome_currency
                    assert currency not in outcomes
                    v = outcomes[currency] = sub_cost(currency, qty)
                    # v = outcomes[currency] = update_balance(currency, qty, None)
                    # pnl += v['jpy']
                for currency, qty in doc['fees']:
                    assert currency not in fees
                    v = update_cost(currency, qty, None)
                    fees[currency] = v
                    update_cost(income_currency, 0, abs(v['jpy']))
        else:
            for currency, qty in doc['incomes']:
                assert currency not in incomes
                v = incomes[currency] = update_cost(currency, qty, None)
                pnl += v['jpy']
            for currency, qty in doc['outcomes']:
                assert currency not in outcomes
                v = outcomes[currency] = sub_cost(currency, qty)
                # v = outcomes[currency] = update_balance(currency, qty, None)
                # pnl += v['jpy']
            for currency, qty in doc['fees']:
                assert currency not in fees
                v = fees[currency] = sub_cost(currency, qty)
                assert v['jpy'] <= 0
                pnl += v['jpy']
                # v = fees[currency] = update_balance(currency, qty, None)
                # pnl += v['jpy']
        # assert incomes or outcomes or fees
        for v in list(self.ga_costs.values()):
            if v['qty']:
                v['price'] = v['jpy'] / v['qty']
            v['remain'] = v['qty']
        self.ga_costs['JPY']['price'] = 1.0
        return dict(time=doc_time,
                    exchange=doc['exchange'],
                    kind=kind,
                    id=doc['id'],
                    instrument=instrument,
                    side=side,
                    _incomes=incomes,
                    _outcomes=outcomes,
                    _fees=fees,
                    __incomes=doc['incomes'],
                    __outcomes=doc['outcomes'],
                    __fees=doc['fees'],
                    costs=self.ga_costs,
                    outcomes=self.ga_outcomes)

    def calculate_ga_prepare(self, doc: dict):
        doc_time = doc['time']
        kind = doc['kind']
        instrument = ''
        side = ''
        incomes = defaultdict(lambda: dict(qty=.0, jpy=.0))
        outcomes = defaultdict(lambda: dict(qty=.0, jpy=.0))
        fees = defaultdict(lambda: dict(qty=.0, jpy=.0))

        def update_cost(_c: str, _q: float, _jpy: Optional[float]):
            _cost = self.ga_costs[_c]
            _cost['qty'] += _q
            _rate = None
            if _jpy is None:
                _rate = self.get_rate(_c, doc_time)
                _jpy = _q * _rate
            _cost['jpy'] += _jpy
            return dict(qty=_q, jpy=_jpy, rate_note=_rate)

        pnl = .0
        if kind in ('jpy_deposit', 'jpy_withdrawal'):
            if not doc['fees']:
                return
            for currency, qty in doc['incomes']:
                assert currency not in incomes
                # incomes[currency] = update_balance(currency, qty, None)
            for currency, qty in doc['outcomes']:
                assert currency not in outcomes
                # outcomes[currency] = update_balance(currency, qty, None)
            for currency, qty in doc['fees']:
                assert currency not in fees
                v = fees[currency] = update_cost(currency, qty, None)
                pnl += v['jpy']
        elif kind in ('deposit', 'withdrawal', 'withdrawal_fee'):
            if not doc['fees']:
                return
            for currency, qty in doc['incomes']:
                assert currency not in incomes
                # incomes[currency] = update_balance(currency, qty, None)
            for currency, qty in doc['outcomes']:
                assert currency not in outcomes
                # outcomes[currency] = update_balance(currency, qty, None)
            for currency, qty in doc['fees']:
                assert currency not in fees
                v = fees[currency] = update_cost(currency, qty, None)
                pnl += v['jpy']
        elif kind in ('margin_deposit', 'margin_withdrawal',):
            assert not doc['fees']
            for currency, qty in doc['incomes']:
                rate = self.get_rate(currency, doc_time)
                v = incomes[currency] = update_cost(currency, qty, None)
                pnl += v['jpy']
            for currency, qty in doc['outcomes']:
                continue
                v = outcomes[currency] = update_balance(currency, qty, None)
                pnl += v['jpy']
        elif kind == 'margin_transfer':
            assert False
        elif kind in ('spot', 'spot_fee'):
            instrument = doc['instrument']
            base, quote = instrument.split('/')
            base = self.CURRENCY_MAP.get(base, base)
            quote = self.CURRENCY_MAP.get(quote, quote)
            side = doc['side']
            if side == 'BUY':
                income_currency = base
                outcome_currency = quote
            else:
                income_currency = quote
                outcome_currency = base
            assert base in self.CRYPTO_CURRENCIES
            if quote in self.FIAT_CURRENCIES and quote == 'JPY':
                if side == 'BUY':
                    for currency, qty in doc['incomes']:
                        assert currency == income_currency
                        assert currency not in incomes
                        incomes[currency] = update_cost(currency, qty, 0)
                    for currency, qty in doc['outcomes']:
                        assert currency == outcome_currency
                        assert currency not in outcomes
                        # outcomes[currency] = v = update_balance(currency, qty, None)
                        v = update_cost(income_currency, .0, abs(qty))
                        incomes[income_currency]['jpy'] += v['jpy']
                    for currency, qty in doc['fees']:
                        assert currency not in fees
                        v = update_cost(currency, qty, None)
                        assert v['jpy'] <= 0
                        fees[currency] = v
                        update_cost(income_currency, 0, abs(v['jpy']))
                else:
                    return
                    for currency, qty in doc['incomes']:
                        assert currency == income_currency
                        assert currency not in incomes
                        v = incomes[currency] = update_balance(currency, qty, None)
                        pnl += v['jpy']
                    for currency, qty in doc['outcomes']:
                        assert currency == outcome_currency
                        assert currency not in outcomes
                        v = outcomes[currency] = update_balance(currency, qty, None)
                        pnl += v['jpy']
                    for currency, qty in doc['fees']:
                        assert currency not in fees
                        v = update_balance(currency, qty, None)
                        fees[currency] = v
                        assert v['jpy'] <= 0
                        pnl += v['jpy']
                        # update_balance(income_currency, 0, abs(v['jpy']))
            else:
                for currency, qty in doc['incomes']:
                    assert currency == income_currency
                    assert currency not in incomes
                    v = incomes[currency] = update_cost(currency, qty, None)
                    pnl += v['jpy']
                for currency, qty in doc['outcomes']:
                    assert currency == outcome_currency
                    assert currency not in outcomes
                    # v = outcomes[currency] = update_balance(currency, qty, None)
                    # pnl += v['jpy']
                for currency, qty in doc['fees']:
                    assert currency not in fees
                    v = update_cost(currency, qty, None)
                    fees[currency] = v
                    update_cost(income_currency, 0, abs(v['jpy']))
        else:
            for currency, qty in doc['incomes']:
                assert currency not in incomes
                v = incomes[currency] = update_cost(currency, qty, None)
                pnl += v['jpy']
            for currency, qty in doc['outcomes']:
                assert currency not in outcomes
                # v = outcomes[currency] = update_balance(currency, qty, None)
                # pnl += v['jpy']
            for currency, qty in doc['fees']:
                assert currency not in fees
                # v = fees[currency] = update_balance(currency, qty, None)
                # pnl += v['jpy']
        # assert incomes or outcomes or fees
        for v in list(self.ga_costs.values()):
            if v['qty']:
                v['price'] = v['jpy'] / v['qty']
            v['remain'] = v['qty']
        self.ga_costs['JPY']['price'] = 1.0
        return dict(time=doc_time,
                    exchange=doc['exchange'],
                    kind=kind,
                    id=doc['id'],
                    instrument=instrument,
                    side=side,
                    _incomes=incomes,
                    _outcomes=outcomes,
                    _fees=fees,
                    __incomes=doc['incomes'],
                    __outcomes=doc['outcomes'],
                    __fees=doc['fees'],
                    costs=self.ga_costs)

    def calculate_ga(self, doc: dict):
        doc_time = doc['time']
        kind = doc['kind']
        instrument = ''
        side = ''
        incomes = defaultdict(lambda: dict(qty=.0, jpy=.0))
        outcomes = defaultdict(lambda: dict(qty=.0, jpy=.0))
        fees = defaultdict(lambda: dict(qty=.0, jpy=.0))

        def sub_cost(_c: str, _q: float):
            _cost = self.ga_costs[_c]
            assert _q <= 0, (_c, _q, _cost)
            _rate = _cost['price']
            _cost['remain'] += _q
            return dict(qty=_q, jpy=_q * _rate, rate_note=_rate)

        def update_cost(_c: str, _q: float, _jpy: Optional[float]):
            _cost = self.ga_costs[_c]
            _cost['qty'] += _q
            _rate = None
            if _jpy is None:
                _rate = self.get_rate(_c, doc_time)
                _jpy = _q * _rate
            _cost['jpy'] += _jpy
            return dict(qty=_q, jpy=_jpy, rate_note=_rate)

        pnl = .0
        if kind in ('jpy_deposit', 'jpy_withdrawal'):
            if not doc['fees']:
                return
            for currency, qty in doc['incomes']:
                assert currency not in incomes
                # incomes[currency] = update_balance(currency, qty, None)
            for currency, qty in doc['outcomes']:
                assert currency not in outcomes
                # outcomes[currency] = update_balance(currency, qty, None)
            for currency, qty in doc['fees']:
                assert currency not in fees
                # v = fees[currency] = update_cost(currency, qty, None)
                # pnl += v['jpy']
        elif kind in ('deposit', 'withdrawal', 'withdrawal_fee'):
            if not doc['fees']:
                return
            for currency, qty in doc['incomes']:
                assert currency not in incomes
                # incomes[currency] = update_balance(currency, qty, None)
            for currency, qty in doc['outcomes']:
                assert currency not in outcomes
                # outcomes[currency] = update_balance(currency, qty, None)
            for currency, qty in doc['fees']:
                assert currency not in fees
                # v = fees[currency] = update_cost(currency, qty, None)
                # pnl += v['jpy']
        elif kind in ('margin_deposit', 'margin_withdrawal',):
            assert not doc['fees']
            for currency, qty in doc['incomes']:
                rate = self.get_rate(currency, doc_time)
                pnl += qty * rate
                # v = incomes[currency] = update_cost(currency, qty, None)
                # pnl += v['jpy']
            for currency, qty in doc['outcomes']:
                v = sub_cost(currency, qty)
                pnl += v['jpy']
        elif kind == 'margin_transfer':
            assert False
        elif kind in ('spot', 'spot_fee'):
            instrument = doc['instrument']
            base, quote = instrument.split('/')
            base = self.CURRENCY_MAP.get(base, base)
            quote = self.CURRENCY_MAP.get(quote, quote)
            side = doc['side']
            if side == 'BUY':
                income_currency = base
                outcome_currency = quote
            else:
                income_currency = quote
                outcome_currency = base
            assert base in self.CRYPTO_CURRENCIES
            if quote in self.FIAT_CURRENCIES and quote == 'JPY':
                if side == 'BUY':
                    return
                    for currency, qty in doc['incomes']:
                        assert currency == income_currency
                        assert currency not in incomes
                        incomes[currency] = update_cost(currency, qty, 0)
                    for currency, qty in doc['outcomes']:
                        assert currency == outcome_currency
                        assert currency not in outcomes
                        # outcomes[currency] = v = update_balance(currency, qty, None)
                        v = update_cost(currency, .0, abs(qty))
                        incomes[income_currency]['jpy'] += v['jpy']
                    for currency, qty in doc['fees']:
                        assert currency not in fees
                        v = fees[currency] = update_cost(currency, qty, None)
                        assert v['jpy'] <= 0
                        update_cost(income_currency, 0, abs(v['jpy']))
                else:
                    for currency, qty in doc['incomes']:
                        assert currency == income_currency
                        assert currency not in incomes
                        rate = self.get_rate(currency, doc_time)
                        v = incomes[currency] = dict(qty=qty, jpy=qty * rate)  # update_balance(currency, qty, None)
                        pnl += v['jpy']
                    for currency, qty in doc['outcomes']:
                        assert currency == outcome_currency
                        assert currency not in outcomes
                        v = outcomes[currency] = sub_cost(currency, qty)
                        pnl += v['jpy']
                    for currency, qty in doc['fees']:
                        assert currency not in fees
                        v = fees[currency] = sub_cost(currency, qty)
                        assert v['jpy'] <= 0
                        pnl += v['jpy']
                        # update_balance(income_currency, 0, abs(v['jpy']))
            else:
                for currency, qty in doc['incomes']:
                    assert currency == income_currency
                    assert currency not in incomes
                    rate = self.get_rate(currency, doc_time)
                    v = incomes[currency] = dict(qty=qty, jpy=qty * rate)  # update_balance(currency, qty, None)
                    pnl += v['jpy']
                for currency, qty in doc['outcomes']:
                    assert currency == outcome_currency
                    assert currency not in outcomes
                    v = outcomes[currency] = sub_cost(currency, qty)
                    pnl += v['jpy']
                    # v = outcomes[currency] = update_balance(currency, qty, None)
                    # pnl += v['jpy']
                for currency, qty in doc['fees']:
                    assert currency not in fees
                    # v = update_cost(currency, qty, None)
                    # fees[currency] = v
                    # update_cost(income_currency, 0, abs(v['jpy']))
        else:
            for currency, qty in doc['incomes']:
                assert currency not in incomes
                rate = self.get_rate(currency, doc_time)
                v = incomes[currency] = dict(qty=qty, jpy=qty * rate)  # update_balance(currency, qty, None)
                pnl += v['jpy']
                # v = incomes[currency] = update_cost(currency, qty, None)
                # pnl += v['jpy']
            for currency, qty in doc['outcomes']:
                assert currency not in outcomes
                v = outcomes[currency] = sub_cost(currency, qty)
                pnl += v['jpy']
                # v = outcomes[currency] = update_balance(currency, qty, None)
                # pnl += v['jpy']
            for currency, qty in doc['fees']:
                assert currency not in fees
                v = fees[currency] = sub_cost(currency, qty)
                assert v['jpy'] <= 0
                pnl += v['jpy']
                # v = fees[currency] = update_balance(currency, qty, None)
                # pnl += v['jpy']
        # assert incomes or outcomes or fees
        self.pnl += pnl
        return dict(time=doc_time,
                    exchange=doc['exchange'],
                    kind=kind,
                    id=doc['id'],
                    instrument=instrument,
                    side=side,
                    _incomes=incomes,
                    _outcomes=outcomes,
                    _fees=fees,
                    __incomes=doc['incomes'],
                    __outcomes=doc['outcomes'],
                    __fees=doc['fees'],
                    pnl=self.pnl,
                    pnl_delta=pnl)

    def calculate_simple(self, doc: dict):
        doc_time = doc['time']
        kind = doc['kind']
        instrument = ''
        side = ''
        incomes = defaultdict(lambda: dict(qty=.0, jpy=.0))
        outcomes = defaultdict(lambda: dict(qty=.0, jpy=.0))
        fees = defaultdict(lambda: dict(qty=.0, jpy=.0))

        def update_balance(_c: str, _q: float, _jpy: Optional[float]):
            if _c == 'JPY':
                return dict(qty=_q, jpy=_q)
            return self.update_balance(_c, doc_time, _q, _jpy)

        pnl = .0
        if kind in ('jpy_deposit', 'jpy_withdrawal'):
            if not doc['fees']:
                return
            for currency, qty in doc['incomes']:
                assert currency not in incomes
                # incomes[currency] = update_balance(currency, qty, None)
            for currency, qty in doc['outcomes']:
                assert currency not in outcomes
                # outcomes[currency] = update_balance(currency, qty, None)
            for currency, qty in doc['fees']:
                assert currency not in fees
                v = fees[currency] = update_balance(currency, qty, None)
                pnl += v['jpy']
        elif kind in ('deposit', 'withdrawal'):
            if not doc['fees']:
                return
            for currency, qty in doc['incomes']:
                assert currency not in incomes
                # incomes[currency] = update_balance(currency, qty, None)
            for currency, qty in doc['outcomes']:
                assert currency not in outcomes
                # outcomes[currency] = update_balance(currency, qty, None)
            for currency, qty in doc['fees']:
                assert currency not in fees
                v = fees[currency] = update_balance(currency, qty, None)
                pnl += v['jpy']
        elif kind in ('margin_deposit', 'margin_withdrawal',):
            assert not fees
            for currency, qty in doc['incomes']:
                rate = self.get_rate(currency, doc_time)
                v = incomes[currency] = update_balance(currency, qty, qty * rate)
                pnl += v['jpy']
            for currency, qty in doc['outcomes']:
                v = outcomes[currency] = update_balance(currency, qty, None)
                pnl += v['jpy']
            for currency, qty in doc['fees']:
                assert currency not in fees
                v = fees[currency] = update_balance(currency, qty, None)
                pnl += v['jpy']
        elif kind == 'margin_transfer':
            assert False
        elif kind in ('spot', 'spot_fee'):
            instrument = doc['instrument']
            base, quote = instrument.split('/')
            base = self.CURRENCY_MAP.get(base, base)
            quote = self.CURRENCY_MAP.get(quote, quote)
            side = doc['side']
            if side == 'BUY':
                income_currency = base
                outcome_currency = quote
            else:
                income_currency = quote
                outcome_currency = base
            assert base in self.CRYPTO_CURRENCIES
            if quote in self.FIAT_CURRENCIES and quote == 'JPY':
                if side == 'BUY':
                    for currency, qty in doc['incomes']:
                        assert currency == income_currency
                        assert currency not in incomes
                        incomes[currency] = update_balance(currency, qty, 0)
                    for currency, qty in doc['outcomes']:
                        assert currency == outcome_currency
                        assert currency not in outcomes
                        outcomes[currency] = v = update_balance(currency, qty, None)
                        v = self.update_balance(income_currency, doc_time, 0, abs(v['jpy']))
                        incomes[income_currency]['jpy'] += v['jpy']
                    for currency, qty in doc['fees']:
                        assert currency not in fees
                        v = update_balance(currency, qty, None)
                        assert v['jpy'] <= 0
                        fees[currency] = v
                        update_balance(income_currency, 0, abs(v['jpy']))
                else:
                    for currency, qty in doc['incomes']:
                        assert currency == income_currency
                        assert currency not in incomes
                        v = incomes[currency] = update_balance(currency, qty, None)
                        pnl += v['jpy']
                    for currency, qty in doc['outcomes']:
                        assert currency == outcome_currency
                        assert currency not in outcomes
                        v = outcomes[currency] = update_balance(currency, qty, None)
                        pnl += v['jpy']
                    for currency, qty in doc['fees']:
                        assert currency not in fees
                        v = update_balance(currency, qty, None)
                        fees[currency] = v
                        assert v['jpy'] <= 0
                        pnl += v['jpy']
                        # update_balance(income_currency, 0, abs(v['jpy']))
            else:
                for currency, qty in doc['incomes']:
                    assert currency == income_currency
                    assert currency not in incomes
                    v = incomes[currency] = update_balance(currency, qty, None)
                    pnl += v['jpy']
                for currency, qty in doc['outcomes']:
                    assert currency == outcome_currency
                    assert currency not in outcomes
                    v = outcomes[currency] = update_balance(currency, qty, None)
                    pnl += v['jpy']
                for currency, qty in doc['fees']:
                    assert currency not in fees
                    v = update_balance(currency, qty, None)
                    fees[currency] = v
                    update_balance(income_currency, 0, abs(v['jpy']))
        else:
            for currency, qty in doc['incomes']:
                assert currency not in incomes
                v = incomes[currency] = update_balance(currency, qty, None)
                pnl += v['jpy']
            for currency, qty in doc['outcomes']:
                assert currency not in outcomes
                v = outcomes[currency] = update_balance(currency, qty, None)
                pnl += v['jpy']
            for currency, qty in doc['fees']:
                assert currency not in fees
                v = fees[currency] = update_balance(currency, qty, None)
                pnl += v['jpy']
        # assert incomes or outcomes or fees
        for v in list(self.balances.values()) + list(self.debt_balances.values()):
            if v['qty']:
                v['price'] = v['jpy'] / v['qty']
        self.pnl += pnl
        return dict(time=doc_time,
                    exchange=doc['exchange'],
                    kind=kind,
                    id=doc['id'],
                    instrument=instrument,
                    side=side,
                    _incomes=incomes,
                    _outcomes=outcomes,
                    _fees=fees,
                    __incomes=doc['incomes'],
                    __outcomes=doc['outcomes'],
                    __fees=doc['fees'],
                    pnl=self.pnl,
                    pnl_delta=pnl,
                    balances=self.balances,
                    debt_balances=self.debt_balances)
