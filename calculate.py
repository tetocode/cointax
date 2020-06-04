import copy
import json
import logging
import pathlib
import sys
from collections import defaultdict
from datetime import timedelta, datetime
from pprint import pprint, pformat

import pymongo
from docopt import docopt

from coinapi.clientbase import ClientBase
from coincalcurator.calculrator import Calculator
from coindb.bulkop import BulkOp

UTC = ClientBase.UTC
JST = ClientBase.JST
utc_now = ClientBase.utc_now
parse_time = ClientBase.parse_time


def support_datetime_default(o):
    if isinstance(o, datetime):
        return o.astimezone(JST).isoformat()
    raise TypeError(repr(o) + " is not JSON serializable")


def main():
    logging.basicConfig(level=logging.INFO)
    args = docopt("""
    Usage:
        {f} [options] (import | export csv)
        {f} [options] (ma_old | ma | ma2 | ga | simple) JSON_FILE
        {f} [options] balance
        {f} [options] asset FILE
        {f} [options] jpy
    
    Options:
        --db DB  [default: tax]
        --collection COLLECTION  [default: pl]
        --start START  [default: 2017-01-01T00:00]
        --stop STOP  [default: 2018-01-01T00:00]
        --exchanges EXCHANGES
        --balance FILE

    """.format(f=pathlib.Path(sys.argv[0]).name))
    json_file = args['JSON_FILE']
    pprint(args)
    db = args['--db']
    collection = args['--collection']
    start = parse_time(args['--start'], JST)
    start_after = start - timedelta(microseconds=1000)
    stop = parse_time(args['--stop'], JST)
    pprint(start)
    pprint(stop)

    exchanges = ['bitfinex', 'bitflyer', 'bitmex',
                 'coincheck', 'kraken', 'minbtc',
                 'quoinex', 'xmr', 'zaif']
    if args['--exchanges']:
        exchanges = args['--exchanges'].split(',')
    db_client = pymongo.MongoClient()
    collection = db_client[db][collection]
    if args['import']:
        collection.create_index([
            ('time', 1),
            ('exchange', 1),
            ('kind', 1),
            ('id', 1)
        ], unique=True)
        collection.drop()
        with BulkOp(collection) as bulk_op:
            for doc in Calculator().import_data(exchanges, start, stop):
                try:
                    bulk_op.insert(doc)
                except Exception:
                    pprint(doc)
                    raise
        return
    if args['asset']:
        calculator = Calculator()
        with open(args['FILE']) as f:
            data = json.load(f)
        cost = .0
        total = .0
        for k, v in sorted(data.items()):
            print(v)
            jpy = v['qty'] * calculator.get_rate(k, stop)
            print('#{} {:,}'.format(k, jpy))
            cost += v['jpy']
            total += jpy
        print('#cost={:,}'.format(cost))
        print('#total={:,}'.format(total))
        return
    if args['balance']:
        balances = defaultdict(float)
        for i, doc in enumerate(collection.find({'time': {'$gt': start_after, '$lt': stop}}).sort('time', 1), 1):
            try:
                doc['time'] = t = UTC.localize(doc['time'])
                kind = doc['kind']
                if kind in ('margin_deposit', 'margin_withdrawal') or (
                        'deposit' not in kind and 'withdrawal' not in kind):
                    for currency, qty in doc['incomes'] + doc['outcomes']:
                        balances[currency] += qty
                        if currency == 'XRP':
                            pprint(doc)
                for currency, qty in doc['fees']:
                    balances[currency] += qty
                    if currency == 'XRP':
                        pprint(doc)

            except Exception:
                logging.error(pformat(doc))
                raise
        pprint(balances)
        return
    if args['jpy']:
        balances = defaultdict(float)
        total = .0
        for i, doc in enumerate(collection.find({'time': {'$gt': start_after, '$lt': stop}}).sort('time', 1), 1):
            try:
                doc['time'] = t = UTC.localize(doc['time'])
                kind = doc['kind']
                if kind in ('jpy_deposit', 'jpy_withdrawal'):
                    for currency, qty in doc['incomes'] + doc['outcomes'] + doc['fees']:
                        balances[kind] += qty
                        total += qty
            except Exception:
                logging.error(pformat(doc))
                raise
        pprint(balances)
        return
    if args['simple']:
        calculator = Calculator()
        calculator.reset()
        daily_pnl = .0
        hourly_pnl = .0
        pnl = .0
        daily_stop = None
        hourly_stop = None
        json_data = []
        for i, doc in enumerate(collection.find({'time': {'$gt': start_after, '$lt': stop}}).sort('time', 1), 1):
            doc['time'] = t = UTC.localize(doc['time'])
            for currency, qty in doc['incomes']:
                pnl += calculator.get_rate(currency, t) * qty
            for currency, qty in doc['outcomes']:
                pnl += calculator.get_rate(currency, t) * qty
            for currency, qty in doc['fees']:
                pnl += calculator.get_rate(currency, t) * qty
        print('#pnl={}'.format(pnl))
        return
    if args['ma_old'] or args['ma']:
        calculator = Calculator()
        calculator.reset()
        if args['--balance']:
            with open(args['--balance']) as f:
                balances = json.load(f)
                calculator.load_balances(balances)
        daily_pnl = .0
        hourly_pnl = .0
        pnl = .0
        daily_stop = None
        hourly_stop = None
        json_data = []
        for i, doc in enumerate(collection.find({'time': {'$gt': start_after, '$lt': stop}}).sort('time', 1), 1):
            try:
                doc['time'] = t = UTC.localize(doc['time'])
                if daily_stop is None:
                    daily_stop = t.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
                if hourly_stop is None:
                    hourly_stop = t.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
                while hourly_stop <= t:
                    print('# {} hourly_pnl={:,.3f} delta={:,.3f}'.format(
                        hourly_stop, pnl, pnl - hourly_pnl))
                    pprint(calculator.get_current_value(hourly_stop - timedelta(hours=1)))
                    hourly_pnl = pnl
                    hourly_stop += timedelta(hours=1)
                while daily_stop <= t:
                    print('# {} daily_pnl={:,.3f} delta={:,.3f}'.format(
                        daily_stop, pnl, pnl - daily_pnl))
                    pprint(calculator.get_current_value(daily_stop - timedelta(days=1)))
                    daily_pnl = pnl
                    daily_stop += timedelta(days=1)
                if args['ma_old']:
                    result = calculator.calculate_ma_old(doc)
                elif args['ma']:
                    result = calculator.calculate_ma(doc)
                else:
                    assert False
                if not result:
                    continue
                pnl = result['pnl']
                print('#{}'.format(i))
                print('# pnl={:,.3f}  delta={:,.3f}'.format(pnl, result['pnl']))
                pprint(result)
                json_data.append(copy.deepcopy(result))
            except Exception:
                logging.error(pformat(doc))
                raise
        balances = copy.deepcopy(calculator.balances)
        for currency, debt in calculator.debt_balances.items():
            balances[currency]['qty'] += debt['qty']
        print('#balances')
        pprint(balances)
        json_data = {
            'result': balances,
            'pnl': json_data[-1]['pnl'],
            'history': json_data,
        }
        with open(json_file, 'w') as f:
            json.dump(json_data, f, sort_keys=True, indent=4, default=support_datetime_default)
        with open('ma_result.json', 'w') as f:
            json.dump(balances, f, sort_keys=True, indent=4, default=support_datetime_default)
        return
    if args['ma2']:
        calculator = Calculator()
        calculator.reset()
        if args['--balance']:
            with open(args['--balance']) as f:
                balances = json.load(f)
                calculator.load_balances(balances)
        daily_pnl = .0
        hourly_pnl = .0
        pnl = .0
        daily_stop = None
        hourly_stop = None
        json_data = []
        for i, doc in enumerate(collection.find({'time': {'$gt': start_after, '$lt': stop}}).sort('time', 1), 1):
            try:
                doc['time'] = t = UTC.localize(doc['time'])
                if daily_stop is None:
                    daily_stop = t.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
                if hourly_stop is None:
                    hourly_stop = t.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
                while hourly_stop <= t:
                    print('# {} hourly_pnl={:,.3f} delta={:,.3f}'.format(
                        hourly_stop, pnl, pnl - hourly_pnl))
                    pprint(calculator.get_current_value(hourly_stop - timedelta(hours=1)))
                    hourly_pnl = pnl
                    hourly_stop += timedelta(hours=1)
                while daily_stop <= t:
                    print('# {} daily_pnl={:,.3f} delta={:,.3f}'.format(
                        daily_stop, pnl, pnl - daily_pnl))
                    pprint(calculator.get_current_value(daily_stop - timedelta(days=1)))
                    daily_pnl = pnl
                    daily_stop += timedelta(days=1)
                result = calculator.calculate_ma2(doc)
                if not result:
                    continue
                pnl = result['pnl']
                print('#{}'.format(i))
                print('# pnl={:,.3f}  delta={:,.3f}'.format(pnl, result['pnl']))
                pprint(result)
                json_data.append(copy.deepcopy(result))
            except Exception:
                logging.error(pformat(doc))
                raise
        balances = copy.deepcopy(calculator.balances)
        for currency, debt in calculator.debt_balances.items():
            balances[currency]['qty'] += debt['qty']
        print('#balances')
        pprint(balances)
        json_data = {
            'result': balances,
            'pnl': json_data[-1]['pnl'],
            'history': json_data,
        }
        with open(json_file, 'w') as f:
            json.dump(json_data, f, sort_keys=True, indent=4, default=support_datetime_default)
        with open('ma2_result.json', 'w') as f:
            json.dump(balances, f, sort_keys=True, indent=4, default=support_datetime_default)
        return
    if args['ga']:
        calculator = Calculator()
        calculator.reset()
        if args['--balance']:
            with open(args['--balance']) as f:
                balances = json.load(f)
                calculator.load_balances(balances)
        for i, doc in enumerate(collection.find({'time': {'$gt': start_after, '$lt': stop}}).sort('time', 1), 1):
            try:
                doc['time'] = t = UTC.localize(doc['time'])
                result = calculator.calculate_ga_prepare(doc)
                print('#{}'.format(i))
                pprint(result)
            except Exception:
                logging.error(pformat(doc))
                raise
        json_data = []
        calculator.pnl = .0
        for i, doc in enumerate(collection.find({'time': {'$gt': start_after, '$lt': stop}}).sort('time', 1), 1):
            try:
                doc['time'] = t = UTC.localize(doc['time'])
                result = calculator.calculate_ga(doc)
                print('#{}'.format(i))
                if not result:
                    continue
                pnl = result['pnl']
                print('#{}'.format(i))
                print('# pnl={:,.3f}  delta={:,.3f}'.format(pnl, result['pnl']))
                json_data.append(result)
            except Exception:
                logging.error(pformat(doc))
                raise
        print('#ga_cost')
        pprint(calculator.ga_costs)
        balances = copy.deepcopy(calculator.ga_costs)
        for v in balances.values():
            v['qty'] = v['remain']
            del v['remain']
            v['jpy'] = v['qty'] * v['price']
        with open(json_file, 'w') as f:
            json.dump(json_data, f, sort_keys=True, indent=4, default=support_datetime_default)
        with open('ga_result.json', 'w') as f:
            json.dump(balances, f, sort_keys=True, indent=4, default=support_datetime_default)
        return


if __name__ == '__main__':
    main()
