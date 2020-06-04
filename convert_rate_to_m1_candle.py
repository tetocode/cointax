import logging
import pathlib
import sys
from collections import OrderedDict
from datetime import datetime
from pprint import pprint
from typing import Dict

import pymongo
from dateutil.relativedelta import relativedelta
from docopt import docopt

from coinapi.clientbase import ClientBase
from coindb.bulkop import BulkOp

UTC = ClientBase.UTC
JST = ClientBase.JST
utc_now = ClientBase.utc_now
parse_time = ClientBase.parse_time


def main():
    logging.basicConfig(level=logging.INFO)
    args = docopt("""
    Usage:
        {f} [options] EXCHANGE INSTRUMENT
    
    Options:
        --start START  [default: {start}]
        --stop STOP  [default: {now}]

    """.format(f=pathlib.Path(sys.argv[0]).name,
               start=JST.localize(datetime(2016, 12, 29)),
               now=utc_now().astimezone(JST)))
    pprint(args)
    db_client = pymongo.MongoClient()
    start = parse_time(args['--start'])
    stop = parse_time(args['--stop'])

    exchange = args['EXCHANGE']
    instrument = args['INSTRUMENT']
    db = db_client[exchange]
    collection_in = db[instrument]
    collection_out = db['{}_M1'.format(instrument)]
    collection_out.drop()
    collection_out.create_index([('time', 1)], unique=True)
    candles = OrderedDict()  # type: Dict[datetime, Candle]
    with BulkOp(collection_out) as bulk_op:
        start_1 = start - relativedelta(days=1)
        for doc in collection_in.find({'time': {'$gt': start_1, '$lt': stop}}, {'_id': 0}).sort('time', 1):
            dt = UTC.localize(doc['time']).astimezone(JST).replace(second=0, microsecond=0)
            if dt not in candles:
                candles[dt] = Candle(dt)
                if len(candles) >= 3:
                    candle = candles.pop(list(candles.keys())[1])
                    bulk_op.insert(candle.as_dict())
            candle = candles[dt]
            candle.update(doc['price'], abs(doc['qty']))


class Candle:
    def __init__(self, dt: datetime):
        self.dt = dt
        self.open = None  # type: float
        self.high = -float('Inf')
        self.low = float('Inf')
        self.close = None  # type: float
        self.volume = 0.
        self.price_volume = 0.

    def update(self, price: float, volume: float):
        if self.open is None:
            self.open = price
        self.high = max([self.high, price])
        self.low = min([self.low, price])
        self.close = price
        self.volume += volume
        self.price_volume += price * volume

    def as_dict(self) -> dict:
        return dict(time=self.dt, o=self.open, h=self.high, l=self.low, c=self.close,
                    v=self.volume, vwap=self.price_volume / self.volume)


if __name__ == '__main__':
    main()
