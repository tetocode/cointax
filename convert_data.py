import logging
import pathlib
import sys
from collections import defaultdict
from datetime import datetime
from pprint import pprint

import pymongo
from docopt import docopt

import coinapi
from coinapi.clientbase import ClientBase
from coindb import Database
from coindb.bulkop import BulkOp

UTC = ClientBase.UTC
JST = ClientBase.JST
utc_now = ClientBase.utc_now
parse_time = ClientBase.parse_time


def main():
    logging.basicConfig(level=logging.INFO)
    args = docopt("""
    Usage:
        {f} [options] EXCHANGE
    
    Options:
        --db DB
        --start START  [default: {start}]
        --stop STOP  [default: {now}]

    """.format(f=pathlib.Path(sys.argv[0]).name,
               start=JST.localize(datetime(2016, 12, 29)),
               now=utc_now().astimezone(JST)))
    pprint(args)
    db_client = pymongo.MongoClient()
    db = args['--db']
    start = parse_time(args['--start'])
    stop = parse_time(args['--stop'])

    exchange = args['EXCHANGE']
    db = db or exchange
    db = db_client[db]
    client = getattr(coinapi, exchange).Client()  # type: ClientBase
    client.convert_data_all(db, start, stop, drop=True)
    adjust_data(db, exchange)


def adjust_data(db: Database, exchange: str):
    pass


if __name__ == '__main__':
    main()
