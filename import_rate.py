import logging
import pathlib
import re
import sys
from datetime import datetime
from pprint import pprint

import lxml.html
import pymongo
import requests
from dateutil.relativedelta import relativedelta
from docopt import docopt

import coinapi
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
        {f} [options] EXCHANGE INSTRUMENT [PARAM...]
    
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
    instrument = args['INSTRUMENT']
    if exchange == 'yahoo':
        client = 'yahoo'
    else:
        client = getattr(coinapi, exchange).Client()  # type: ClientBase
    db = db or exchange
    db = db_client[db]
    collection = db[instrument]
    params = dict(map(lambda s: s.split('='), args['PARAM']))
    with BulkOp(collection) as bulk_op:
        collection.create_index([
            ('time', 1), ('id', 1)
        ], unique=True)

        if client == 'yahoo':
            gen = import_yahoo(instrument, start, stop)
        else:
            try:
                gen = client.public_executions_asc(instrument, start, stop, **params)
            except StopIteration:
                gen = client.public_executions_desc(instrument, start, stop, **params)

        for data in gen:
            bulk_op.insert(data)


def import_yahoo(instrument: str, start: datetime, stop: datetime):
    """
    https://info.finance.yahoo.co.jp/history/?code=USDJPY%3DX&sy=2017&sm=11&sd=18&ey=2018&em=2&ed=16&tm=d&p=1
    https://info.finance.yahoo.co.jp/history/?code=USDJPY%3DX&sy=2017&sm=11&sd=18&ey=2018&em=2&ed=16&tm=d&p=3
    """
    start = start.astimezone(JST)
    end = stop.astimezone(JST) - relativedelta(days=1)
    symbol = instrument.replace('/', '')
    assert len(symbol) == 6, instrument
    url_format = 'https://info.finance.yahoo.co.jp/history/?code={symbol}%3DX&sy={sy}&sm={sm}&sd={sd}&ey={ey}&em={em}&ed={ed}&tm=d&p={p}'
    with requests.Session() as s:
        params = dict(symbol=symbol,
                      sy=start.year, sm=start.month, sd=start.day,
                      ey=end.year, em=end.month, ed=end.day,
                      p=1)
        while True:
            res = s.get(url_format.format(**params))
            m = re.search('\d+～(\d+)件/(\d+)件中', res.text, re.MULTILINE)
            if not m:
                raise Exception('#no match')

            root = lxml.html.fromstring(res.text)
            td_list = []
            for tr in root.cssselect('tr'):
                for td in tr.cssselect('td'):
                    td_list.append(td.text_content())
            text = ' '.join(td_list)
            text = re.sub('\s+', ' ', text)
            for m in re.finditer(
                    '(?P<y>\d+)年(?P<m>\d+)月(?P<d>\d+)日 (?P<o>[.\d]+) (?P<h>[.\d]+) (?P<l>[.\d]+) (?P<c>[.\d]+)',
                    text, re.MULTILINE):
                """
                yield dict(time=t,
                           id='#{}'.format(i),
                           price=float(data['price']),
                           qty=float(data['amount']),
                           data=data)
                """
                d = m.groupdict()
                print(d)
                yield dict(time=JST.localize(datetime(int(d['y']), int(d['m']), int(d['d']))),
                           id='{:04d}{:02d}{:02d}'.format(int(d['y']), int(d['m']), int(d['d'])),
                           o=float(d['o']), h=float(d['h']), l=float(d['l']), c=float(d['c']))

            if m.group(1) == m.group(2):
                print('#end')
                return

            params['p'] += 1


if __name__ == '__main__':
    main()
