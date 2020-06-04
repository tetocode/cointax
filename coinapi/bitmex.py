from collections import defaultdict
from datetime import timedelta
from typing import Generator

import ccxt

from .ccxtclient import CCXTClient
from .ratelimiter import RateLimiter


class Client(CCXTClient):
    NAME = 'bitmex'
    CCXT_CLASS = ccxt.bitmex
    LIMIT = 500
    COLLECTIONS = ('wallet_history',)

    def get_page_items(self, fn, parse, rps_limit: float, **params):
        """
        count:100
        start:0
        reverse:true
        endTime: datetime
        """
        fn = RateLimiter(rps_limit, fn)
        limit = self.LIMIT
        start = 0
        end_time = (self.utc_now() - timedelta(seconds=1)).isoformat()
        while True:
            params.update(count=limit, start=start, endTime=end_time, reverse=True)
            res = fn(params)
            for x in res:
                x = parse(x)
                yield x
            if len(res) < limit:
                break
            start += limit

    def executions_all(self) -> Generator[dict, None, None]:
        def parse(data: dict):
            """
            {
                "execID": "string",
                "orderID": "string",
                "clOrdID": "string",
                "clOrdLinkID": "string",
                "account": 0,
                "symbol": "string",
                "side": "string",
                "lastQty": 0,
                "lastPx": 0,
                "underlyingLastPx": 0,
                "lastMkt": "string",
                "lastLiquidityInd": "string",
                "simpleOrderQty": 0,
                "orderQty": 0,
                "price": 0,
                "displayQty": 0,
                "stopPx": 0,
                "pegOffsetValue": 0,
                "pegPriceType": "string",
                "currency": "string",
                "settlCurrency": "string",
                "execType": "string",
                "ordType": "string",
                "timeInForce": "string",
                "execInst": "string",
                "contingencyType": "string",
                "exDestination": "string",
                "ordStatus": "string",
                "triggered": "string",
                "workingIndicator": true,
                "ordRejReason": "string",
                "simpleLeavesQty": 0,
                "leavesQty": 0,
                "simpleCumQty": 0,
                "cumQty": 0,
                "avgPx": 0,
                "commission": 0,
                "tradePublishIndicator": "string",
                "multiLegReportingType": "string",
                "text": "string",
                "trdMatchID": "string",
                "execCost": 0,
                "execComm": 0,
                "homeNotional": 0,
                "foreignNotional": 0,
                "transactTime": "2018-01-20T07:23:07.412Z",
                "timestamp": "2018-01-20T07:23:07.412Z"
            }
            """
            return dict(time=self.parse_time(data['timestamp']),
                        id=data['execID'],
                        data=data)

        yield from self.get_page_items(self.privateGetExecutionTradehistory, parse, 150 / 300)

    def transfers_all(self) -> Generator[dict, None, None]:
        def parse(data: dict):
            """
            {
                "transactID": "string",
                "account": 0,
                "currency": "string",
                "transactType": "string",
                "amount": 0,
                "fee": 0,
                "transactStatus": "string",
                "address": "string",
                "tx": "string",
                "text": "string",
                "transactTime": "2018-01-20T07:23:09.119Z",
                "timestamp": "2018-01-20T07:23:09.119Z"
            }
            """
            return dict(time=self.parse_time(data['timestamp']),
                        id=data['transactID'],
                        data=data)

        def filter_items(it):
            return filter(lambda x: x['data']['transactType'].lower() in ('deposit', 'withdrawal'), it)

        yield from filter_items(self.get_page_items(self.privateGetUserWallethistory, parse, 150 / 300))

    def wallet_history_all(self) -> Generator[dict, None, None]:
        def parse(data: dict):
            """
            {
                "transactID": "string",
                "account": 0,
                "currency": "string",
                "transactType": "string",
                "amount": 0,
                "fee": 0,
                "transactStatus": "string",
                "address": "string",
                "tx": "string",
                "text": "string",
                "transactTime": "2018-01-20T07:23:09.119Z",
                "timestamp": "2018-01-20T07:23:09.119Z"
            }
            """
            return dict(time=self.parse_time(data['timestamp']),
                        id=data['transactID'],
                        data=data)

        yield from self.get_page_items(self.privateGetUserWallethistory, parse, 150 / 300)

    @property
    def import_data_methods(self):
        name_methods = defaultdict(list)
        # name_methods['transfer'].append((self.transfers_all, ()))
        name_methods['wallet_history'].append((self.wallet_history_all, ()))
        return name_methods

    def convert_data(self, name: str):
        def convert_one(doc: dict):
            data = doc['data']
            transaction_type = data['transactType'].lower()
            assert data['currency'].upper() == 'XBT'
            currency = 'BTC'
            qty = float(data['amount']) / 1e8

            if transaction_type == 'deposit':
                assert qty > 0
                return dict(kind='deposit',
                            pnl=(currency, qty, ''))
            elif transaction_type == 'withdrawal':
                if data['transactStatus'].lower() == 'canceled':
                    return
                fee_qty = -float(data['fee']) / 1e8
                qty -= fee_qty
                assert qty < 0
                assert fee_qty < 0
                return dict(kind='withdrawal',
                            pnl=[
                                (currency, qty, ''),
                                (currency, fee_qty, 'fee'),
                            ])
            elif transaction_type == 'realisedpnl':
                assert data['fee'] == 0
                return dict(kind='margin',
                            pnl=(currency, qty, ''))
            assert False

        result = {}
        while True:
            _doc = yield result
            result = convert_one(_doc)
