import time
from abc import abstractmethod
from collections import OrderedDict
from typing import Dict
from typing import Type

from ccxt import Exchange, DDoSProtection, ExchangeNotAvailable

from .clientbase import ClientBase


class CCXTClient(ClientBase):
    CCXT_CLASS = None  # type: Type[Exchange]

    def __init__(self, api_key: str = None, api_secret: str = None, timeout: float = None, **__):
        assert self.CCXT_CLASS, 'ccxt class not set'
        super().__init__(api_key, api_secret, timeout)

        config = dict(apiKey=self.api_key, secret=self.api_secret)
        self._delegate = self.CCXT_CLASS(config=config)  # type: Exchange
        self._delegate.nonce = self.make_nonce
        self._delegate.timeout = self.timeout * 1000
        self._delegate.userAgent = self.USER_AGENT

    def get_instruments(self) -> Dict[str, dict]:
        markets = self.load_markets()
        instruments = OrderedDict([(k, markets[k]) for k in sorted(markets)])
        return instruments

    def tick(self, instrument: str):
        return getattr(self._delegate, 'fetch_ticker')(instrument)

    def balance(self):
        balance = self.fetch_balance()
        for k in tuple(balance.keys()):
            if k == k.lower():
                del balance[k]
        return balance

    def _handle_error(self, e: Exception):
        raise e

    def __getattr__(self, item):
        fn = getattr(self._delegate, item)

        def retry(*args, **kwargs):
            start = time.time()
            while time.time() - start < self.RATE_LIMIT_TIMEOUT:
                try:
                    return fn(*args, **kwargs)
                except DDoSProtection:
                    self.info('DDoSProtection. sleep {} seconds.'.format(self.RATE_LIMIT_INTERVAL))
                except ExchangeNotAvailable as e:
                    self.warning(str(e))
                except Exception as e:
                    time.sleep(self._handle_error(e) or 0)
                    continue
                time.sleep(self.RATE_LIMIT_INTERVAL)
            raise Exception('retry timeout')

        return retry

    @property
    @abstractmethod
    def import_data_methods(self):
        pass

    @abstractmethod
    def convert_data(self, name: str):
        pass
