import base64
import hashlib
import json
import logging
import pathlib
import time
from abc import ABC, abstractmethod
from collections import OrderedDict
from datetime import datetime
from pprint import pformat
from typing import Dict, List, Union, Sequence, Generator

import dateutil.parser
import pytz
import yaml

from coindb import Database, DBCollection
from coindb.bulkop import BulkOp


class ClientBase(ABC):
    NAME = ''
    TIMEOUT = 60.0
    RATE_LIMIT_INTERVAL = 30.0
    RATE_LIMIT_TIMEOUT = 300.0
    USER_AGENT = 'coinapi 0.0.1'
    FIAT_CURRENCIES = ()
    CRYPTO_CURRENCIES = ()
    UTC = pytz.UTC
    JST = pytz.timezone('Asia/Tokyo')
    NYT = pytz.timezone('America/New_York')
    COLLECTIONS = ()

    def __init__(self, api_key: str = None, api_secret: str = None, timeout: float = None, **__):
        assert self.NAME

        if not api_key or not api_secret:
            api_config = self.api_config
            if api_config:
                api_config = api_config[0]
                api_key, api_secret = api_config['api_key'], api_config['api_secret']

        api_digest = None
        if api_key:
            api_digest = base64.b64encode(hashlib.sha1(api_key.encode()).digest()).decode()
        self.logger = logging.getLogger('{}.{}'.format(self.NAME, api_digest))

        self.api_key = api_key
        self.api_secret = api_secret
        self.timeout = timeout or self.TIMEOUT

        self._currencies = {}  # type:Dict[str, dict]
        self._rcurrencies = {}  # type:Dict[str, dict]
        self._instruments = {}  # type: Dict[str, dict]
        self._rinstruments = {}  # type: Dict[str, dict]

    def debug(self, *args, **kwargs):
        return self.logger.debug(*args, **kwargs)

    def info(self, *args, **kwargs):
        return self.logger.info(*args, **kwargs)

    def warning(self, *args, **kwargs):
        return self.logger.warning(*args, **kwargs)

    def error(self, *args, **kwargs):
        return self.logger.error(*args, **kwargs)

    def critical(self, *args, **kwargs):
        return self.logger.critical(*args, **kwargs)

    def exception(self, *args, **kwargs):
        return self.logger.exception(*args, **kwargs)

    @classmethod
    def make_nonce(cls):
        return int(time.time() * 1000)

    def get_currencies(self) -> Dict[str, dict]:
        currencies = {}
        for x in self.instruments.values():
            if 'base' in x:
                currencies[x['base']] = dict(name=x['base'], id=x['base'])
            if 'quote' in x:
                currencies[x['quote']] = dict(name=x['quote'], id=x['quote'])
        currencies = OrderedDict((k, currencies[k]) for k in sorted(currencies))
        return currencies

    @property
    def currencies(self):
        if not self._currencies:
            self._currencies = self.get_currencies()
        return self._currencies

    @currencies.setter
    def currencies(self, value):
        self._currencies = value

    @property
    def rcurrencies(self) -> Dict[str, str]:
        if not self._rcurrencies:
            rcurrencies = {v['id']: k for k, v in self.currencies.items()}
            self._rcurrencies = OrderedDict((k, rcurrencies[k]) for k in sorted(rcurrencies))
        return self._rcurrencies

    @rcurrencies.setter
    def rcurrencies(self, value):
        self._rcurrencies = value

    @abstractmethod
    def get_instruments(self) -> Dict[str, dict]:
        pass

    @property
    def instruments(self):
        if not self._instruments:
            self._instruments = self.get_instruments()
        return self._instruments

    @instruments.setter
    def instruments(self, value):
        self._instruments = value

    @property
    def rinstruments(self) -> Dict[str, str]:
        if not self._rinstruments:
            rinstruments = {v['id']: k for k, v in self.instruments.items()}
            self._rinstruments = OrderedDict((k, rinstruments[k]) for k in sorted(rinstruments))
        return self._rinstruments

    @rinstruments.setter
    def rinstruments(self, value):
        self._rinstruments = value

    @abstractmethod
    def tick(self, instrument:str):
        pass

    @abstractmethod
    def balance(self):
        pass

    @property
    def api_config(self) -> List[dict]:
        path = pathlib.Path.home() / '.crypto.yaml'
        with path.open('r') as f:
            config = yaml.load(f)
            if self.NAME in config:
                return config[self.NAME]

    @property
    def crypto_path(self) -> pathlib.Path:
        path = pathlib.Path.home() / 'cryptofiles' / self.NAME
        assert path.exists(), path
        return path

    @classmethod
    def utc_now(cls) -> datetime:
        return cls.UTC.localize(datetime.utcnow())

    @classmethod
    def utc_from_timestamp(cls, timestamp: float) -> datetime:
        assert isinstance(timestamp, float), (type(timestamp), timestamp)
        return cls.UTC.localize(datetime.utcfromtimestamp(timestamp))

    @classmethod
    def parse_time(cls, time_obj: Union[str, datetime], tz: pytz.timezone = None) -> datetime:
        assert isinstance(time_obj, (str, datetime)), (type(time_obj), time_obj)
        if isinstance(time_obj, str):
            time_obj = dateutil.parser.parse(time_obj)
        if not time_obj.tzinfo:
            assert tz, '{} is naive. tz must set'.format(time_obj)
            time_obj = tz.localize(time_obj)
        else:
            time_obj = time_obj.astimezone(tz)
        return time_obj

    @classmethod
    def json_hash(cls, data: dict) -> str:
        return hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()

    def bulk_op(self, collection: DBCollection):
        return BulkOp(collection, self.logger)

    @property
    @abstractmethod
    def import_data_methods(self) -> Dict[str, Sequence[Sequence]]:
        pass

    def import_data_all(self, db: Database, start: str, stop: str, *, drop: bool = False):
        _, _ = start, stop
        for name, methods in self.import_data_methods.items():
            assert name in self.COLLECTIONS, '{} not in {}'.format(name, self.COLLECTIONS)
            collection = db[name]
            if drop:
                collection.drop()
            collection.create_index([('time', 1), ('id', 1)], unique=True)
            collection.create_index([('id', 1)])
            for method, args in methods:
                self.info('{}{}'.format(method.__name__, tuple(args)))
                with self.bulk_op(collection) as bulk_op:
                    try:
                        for data in method(*args):
                            bulk_op.insert(data)
                    except Exception as e:
                        self.exception(str(e))

    @abstractmethod
    def convert_data(self, name: str) -> Generator[dict, dict, None]:
        pass

    def convert_data_all(self, db: Database, start: str, stop: str, *, drop: bool = False):
        _, _ = start, stop
        assert self.COLLECTIONS
        out_collection = db['converted']
        if drop:
            out_collection.drop()
        out_collection.create_index([('time', 1), ('kind', 1), ('id', 1)], unique=True)
        out_collection.create_index([('id', 1)])
        with self.bulk_op(out_collection) as bulk_op:
            for name in self.COLLECTIONS:
                self.info('convert_data_all {}'.format(name))
                try:
                    collection = db[name]
                    converter = self.convert_data(name)
                    _ = next(converter)
                    assert not _, _
                    for data in collection.find({}, {'_id': 0}).sort([('time', 1), ('id', 1)]):
                        assert '_id' not in data
                        try:
                            one_or_list = converter.send(data)
                            if one_or_list:
                                if not isinstance(one_or_list, Sequence):
                                    list_obj = [one_or_list]
                                else:
                                    list_obj = one_or_list
                                converted_list = []
                                for x in list_obj:
                                    assert 'kind' in x
                                    assert 'pnl' in x
                                    if x['kind'] == 'spot':
                                        assert 'instrument' in x
                                        assert 'side' in x
                                        assert x['side'] in ('BUY', 'SELL')
                                    copied = data.copy()
                                    copied.update(x)
                                    converted_list.append(copied)
                                bulk_op.insert(converted_list)
                        except Exception:
                            self.error('invalid data {}'.format(pformat(data)))
                            raise
                except Exception as e:
                    self.exception(str(e))

    def public_executions(self, instrument: str, **params) -> Generator[dict, None, None]:
        raise StopIteration

    def public_executions_asc(self, instrument: str,
                              start: datetime, stop: datetime, **params) -> Generator[dict, None, None]:
        raise StopIteration

    def public_executions_desc(self, instrument: str,
                               start: datetime, stop: datetime, **params) -> Generator[dict, None, None]:
        for data in self.public_executions(instrument, **params):
            dt = data['time']
            if start <= dt < stop:
                yield data
            if dt < start:
                return
