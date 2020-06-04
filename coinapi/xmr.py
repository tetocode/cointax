from collections import defaultdict
from typing import TextIO

from .clientbase import ClientBase


class Client(ClientBase):
    NAME = 'xmr'
    COLLECTIONS = ('transfer',)

    def balance(self):
        return {}

    def get_instruments(self):
        return {}

    def load_transfers_txt(self, file: TextIO):
        for i, line in enumerate(file, 1):
            line = line.strip()
            row = list(filter(str, line.split(' ')))  # type: list
            yield dict(time=self.parse_time(row[2], self.UTC),
                       id='#{}'.format(i),
                       data=row)

    def load_transfers_txt_file(self, path: str):
        with open(path, 'r') as file:
            yield from self.load_transfers_txt(file)

    @property
    def import_data_methods(self):
        crypto_path = self.crypto_path
        name_methods = defaultdict(list)
        name_methods['transfer'].append((self.load_transfers_txt_file,
                                         [str(crypto_path / 'transfers.txt')]))

        return name_methods

    def convert_data(self, name: str):
        result = {}
        while True:
            doc = yield result
            _, inout, _, qty, _, _, _ = doc['data']
            if inout == 'in':
                kind = 'mining'
                pnl = ('XMR', float(qty), '')
            else:
                assert inout == 'out'
                kind = 'withdrawal'
                pnl = ('XMR', -float(qty), '')
            result = dict(kind=kind, pnl=pnl)
