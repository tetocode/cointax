import copy
import csv
import json
import logging
import pathlib
import re
import sys
from collections import defaultdict, OrderedDict
from pprint import pprint
from typing import List

import dateutil.parser
from docopt import docopt


def main():
    args = docopt("""
    Usage:
        {f} [options] FILE...
    
    Options:
        --delimiter CHAR  [default: |]

    """.format(f=pathlib.Path(sys.argv[0]).name))
    pprint(args)

    dict_list = []
    for path in args['FILE']:
        path = pathlib.Path(path).absolute()
        print('#input', path)
        dict_list.extend(csv_to_dict(str(path), args['--delimiter']))
        path_out = path.parent / (str(path.stem) + '.json')
        print('#output', path_out)
        with path_out.open('w') as file:
            json.dump(dict_list, file, sort_keys=True, ensure_ascii=False, indent=4)


def csv_to_dict(path, delimiter):
    kinds = [
        'fiat_balance', 'spot', 'crypto_balance', 'margin', 'position',
    ]
    tmp_kinds = kinds.copy()
    fieldnames_map = {}
    fields_map = defaultdict(list)
    kind = ''
    with open(path, 'r') as file:
        for row in csv.reader(file, delimiter=delimiter):
            if not row:
                continue
            if len(row) == 1 and ('＜' in row[0] or '【' in row[0]):
                continue
            if row[0] == 'アカウント':
                kind = tmp_kinds.pop(0)
                fieldnames_map[kind] = row
                continue
            if not kind:
                continue
            fields_map[kind].append(row)
    assert not tmp_kinds, tmp_kinds
    rows = []
    for kind in kinds:
        rows.extend(fix_rows(kind, fieldnames_map[kind], fields_map[kind]))
    return rows


def fix_rows(kind: str, fieldnames: List[str], rows: List[list]):
    data_stack = []
    stack = []
    currencies = ['BTC', 'ETH', 'BCH',
                  'AUD', 'CNY', 'EUR', 'JPY', 'HKD', 'IDR', 'INR', 'PHP', 'SGD', 'USD']
    assert '日時' in fieldnames[1], (kind, fieldnames)
    for row in reversed(rows):
        stack.append(row)
        if len(row) >= 2:
            if row[0] in currencies:
                dt = '{} {}'.format(row[1], row[2])
            else:
                dt = '{} {}'.format(row[0], row[1])
            if re.search('\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}', dt):
                data_stack.append(stack)
                stack = []
    currency = None
    for stack in reversed(data_stack):
        stack.reverse()
        if stack[0][0] in currencies:
            currency = stack[0][0]
        else:
            assert currency, stack
            stack[0].insert(0, currency)
        stack2 = []
        single_digits = None
        for data in reversed(stack):
            if single_digits:
                data[-1] += single_digits[0]
                stack2.append(data)
                single_digits = None
            else:
                if len(data) == 1 and data[0].isdigit():
                    single_digits = data
                else:
                    stack2.append(data)
        stack.clear()
        for x in reversed(stack2):
            stack.extend(x)

    def get_remove_keys(v_list):
        if False:
            pass
        elif kind == 'fiat_balance':
            if ('売' in v_list) or ('買' in v_list):
                return ['建玉番号']
            elif ('新規' in v_list) or ('決済' in v_list):
                return ['約定番号']
            elif 'ロールオーバー' in v_list:
                return ['約定番号']
            elif '出金' in v_list:
                return ['注文番号', '建玉番号', '約定番号']
        elif kind == 'spot':
            if ('売' in v_list) or ('買' in v_list):
                return
        elif kind == 'crypto_balance':
            if ('入金' in v_list) or ('出金' in v_list):
                return ['注文番号', '建玉番号', '約定番号', '仮想通貨名']
            elif ('売' in v_list) or ('買' in v_list):
                return ['建玉番号']
            elif ('交換(売)' in v_list) or ('交換(買)' in v_list):
                return ['建玉番号']
        elif kind == 'margin':
            if ('新規' in v_list) or ('決済' in v_list):
                return
        elif kind == 'position':
            if ('売' in v_list) or ('買' in v_list):
                return
        assert False

    results = []
    fieldnames = copy.deepcopy(fieldnames)
    for values in reversed(data_stack):
        keys = copy.deepcopy(fieldnames)
        values = values[:1] + ['{} {}'.format(*values[1:3])] + values[3:]
        try:
            for k in (get_remove_keys(values) or []):
                keys.remove(k)
            dateutil.parser.parse(values[1])
        except Exception:
            logging.error('#{}'.format(kind))
            logging.error(keys)
            logging.error(values)
            logging.error(OrderedDict(zip(keys, values)))
            raise
        results.append(dict(zip(keys, values),
                            kind=kind, currency=values[0], time=values[1]))
    return results


if __name__ == '__main__':
    main()
