#!/bin/sh

set -e

CRYPTO_DIR=$HOME/cryptofiles/minbtc

echo "#clean $CRYPTO_DIR"
rm -f $CRYPTO_DIR/*.csv
rm -f $CRYPTO_DIR/*.json

# minbtc
for PDF in `find $CRYPTO_DIR/*DAILY*.pdf`; do
  CSV=${PDF%.pdf}.csv
  echo "#$PDF -> $CSV"
  pdftotext -raw -layout $PDF - | sed -E -e 's/^\s+//' -e 's/\s+/|/g' -e 's/【\|/【/' -e 's/\|】/】/' > $CSV
  python3 ./minbtc_fix_csv_to_json.py $CSV
done
