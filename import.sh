set -xe

#export https_proxy=haproxy:8080
EXCHANGES="
bitfinex
bitflyer
bitmex
coincheck
kraken
minbtc
quoinex
zaif
xmr
"

for EXCHANGE in $EXCHANGES; do
    if [ "$1" = "all" ] || [ "$1" = $EXCHANGE ]; then
        echo "# execute $EXCHANGE"
        sleep 1
        python3 ./import_data.py $EXCHANGE
    else
        echo "# skip $EXCHANGE"
        sleep 1
    fi
done
wait
