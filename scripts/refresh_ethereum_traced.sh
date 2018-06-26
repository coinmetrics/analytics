#!/bin/bash

PSQL="psql -h 127.0.0.1 -U postgres"

# update tables / materialized views
time $PSQL -f $(dirname "$0")/ethereum_tx.sql
time $PSQL -f $(dirname "$0")/ethereum_actions.sql
time $PSQL -f $(dirname "$0")/ethereum_short_addrs.sql
time $PSQL -f $(dirname "$0")/ethereum_stats_traced.sql

### export csv's

# ethereum
$PSQL -qc "\\pset footer off" -c 'SELECT SUBSTRING("date"::TEXT FOR 10) "date", "tx_cnt" "txCount", "sum_value" "txVolume", "med_value" "medTxVolume", "avg_difficulty" "avgDifficulty", "avg_tx_size" "avgTxSize", "sum_fee" "sumFee", "block_cnt" "blockCount", "sum_size" "totalSize", "med_fee" "medFee", "reward" "generatedVolume", "from_cnt" "fromAddrCount", "to_cnt" "toAddrCount", "addr_cnt" "addrCount", "payment_cnt" "paymentCount", "payment_value" "paymentVolume", "contract_cnt" "contractTransferCount", "contract_value" "contractTransferVolume", "create_contract_cnt" "createContractCount", "create_contract_value" "createContractVolume", "action_cnt" "actionCount", "sum_action_value" "actionVolume", "short_cnt" "shortTxCount", "short_value" "shortTxValue" FROM ethereum_stats_traced ORDER BY "date"' -A -F ',' -o 'eth_stats.csv'

# ERC20 tokens
time $PSQL -qc 'REFRESH MATERIALIZED VIEW erc20transfers'

for SYMBOL in $($PSQL -qtc 'SELECT symbol FROM erc20tokens')
do
	$PSQL -qc "\\pset footer off" -c 'SELECT SUBSTRING("date"::TEXT FOR 10) "date", "cnt" "txCount", "value" "txVolume", "max_value" "maxOneTxVolume", "max_sum_from_value" "maxAddrSumFromValue", "max_sum_to_value" "maxAddrSumToValue", "from_cnt" "fromAddrCount", "to_cnt" "toAddrCount", "addr_cnt" "addrCount", "med_value" "medTxVolume" FROM erc20transfers WHERE "symbol" = '\'$SYMBOL\'' ORDER BY "date"' -A -F ',' -o "$SYMBOL.csv"
done
