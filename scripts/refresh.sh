#!/bin/bash

# additional arguments
PSQL='psql -h 127.0.0.1 -U postgres'

# update materialized views

# cardano
time $PSQL -q -c 'BEGIN' \
	-c 'INSERT INTO analytics_stats ("view", "sync_time") VALUES ('\''cardano'\'', (SELECT TO_TIMESTAMP(MAX(block."timeIssued")) FROM cardano block)) RETURNING *' \
	-c 'REFRESH MATERIALIZED VIEW cardano_stats' \
	-c 'COMMIT'

# ripple_payment_xrp
time $PSQL -q -c 'BEGIN' \
	-c 'INSERT INTO analytics_stats ("view", "sync_time") VALUES ('\''ripple_payment_xrp'\'', (SELECT TO_TIMESTAMP(MAX(ledger."closeTime")) FROM ripple ledger)) RETURNING *' \
	-c 'REFRESH MATERIALIZED VIEW ripple_payment_xrp_stats' \
	-c 'COMMIT'

# stellar_payment
time $PSQL -q -c 'BEGIN' \
	-c 'INSERT INTO analytics_stats ("view", "sync_time") VALUES ('\''stellar_payment'\'', (SELECT TO_TIMESTAMP(MAX(ledger."closeTime")) FROM stellar ledger)) RETURNING *' \
	-c 'REFRESH MATERIALIZED VIEW stellar_payment_stats' \
	-c 'REFRESH MATERIALIZED VIEW stellar_payment_stats_prep' \
	-c 'COMMIT'

# iota
time $PSQL -q -c 'BEGIN' \
	-c 'INSERT INTO analytics_stats ("view", "sync_time") VALUES ('\''iota'\'', (SELECT TO_TIMESTAMP(MAX(CASE WHEN tx."timestamp" >= 1000000000000 THEN tx."timestamp" / 1000 ELSE tx."timestamp" END)) FROM iota tx)) RETURNING *' \
	-c 'REFRESH MATERIALIZED VIEW iota_stats' \
	-c 'COMMIT'

# monero
time $PSQL -q -c 'BEGIN' \
	-c 'INSERT INTO analytics_stats ("view", "sync_time") VALUES ('\''monero'\'', (SELECT TO_TIMESTAMP(MAX(block."timestamp")) FROM monero block)) RETURNING *' \
	-c 'REFRESH MATERIALIZED VIEW monero_stats' \
	-c 'COMMIT'

# nem
time $PSQL -q -c 'BEGIN' \
	-c 'INSERT INTO analytics_stats ("view", "sync_time") VALUES ('\''nem'\'', (SELECT TIMESTAMP '\''2015-03-29 00:06:25'\'' + MAX(block."timeStamp") * INTERVAL '\''1 second'\'' FROM nem block)) RETURNING *' \
	-c 'REFRESH MATERIALIZED VIEW nem_stats' \
	-c 'COMMIT'

# neo
time $PSQL -q -c 'BEGIN' \
	-c 'INSERT INTO analytics_stats ("view", "sync_time") VALUES ('\''neo'\'', (SELECT TO_TIMESTAMP(MAX(block."time")) FROM neo block)) RETURNING *' \
	-c 'REFRESH MATERIALIZED VIEW neo_stats' \
	-c 'COMMIT'

# lisk
time $PSQL -q -c 'BEGIN' \
	-c 'INSERT INTO analytics_stats ("view", "sync_time") VALUES ('\''lisk'\'', (SELECT TIMESTAMP '\''2016-05-24 17:00:00'\'' + MAX(block."timestamp") * INTERVAL '\''1 second'\'' FROM lisk.blocks block)) RETURNING *' \
	-c 'REFRESH MATERIALIZED VIEW lisk.lisk_stats' \
	-c 'COMMIT'

# ethereum_classic_short_tx
time $PSQL -q -c 'BEGIN' \
	-c 'INSERT INTO analytics_stats ("view", "sync_time") VALUES ('\''ethereum_classic_short_tx'\'', (SELECT TO_TIMESTAMP(MAX(block."timestamp")) FROM ethereum_classic block)) RETURNING *' \
	-c 'REFRESH MATERIALIZED VIEW ethereum_classic_short_tx' \
	-c 'COMMIT'

# eos
time $PSQL -q -c 'BEGIN' \
	-c 'INSERT INTO analytics_stats ("view", "sync_time") VALUES ('\''eos'\'', (SELECT TO_TIMESTAMP(MAX(block."timestamp")) FROM eos block)) RETURNING *' \
	-c 'REFRESH MATERIALIZED VIEW eos_stats' \
	-c 'COMMIT'

# waves
time $PSQL -q -c 'BEGIN' \
	-c 'INSERT INTO analytics_stats ("view", "sync_time") VALUES ('\''waves'\'', (SELECT TO_TIMESTAMP(MAX(block."timestamp" * 0.001)) FROM waves block)) RETURNING *' \
	-c 'REFRESH MATERIALIZED VIEW waves_stats' \
	-c 'COMMIT'


### export csv's

# cardano
$PSQL -qc "\\pset footer off" -c 'SELECT SUBSTRING("date"::TEXT FOR 10) "date", "cnt" "txCount", "volume" "txVolume", "fees" "fees", "from_cnt" "fromAddrCount", "to_cnt" "toAddrCount", "addr_cnt" "addrCount", "med_volume" "medTxVolume", "med_fees" "medFees", "payment_cnt" "paymentCount", "short_volume" "shortTxVolume" FROM cardano_stats ORDER BY "date"' -A -F ',' -o 'cardano.csv'

# ripple XRP payments
$PSQL -qc "\\pset footer off" -c 'SELECT SUBSTRING("date"::TEXT FOR 10) "date", "cnt" "txCount", "value" "txVolume", "fee" "fees", "from_cnt" "fromAddrCount", "to_cnt" "toAddrCount", "addr_cnt" "addrCount", "block_cnt" "blockCount", "missing_block_cnt" "missingBlockCount", "total_cnt" "totalTxCount", "missing_cnt" "missingTxCount" FROM ripple_payment_xrp_stats ORDER BY "date"' -A -F ',' -o 'ripple_payment_xrp.csv'

# stellar payments
$PSQL -qc "\\pset footer off" -c 'SELECT * FROM stellar_payment_stats_prep ORDER BY "date"' -A -F ',' -o 'stellar_payment.csv'
# stellar XLM payments
$PSQL -qc "\\pset footer off" -c 'SELECT SUBSTRING("date"::TEXT FOR 10) "date", "cnt" "txCount", "value" "txVolume", "fees" "fees", "from_cnt" "fromAddrCount", "to_cnt" "toAddrCount", "addr_cnt" "addrCount" FROM stellar_payment_stats WHERE "asset" = '\'\'' ORDER BY "date"' -A -F ',' -o 'stellar_payment_xlm.csv'

# iota
$PSQL -qc "\\pset footer off" -c 'SELECT SUBSTRING("date"::TEXT FOR 10) "date", "cnt" "txCount", "value" "txVolume", "med_value" "medTxVolume", "from_cnt" "fromAddrCount", "to_cnt" "toAddrCount", "addr_cnt" "addrCount" FROM iota_stats ORDER BY "date"' -A -F ',' -o 'iota.csv'

# monero
$PSQL -qc "\\pset footer off" -c 'SELECT SUBSTRING("date"::TEXT FOR 10) "date", "cnt" "txCount", "reward" - "fees" "generatedCoins", "fees" "fees", "avg_difficulty" "avgDifficulty", "med_fees" "medFees", "io_cnt" "addrCount", "payment_cnt" "paymentCount", "block_cnt" "blockCount", "block_size" "blockSize" FROM monero_stats ORDER BY "date"' -A -F ',' -o 'monero.csv'

# nem
$PSQL -qc "\\pset footer off" -c 'SELECT SUBSTRING("date"::TEXT FOR 10) "date", "cnt" "txCount", "value" "txVolume", "fees" "fees", "from_cnt" "fromAddrCount", "to_cnt" "toAddrCount", "addr_cnt" "addrCount", "med_value" "medTxVolume", "med_fees" "medFees" FROM nem_stats ORDER BY "date"' -A -F ',' -o 'nem.csv'

# neo
$PSQL -qc "\\pset footer off" -c 'SELECT SUBSTRING("date"::TEXT FOR 10) "date", "cnt" "txCount", "value" "txVolume", "fees" "fees", "from_cnt" "fromAddrCount", "to_cnt" "toAddrCount", "addr_cnt" "addrCount", "med_fees" "medFees", "med_value" "medTxVolume" FROM neo_stats WHERE "asset" = '\''\x602c79718b16e442de58778e148d0b1084e3b2dffd5de6b7b16cee7969282de7'\'' ORDER BY "date"' -A -F ',' -o 'neo_gas.csv'
$PSQL -qc "\\pset footer off" -c 'SELECT SUBSTRING("date"::TEXT FOR 10) "date", "cnt" "txCount", "value" "txVolume", "fees" "fees", "from_cnt" "fromAddrCount", "to_cnt" "toAddrCount", "addr_cnt" "addrCount", "med_fees" "medFees", "med_value" "medTxVolume" FROM neo_stats WHERE "asset" = '\''\xc56f33fc6ecfcd0c225c4ab356fee59390af8560be0e930faebe74a6daff7c9b'\'' ORDER BY "date"' -A -F ',' -o 'neo_neo.csv'

# lisk
$PSQL -qc "\\pset footer off" -c 'SELECT SUBSTRING("date"::TEXT FOR 10) "date", "cnt" "txCount", "value" "txVolume", "fees" "fees", "from_cnt" "fromAddrCount", "to_cnt" "toAddrCount", "addr_cnt" "addrCount", "med_value" "medTxVolume", "med_fees" "medFees", "payment_cnt" "paymentCount" FROM lisk.lisk_stats ORDER BY "date"' -A -F ',' -o 'lisk.csv'

# eos
$PSQL -qc "\\pset footer off" -c 'SELECT SUBSTRING("date"::TEXT FOR 10) "date", "tx_cnt" "txCount", "value" "txVolume", "med_value" "medTxVolume", "from_cnt" "fromAddrCount", "to_cnt" "toAddrCount", "addr_cnt" "addrCount", "action_cnt" "paymentCount", "block_cnt" "blockCount" FROM eos_stats ORDER BY "date"' -A -F ',' -o 'eos.csv'
