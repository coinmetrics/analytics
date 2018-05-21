#!/bin/bash

# additional arguments
PSQL="psql -h 127.0.0.1 -U postgres"

# update materialized views
time $PSQL -qc "REFRESH MATERIALIZED VIEW erc20transfers"
time $PSQL -qc "REFRESH MATERIALIZED VIEW ethereum_stats"
time $PSQL -qc "REFRESH MATERIALIZED VIEW ethereum_classic_stats"
time $PSQL -qc "REFRESH MATERIALIZED VIEW cardano_stats"
time $PSQL -qc "REFRESH MATERIALIZED VIEW ripple_payment_xrp_stats"
time $PSQL -qc "REFRESH MATERIALIZED VIEW stellar_payment_stats"
time $PSQL -qc "REFRESH MATERIALIZED VIEW stellar_payment_stats_prep"
time $PSQL -qc "REFRESH MATERIALIZED VIEW monero_stats"
time $PSQL -qc "REFRESH MATERIALIZED VIEW nem_stats"
time $PSQL -qc "REFRESH MATERIALIZED VIEW neo_stats"
time $PSQL -qc "REFRESH MATERIALIZED VIEW lisk_stats" lisk_main lisk

### export csv's

# ERC20 tokens
for SYMBOL in $($PSQL -qtc "SELECT symbol FROM erc20tokens")
do
	$PSQL -qc "\\pset footer off" -c "SELECT SUBSTRING(\"date\"::TEXT FOR 10) \"date\", \"cnt\" \"txCount\", \"value\" \"txVolume\", \"max_value\" \"maxOneTxVolume\", \"max_sum_from_value\" \"maxAddrSumFromValue\", \"max_sum_to_value\" \"maxAddrSumToValue\", \"from_cnt\" \"fromAddrCount\", \"to_cnt\" \"toAddrCount\", \"addr_cnt\" \"addrCount\", \"med_value\" \"medValue\" FROM erc20transfers WHERE \"symbol\" = '$SYMBOL' ORDER BY \"date\"" -A -F "," -o "$SYMBOL.csv"
done

# ethereum
$PSQL -qc "\\pset footer off" -c "SELECT SUBSTRING(\"date\"::TEXT FOR 10) \"date\", \"tx_cnt\" \"txCount\", \"sum_value\" \"txVolume\", \"med_value\" \"medValue\", \"avg_difficulty\" \"avgDifficulty\", \"avg_tx_size\" \"avgTxSize\", \"sum_fee\" \"sumFee\", \"block_cnt\" \"blockCount\", \"sum_size\" \"totalSize\", \"med_fee\" \"medFee\", \"reward\" \"generatedVolume\", \"from_cnt\" \"fromAddrCount\", \"to_cnt\" \"toAddrCount\", \"addr_cnt\" \"addrCount\", \"payment_cnt\" \"paymentCount\" FROM ethereum_stats ORDER BY \"date\"" -A -F "," -o "eth_stats.csv"

# ethereum classic
$PSQL -qc "\\pset footer off" -c "SELECT SUBSTRING(\"date\"::TEXT FOR 10) \"date\", \"tx_cnt\" \"txCount\", \"sum_value\" \"txVolume\", \"med_value\" \"medValue\", \"avg_difficulty\" \"avgDifficulty\", \"avg_tx_size\" \"avgTxSize\", \"sum_fee\" \"sumFee\", \"block_cnt\" \"blockCount\", \"sum_size\" \"totalSize\", \"med_fee\" \"medFee\", \"reward\" \"generatedVolume\", \"from_cnt\" \"fromAddrCount\", \"to_cnt\" \"toAddrCount\", \"addr_cnt\" \"addrCount\", \"payment_cnt\" \"paymentCount\" FROM ethereum_classic_stats ORDER BY \"date\"" -A -F "," -o "eth_classic_stats.csv"

# cardano
$PSQL -qc "\\pset footer off" -c "SELECT SUBSTRING(\"date\"::TEXT FOR 10) \"date\", \"cnt\" \"txCount\", \"volume\" \"txVolume\", \"fees\" \"fees\", \"from_cnt\" \"fromAddrCount\", \"to_cnt\" \"toAddrCount\", \"addr_cnt\" \"addrCount\" FROM cardano_stats ORDER BY \"date\"" -A -F "," -o "cardano.csv"

# ripple XRP payments
$PSQL -qc "\\pset footer off" -c "SELECT SUBSTRING(\"date\"::TEXT FOR 10) \"date\", \"cnt\" \"txCount\", \"value\" \"txVolume\", \"fee\" \"fees\", \"from_cnt\" \"fromAddrCount\", \"to_cnt\" \"toAddrCount\", \"addr_cnt\" \"addrCount\", \"block_cnt\" \"blockCount\", \"missing_block_cnt\" \"missingBlockCount\", \"total_cnt\" \"totalTxCount\", \"missing_cnt\" \"missingTxCount\" FROM ripple_payment_xrp_stats ORDER BY \"date\"" -A -F "," -o "ripple_payment_xrp.csv"

# stellar payments
$PSQL -qc "\\pset footer off" -c "SELECT * FROM stellar_payment_stats_prep ORDER BY \"date\"" -A -F "," -o "stellar_payment.csv"
# stellar XLM payments
$PSQL -qc "\\pset footer off" -c "SELECT SUBSTRING(\"date\"::TEXT FOR 10) \"date\", \"cnt\" \"txCount\", \"value\" \"txVolume\", \"fees\" \"fees\", \"from_cnt\" \"fromAddrCount\", \"to_cnt\" \"toAddrCount\", \"addr_cnt\" \"addrCount\" FROM stellar_payment_stats WHERE \"asset\" = '' ORDER BY \"date\"" -A -F "," -o "stellar_payment_xlm.csv"

# monero
$PSQL -qc "\\pset footer off" -c "SELECT SUBSTRING(\"date\"::TEXT FOR 10) \"date\", \"cnt\" \"txCount\", \"reward\" - \"fees\" \"generatedCoins\", \"fees\" \"fees\" FROM monero_stats ORDER BY \"date\"" -A -F "," -o "monero.csv"

# nem
$PSQL -qc "\\pset footer off" -c "SELECT SUBSTRING(\"date\"::TEXT FOR 10) \"date\", \"cnt\" \"txCount\", \"value\" \"txVolume\", \"fees\" \"fees\", \"from_cnt\" \"fromAddrCount\", \"to_cnt\" \"toAddrCount\", \"addr_cnt\" \"addrCount\" FROM nem_stats ORDER BY \"date\"" -A -F "," -o "nem.csv"

# neo
$PSQL -qc "\\pset footer off" -c "SELECT SUBSTRING(\"date\"::TEXT FOR 10) \"date\", \"cnt\" \"txCount\", \"value\" \"txVolume\", \"fees\" \"fees\", \"from_cnt\" \"fromAddrCount\", \"to_cnt\" \"toAddrCount\", \"addr_cnt\" \"addrCount\" FROM neo_stats WHERE \"asset\" = '\\x602c79718b16e442de58778e148d0b1084e3b2dffd5de6b7b16cee7969282de7' ORDER BY \"date\"" -A -F "," -o "neo_gas.csv"
$PSQL -qc "\\pset footer off" -c "SELECT SUBSTRING(\"date\"::TEXT FOR 10) \"date\", \"cnt\" \"txCount\", \"value\" \"txVolume\", \"fees\" \"fees\", \"from_cnt\" \"fromAddrCount\", \"to_cnt\" \"toAddrCount\", \"addr_cnt\" \"addrCount\" FROM neo_stats WHERE \"asset\" = '\\xc56f33fc6ecfcd0c225c4ab356fee59390af8560be0e930faebe74a6daff7c9b' ORDER BY \"date\"" -A -F "," -o "neo_neo.csv"

# lisk
$PSQL -qc "\\pset footer off" -c "SELECT SUBSTRING(\"date\"::TEXT FOR 10) \"date\", \"cnt\" \"txCount\", \"value\" \"txVolume\", \"fees\" \"fees\", \"from_cnt\" \"fromAddrCount\", \"to_cnt\" \"toAddrCount\", \"addr_cnt\" \"addrCount\" FROM lisk_stats ORDER BY \"date\"" -A -F "," -o "lisk.csv" lisk_main lisk
