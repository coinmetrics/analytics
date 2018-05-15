CREATE MATERIALIZED VIEW monero_stats AS (
  WITH
    bs AS (SELECT
      DATE_TRUNC('day', TO_TIMESTAMP(block."timestamp")) "date",
      SUM(block."reward" :: NUMERIC) * 1e-12 "reward"
      FROM monero block
      GROUP BY "date"
      ),
    txi AS (SELECT
      tx."hash" "txid",
      SUM(vin."amount" :: NUMERIC) * 1e-12 "amount"
      FROM monero block, UNNEST(block.transactions) tx, UNNEST(tx.vin) vin
      GROUP BY block."height", "txid"
      ),
    txo AS (SELECT
      tx."hash" "txid",
      SUM(vout."amount" :: NUMERIC) * 1e-12 "amount"
      FROM monero block, UNNEST(block.transactions) tx, UNNEST(tx.vout) vout
      GROUP BY block."height", "txid"
      ),
    txs AS (SELECT
      DATE_TRUNC('day', TO_TIMESTAMP(block."timestamp")) "date",
      COUNT(*) "cnt",
      SUM(CASE WHEN block."height" < 1220516 THEN txi."amount" - txo."amount" ELSE "fee" :: NUMERIC * 1e-12 END) "fees"
      FROM monero block, UNNEST(block.transactions) tx
      LEFT JOIN txi ON tx."hash" = txi."txid"
      LEFT JOIN txo ON tx."hash" = txo."txid"
      GROUP BY "date"
      )
    SELECT
      bs."date" "date",
      bs."reward" "reward",
      txs."cnt" "cnt",
      txs."fees" "fees"
    FROM bs LEFT JOIN txs ON bs."date" = txs."date"
  ) WITH NO DATA;
