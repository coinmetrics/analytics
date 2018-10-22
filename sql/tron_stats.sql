CREATE MATERIALIZED VIEW tron_stats AS (WITH
  block_stats AS (SELECT
    DATE_TRUNC('day', TO_TIMESTAMP(block."timestamp" * 0.001)) "date",
    COUNT(*) "cnt"
    FROM tron block
    WHERE block."number" > 0
    GROUP BY "date"
    ),
  tx_stats AS (SELECT
    DATE_TRUNC('day', TO_TIMESTAMP(block."timestamp" * 0.001)) "date",
    COUNT(*) "cnt"
    FROM tron block, UNNEST(block.transactions) tx
    WHERE block."number" > 0
    GROUP BY "date"
    ),
  payments AS (SELECT
    DATE_TRUNC('day', TO_TIMESTAMP(block."timestamp" * 0.001)) "date",
    contract."owner_address" "from",
    contract."to_address" "to",
    contract."amount" :: NUMERIC * 0.000001 "value"
    FROM tron block, UNNEST(block.transactions) tx, UNNEST(tx.contracts) contract
    WHERE block."number" > 0 AND contract."type" = 'TransferContract'
    ),
  payment_stats AS (SELECT
    tx."date" "date",
    COUNT(*) "cnt",
    COUNT(DISTINCT tx."from") "from_cnt",
    COUNT(DISTINCT tx."to") "to_cnt",
    SUM(tx."value") "value",
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tx."value") "med_value"
    FROM payments tx
    GROUP BY tx."date"
    ),
  addr_stats AS (SELECT
    t."date" "date",
    COUNT(DISTINCT t."addr") "addr_cnt"
    FROM (
      SELECT
        tx."date" "date",
        tx."from" "addr"
      FROM payments tx
      UNION ALL
      SELECT
        tx."date" "date",
        tx."to" "addr"
      FROM payments tx
      ) t
    GROUP BY t."date"
    )
  SELECT
    tx."date" "date",
    block."cnt" "block_cnt",
    tx."cnt" "tx_cnt",
    payment."cnt" "payment_cnt",
    payment."value" "value",
    payment."med_value" "med_value",
    payment."from_cnt" "from_cnt",
    payment."to_cnt" "to_cnt",
    addr."addr_cnt" "addr_cnt"
  FROM block_stats block
  LEFT JOIN tx_stats tx ON block."date" = tx."date"
  LEFT JOIN payment_stats payment ON block."date" = payment."date"
  LEFT JOIN addr_stats addr ON block."date" = addr."date"
  ORDER BY block."date"
  ) WITH NO DATA;
