CREATE MATERIALIZED VIEW cardano_stats AS (
  WITH
    tp AS (
      (SELECT
        tx."id" "txid",
        input."address" "address",
        -input."value" "value"
        FROM cardano block, UNNEST(block.transactions) tx, UNNEST(tx.inputs) input
        )
      UNION ALL
      (SELECT
        tx."id" "txid",
        output."address" "address",
        output."value" "value"
        FROM cardano block, UNNEST(block.transactions) tx, UNNEST(tx.outputs) output
        )
      ),
    txio AS (SELECT
      "txid",
      "address",
      SUM("value") * 0.000001 "value"
      FROM tp
      GROUP BY "txid", "address"
      ),
    txq AS (SELECT
      "txid",
      -SUM("value") "fees",
      SUM(GREATEST("value", 0)) "volume"
      FROM txio GROUP BY "txid"
      ),
    txs AS (SELECT
      DATE_TRUNC('day', TO_TIMESTAMP(tx."timeIssued")) "date",
      COUNT(*) "cnt",
      SUM(txq."fees") "fees",
      SUM(txq."volume") "volume"
      FROM cardano block, UNNEST(block.transactions) tx INNER JOIN txq ON tx."id" = txq."txid"
      GROUP BY "date"
      ),
    tis AS (SELECT
      DATE_TRUNC('day', TO_TIMESTAMP(tx."timeIssued")) "date",
      COUNT(DISTINCT input."address") "from_cnt"
      FROM cardano block, UNNEST(block.transactions) tx, UNNEST(tx.inputs) input
      GROUP BY "date"
      ),
    tos AS (SELECT
      DATE_TRUNC('day', TO_TIMESTAMP(tx."timeIssued")) "date",
      COUNT(DISTINCT output."address") "to_cnt"
      FROM cardano block, UNNEST(block.transactions) tx, UNNEST(tx.outputs) output
      GROUP BY "date"
      ),
    tios AS (SELECT
      t."date" "date",
      COUNT(DISTINCT t."address") "cnt"
      FROM (
        SELECT
          DATE_TRUNC('day', TO_TIMESTAMP(tx."timeIssued")) "date",
          input."address" "address"
        FROM cardano block, UNNEST(block.transactions) tx, UNNEST(tx.inputs) input
        UNION ALL
        SELECT
          DATE_TRUNC('day', TO_TIMESTAMP(tx."timeIssued")) "date",
          output."address" "address"
        FROM cardano block, UNNEST(block.transactions) tx, UNNEST(tx.outputs) output
        ) t
      GROUP BY t."date"
      )
    SELECT
      txs."date" "date",
      txs."cnt" "cnt",
      txs."fees" "fees",
      txs."volume" "volume",
      tis."from_cnt" "from_cnt",
      tos."to_cnt" "to_cnt",
      tios."cnt" "addr_cnt"
    FROM txs
    LEFT JOIN tis ON txs."date" = tis."date"
    LEFT JOIN tos ON txs."date" = tos."date"
    LEFT JOIN tios ON txs."date" = tios."date"
    ORDER BY "date"
  ) WITH NO DATA;
