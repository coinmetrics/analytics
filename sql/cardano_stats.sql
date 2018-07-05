CREATE MATERIALIZED VIEW cardano_stats AS (
  WITH
    tp AS (
      (SELECT
        tx."id" "txid",
        tx."timeIssued" "time",
        input."address" "address",
        -input."value" "value",
        0 :: INTEGER "is_output"
        FROM cardano block, UNNEST(block.transactions) tx, UNNEST(tx.inputs) input
        )
      UNION ALL
      (SELECT
        tx."id" "txid",
        tx."timeIssued" "time",
        output."address" "address",
        output."value" "value",
        1 :: INTEGER "is_output"
        FROM cardano block, UNNEST(block.transactions) tx, UNNEST(tx.outputs) output
        )
      ),
    short_addrs AS (SELECT
      "address"
      FROM tp
      GROUP BY "address"
      HAVING SUM("value") = 0 AND MAX("time") - MIN("time") <= 2400
      ),
    txio AS (SELECT
      "txid",
      "address",
      SUM("value") * 0.000001 "value",
      SUM("is_output") "output_cnt"
      FROM tp
      GROUP BY "txid", "address"
      ),
    txq AS (SELECT
      "txid",
      -SUM("value") "fees",
      SUM(GREATEST("value", 0)) "volume",
      SUM(CASE WHEN addr."address" IS NOT NULL THEN GREATEST("value", 0) ELSE NULL END) "short_volume",
      GREATEST(SUM("output_cnt") - 1, 0) "payment_cnt"
      FROM txio
      LEFT JOIN short_addrs addr ON txio."address" = addr."address"
      GROUP BY "txid"
      ),
    txs AS (SELECT
      DATE_TRUNC('day', TO_TIMESTAMP(tx."timeIssued")) "date",
      COUNT(*) "cnt",
      SUM(txq."fees") "fees",
      SUM(txq."volume") "volume",
      PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY txq."volume") "med_volume",
      PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY txq."fees") "med_fees",
      SUM(txq."payment_cnt") "payment_cnt",
      SUM(txq."short_volume") "short_volume"
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
      txs."volume" "volume",
      txs."fees" "fees",
      txs."med_volume" "med_volume",
      txs."med_fees" "med_fees",
      txs."payment_cnt" "payment_cnt",
      txs."short_volume" "short_volume",
      tis."from_cnt" "from_cnt",
      tos."to_cnt" "to_cnt",
      tios."cnt" "addr_cnt"
    FROM txs
    LEFT JOIN tis ON txs."date" = tis."date"
    LEFT JOIN tos ON txs."date" = tos."date"
    LEFT JOIN tios ON txs."date" = tios."date"
    ORDER BY "date"
  ) WITH NO DATA;
