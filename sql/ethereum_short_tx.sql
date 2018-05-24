CREATE MATERIALIZED VIEW ethereum_short_tx AS (
  SELECT
    DATE_TRUNC('day', TO_TIMESTAMP(t."min_time")) "date",
    SUM(t."cnt") "cnt",
    SUM(t."positive_value") "value"
    FROM
      (SELECT
        COUNT(*) "cnt",
        MIN(t."time") "min_time",
        SUM(GREATEST(t."value", 0)) "positive_value"
        FROM (
          (SELECT
            block."timestamp" "time",
            COALESCE(tx."to", tx."contractAddress") "address",
            tx."value" * 1e-18 "value"
            FROM ethereum block, UNNEST(block.transactions) tx
            )
          UNION ALL
          (SELECT
            block."timestamp" "time",
            tx."from" "address",
            -(tx."value" :: NUMERIC + tx."gasUsed" :: NUMERIC * tx."gasPrice" :: NUMERIC) * 1e-18 "value"
            FROM ethereum block, UNNEST(block.transactions) tx
            )
        ) t
        GROUP BY t."address"
        HAVING MAX(t."time") - MIN(t."time") <= 86400 AND SUM(t."value") = 0
      ) t
    GROUP BY "date" ORDER BY "date"
  ) WITH NO DATA;
