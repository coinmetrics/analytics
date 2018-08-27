DROP TABLE IF EXISTS ethereum_short_addrs;
CREATE UNLOGGED TABLE ethereum_short_addrs (
  "address" BYTEA PRIMARY KEY,
  "cnt" INTEGER NOT NULL,
  "positive_value" NUMERIC NOT NULL,
  "min_time" INTEGER NOT NULL,
  "min_date" TIMESTAMPTZ NOT NULL,
  "component" INTEGER NOT NULL DEFAULT 0
);
INSERT INTO ethereum_short_addrs SELECT
  t."address" "address",
  COUNT(*) "cnt",
  SUM(GREATEST(t."value", 0)) "positive_value",
  MIN(t."time") "min_time",
  DATE_TRUNC('day', TIMESTAMP 'epoch' + MIN(t."time") * INTERVAL '1 second') "min_date"
  FROM (
    (SELECT
      block."timestamp" "time",
      tx."from" "address",
      (tx."value" + tx."gasUsed" * tx."gasPrice") * -1e-18 "value"
      FROM ethereum block, UNNEST(block.transactions) tx
      )
    UNION ALL
    (SELECT
      block."timestamp" "time",
      tx."to" "address",
      tx."value" * 1e-18 "value"
      FROM ethereum block, UNNEST(block.transactions) tx
      )
  ) t
  GROUP BY t."address"
  HAVING MAX(t."time") - MIN(t."time") <= 86400 AND SUM(t."value") = 0
;
ANALYZE ethereum_short_addrs;

CREATE INDEX ON ethereum_short_addrs("min_date");
