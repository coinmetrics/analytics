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
  DATE_TRUNC('day', TO_TIMESTAMP(MIN(t."time"))) "min_date"
  FROM (
    (SELECT
      tx."time" "time",
      tx."from" "address",
      -(tx."value" + tx."fee") "value"
      FROM ethereum_tx tx
      )
    UNION ALL
    (SELECT
      tx."time" "time",
      tx."to" "address",
      tx."value" "value"
      FROM ethereum_tx tx
      )
  ) t
  GROUP BY t."address"
  HAVING MAX(t."time") - MIN(t."time") <= 86400 AND SUM(t."value") = 0
;
ANALYZE ethereum_short_addrs;

CREATE INDEX ON ethereum_short_addrs("min_date");
