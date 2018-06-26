DROP TABLE IF EXISTS ethereum_actions;
CREATE UNLOGGED TABLE ethereum_actions AS SELECT
  block."timestamp" "time",
  DATE_TRUNC('day', TO_TIMESTAMP(block."timestamp")) "date",
  action."from" "from",
  action."to" "to",
  action."value" * 1e-18 "value"
  FROM ethereum block, UNNEST(block.transactions) tx, UNNEST(tx.actions) action
  WHERE action."accounted"
;
ANALYZE ethereum_actions;

CREATE INDEX ON ethereum_actions("date");
