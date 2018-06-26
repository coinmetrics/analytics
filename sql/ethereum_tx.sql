DROP TABLE IF EXISTS ethereum_tx;
CREATE UNLOGGED TABLE ethereum_tx AS SELECT
  block."timestamp" "time",
  DATE_TRUNC('day', TO_TIMESTAMP(block."timestamp")) "date",
  tx."from" "from",
  tx."to" "to",
  tx."contractAddress" "contractAddress",
  tx."value" * 1e-18 "value",
  (tx."gasUsed" * tx."gasPrice") * 1e-18 "fee"
  FROM ethereum block, UNNEST(block.transactions) tx
;
ANALYZE ethereum_tx;

CREATE INDEX ON ethereum_tx ("date");
CREATE INDEX ON ethereum_tx ("contractAddress");
