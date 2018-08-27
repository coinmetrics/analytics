DROP TABLE IF EXISTS ethereum_contracts;
CREATE UNLOGGED TABLE ethereum_contracts (
  "address" BYTEA PRIMARY KEY
);
INSERT INTO ethereum_contracts SELECT
  action."to" "address"
  FROM ethereum block, UNNEST(block.transactions) tx, UNNEST(tx.actions) action
  WHERE action."accounted" AND action."type" = 1 AND action."to" IS NOT NULL
;
