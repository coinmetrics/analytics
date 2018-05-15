CREATE MATERIALIZED VIEW erc20transfers AS (
  WITH
    tx AS (SELECT
      DATE_TRUNC('day', TO_TIMESTAMP(block.timestamp)) "date",
      (bn_in_hex(encode(log."data", 'hex')::cstring)::TEXT::NUMERIC / (10 ^ token."decimals")) "value",
      token."symbol" "tokenSymbol",
      token."contractAddress" "contractAddress",
      SUBSTRING(log."topics"[2] FROM 12 FOR 20) "from",
      SUBSTRING(log."topics"[3] FROM 12 FOR 20) "to"
      FROM ethereum block, UNNEST(block.transactions) tx INNER JOIN erc20tokens token ON tx.to = token."contractAddress", UNNEST(tx.logs) log
      WHERE log."topics"[1] = E'\\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'),
    txc AS (SELECT
      tx.*,
      SUM("value") OVER (PARTITION BY "date", "contractAddress", "from") "sum_from_value",
      SUM("value") OVER (PARTITION BY "date", "contractAddress", "to") "sum_to_value"
      FROM tx),
    txs AS (SELECT
      MIN(txc."tokenSymbol") "symbol",
      txc."date" "date",
      txc."contractAddress" "contractAddress",
      COUNT(*) "cnt",
      SUM(txc."value") "value",
      MAX(txc."value") "max_value",
      MAX(txc."sum_from_value") "max_sum_from_value",
      MAX(txc."sum_to_value") "max_sum_to_value",
      COUNT(DISTINCT txc."from") "from_cnt",
      COUNT(DISTINCT txc."to") "to_cnt"
      FROM txc GROUP BY txc."contractAddress", txc."date"
      ),
    addrs AS (
      SELECT
        tx."date" "date",
        tx."contractAddress" "contractAddress",
        tx."from" "addr"
      FROM tx
      UNION ALL
      SELECT
        tx."date" "date",
        tx."contractAddress" "contractAddress",
        tx."to" "addr"
      FROM tx
      ),
    addr_stats AS (SELECT
      t."date" "date",
      t."contractAddress" "contractAddress",
      COUNT(DISTINCT t."addr") "cnt"
      FROM addrs t
      GROUP BY t."contractAddress", t."date"
      )
    SELECT
      txs."symbol" "symbol",
      txs."date" "date",
      txs."cnt" "cnt",
      txs."value" "value",
      txs."max_value" "max_value",
      txs."max_sum_from_value" "max_sum_from_value",
      txs."max_sum_to_value" "max_sum_to_value",
      txs."from_cnt" "from_cnt",
      txs."to_cnt" "to_cnt",
      addr_stats."cnt" "addr_cnt"
    FROM txs
    LEFT JOIN addr_stats ON txs."date" = addr_stats."date" AND txs."contractAddress" = addr_stats."contractAddress"
    ORDER BY txs."date"
  ) WITH NO DATA;
