CREATE MATERIALIZED VIEW erc20transfers AS (
  WITH
    tx AS (SELECT
      DATE_TRUNC('day', TO_TIMESTAMP(block.timestamp)) "date",
      (bn_in_hex(encode(log."data", 'hex')::cstring)::TEXT::NUMERIC(100, 18) / POWER(10::NUMERIC, token."decimals"::NUMERIC)::NUMERIC(100, 18))::NUMERIC "value",
      token."symbol" "symbol",
      SUBSTRING(log."topics"[2] FROM 13 FOR 20) "from",
      SUBSTRING(log."topics"[3] FROM 13 FOR 20) "to"
      FROM ethereum block, UNNEST(block.transactions) tx, UNNEST(tx.logs) log INNER JOIN erc20tokens token ON log."address" = token."contractAddress"
      WHERE log."topics"[1] = E'\\xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'),
    txc AS (SELECT
      tx.*,
      SUM("value") OVER (PARTITION BY "date", "symbol", "from") "sum_from_value",
      SUM("value") OVER (PARTITION BY "date", "symbol", "to") "sum_to_value"
      FROM tx),
    txs AS (SELECT
      txc."symbol" "symbol",
      txc."date" "date",
      COUNT(*) "cnt",
      SUM(txc."value") "value",
      MAX(txc."value") "max_value",
      PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY txc."value") "med_value",
      MAX(txc."sum_from_value") "max_sum_from_value",
      MAX(txc."sum_to_value") "max_sum_to_value",
      COUNT(DISTINCT txc."from") "from_cnt",
      COUNT(DISTINCT txc."to") "to_cnt"
      FROM txc GROUP BY txc."symbol", txc."date"
      ),
    addrs AS (
      SELECT
        tx."date" "date",
        tx."symbol" "symbol",
        tx."from" "addr"
      FROM tx
      UNION ALL
      SELECT
        tx."date" "date",
        tx."symbol" "symbol",
        tx."to" "addr"
      FROM tx
      ),
    addr_stats AS (SELECT
      t."date" "date",
      t."symbol" "symbol",
      COUNT(DISTINCT t."addr") "cnt"
      FROM addrs t
      GROUP BY t."symbol", t."date"
      )
    SELECT
      txs."symbol" "symbol",
      txs."date" "date",
      txs."cnt" "cnt",
      txs."value" "value",
      txs."max_value" "max_value",
      txs."med_value" "med_value",
      txs."max_sum_from_value" "max_sum_from_value",
      txs."max_sum_to_value" "max_sum_to_value",
      txs."from_cnt" "from_cnt",
      txs."to_cnt" "to_cnt",
      addr_stats."cnt" "addr_cnt"
    FROM txs
    LEFT JOIN addr_stats ON txs."date" = addr_stats."date" AND txs."symbol" = addr_stats."symbol"
    ORDER BY txs."date"
  ) WITH NO DATA;
