DROP TABLE IF EXISTS ethereum_stats_traced;
CREATE UNLOGGED TABLE ethereum_stats_traced AS (
  WITH
    blocks AS (SELECT
      DATE_TRUNC('day', TO_TIMESTAMP(block."timestamp")) "date",
      CASE WHEN block."number" < 4370000 THEN 5 ELSE 3 END "reward",
      block."number" "number",
      block."difficulty" "difficulty",
      block."size" "size",
      ARRAY_LENGTH(block."transactions", 1) "tx_cnt",
      block."uncles" "uncles"
      FROM ethereum block),
    blocks_stats AS (SELECT
      block."date" "date",
      AVG(block."difficulty") "avg_difficulty",
      SUM(block."size") / SUM(block."tx_cnt") "avg_tx_size",
      COUNT(*) "cnt",
      SUM(block."size") "sum_size",
      SUM(block."reward") "reward"
      FROM blocks block GROUP BY block."date"),
    uncles_stats AS (SELECT
      block."date" "date",
      SUM((((uncle."number" - block."number") :: NUMERIC) * 0.125 + 1.03125) * block."reward") "reward"
      FROM blocks block, UNNEST(block."uncles") uncle GROUP BY block."date"),
    txs_stats AS (SELECT
      tx."date" "date",
      COUNT(*) "cnt",
      SUM(tx."value") "sum_value",
      PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tx."value") "med_value",
      PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tx."fee") "med_fee",
      SUM(tx."fee") "sum_fee",
      COUNT(DISTINCT tx."from") "from_cnt",
      COUNT(DISTINCT tx."to") "to_cnt"
      FROM ethereum_tx tx GROUP BY tx."date"),
    actions_stats AS (SELECT
      action."date" "date",
      COUNT(*) "cnt",
      SUM(action."value") "sum_value",
      PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY action."value") "med_value",
      COUNT(DISTINCT action."from") "from_cnt",
      COUNT(DISTINCT action."to") "to_cnt"
      FROM ethereum_actions action GROUP BY action."date"),
    addr_stats AS (SELECT
      t."date" "date",
      COUNT(DISTINCT t."addr") "cnt"
      FROM (
        SELECT
          tx."date" "date",
          tx."from" "addr"
          FROM ethereum_tx tx
        UNION ALL
        SELECT
          tx."date" "date",
          tx."to" "addr"
          FROM ethereum_tx tx
        ) t
      GROUP BY "date"),
    addr_actions_stats AS (SELECT
      t."date" "date",
      COUNT(DISTINCT t."addr") "cnt"
      FROM (
        SELECT
          action."date" "date",
          action."from" "addr"
          FROM ethereum_actions action
        UNION ALL
        SELECT
          action."date" "date",
          action."to" "addr"
          FROM ethereum_actions action
        ) t
      GROUP BY "date"),
    payments_stats AS (SELECT
      tx."date" "date",
      COUNT(*) "cnt"
      FROM ethereum_tx tx LEFT JOIN ethereum_tx contract ON tx."to" = contract."contractAddress"
      WHERE tx."to" IS NOT NULL AND contract IS NULL
      GROUP BY tx."date")
    SELECT
      block."date" "date",
      tx."cnt" "tx_cnt",
      payment."cnt" "payment_cnt",
      action."cnt" "action_cnt",
      tx."sum_value" "sum_value",
      tx."med_value" "med_value",
      action."sum_value" "sum_action_value",
      action."med_value" "med_action_value",
      block."avg_difficulty" "avg_difficulty",
      block."avg_tx_size" "avg_tx_size",
      tx."sum_fee" "sum_fee",
      block."cnt" "block_cnt",
      block."sum_size" "sum_size",
      tx."med_fee" "med_fee",
      block."reward" + uncle."reward" "reward",
      tx."from_cnt" "from_cnt",
      tx."to_cnt" "to_cnt",
      addr."cnt" "addr_cnt",
      action."from_cnt" "action_from_cnt",
      action."to_cnt" "action_to_cnt",
      addr_action."cnt" "addr_action_cnt"
    FROM blocks_stats block
    LEFT JOIN uncles_stats uncle ON block."date" = uncle."date"
    LEFT JOIN txs_stats tx ON block."date" = tx."date"
    LEFT JOIN addr_stats addr ON block."date" = addr."date"
    LEFT JOIN actions_stats action ON block."date" = action."date"
    LEFT JOIN addr_actions_stats addr_action ON block."date" = addr_action."date"
    LEFT JOIN payments_stats payment ON block."date" = payment."date"
    ORDER BY "date"
  );
