CREATE MATERIALIZED VIEW ripple_payment_xrp_stats AS (
  WITH
    txq AS (
      SELECT
        DATE_TRUNC('day', TO_TIMESTAMP(tx."date")) "date",
        tx."amount" * 0.000001 "amount",
        tx."fee" * 0.000001 "fee",
        tx."account" "from",
        tx."destination" "to"
      FROM ripple ledger, UNNEST(ledger.transactions) tx
      WHERE tx."result" = 'tesSUCCESS' AND tx."type" = 'Payment' AND tx."currency" IS NULL AND tx."account" <> tx."destination"
      ),
    txs AS (
      SELECT
        txq."date" "date",
        COUNT(*) "cnt",
        SUM(txq."amount") "value",
        SUM(txq."fee") "fee",
        COUNT(DISTINCT txq."from") "from_cnt",
        COUNT(DISTINCT txq."to") "to_cnt"
      FROM txq
      GROUP BY txq."date"
      ),
    total_txs AS (
      SELECT
        DATE_TRUNC('day', TO_TIMESTAMP(ledger."closeTime")) "date1",
        SUM(ARRAY_LENGTH(ledger.transactions, 1)) "cnt"
      FROM ripple ledger
      GROUP BY "date1"
      ),
    missing_txs AS (
      SELECT
        DATE_TRUNC('day', TO_TIMESTAMP(ledger."closeTime")) "date1",
        COUNT(*) "cnt"
      FROM ripple ledger, UNNEST(ledger.transactions) tx
      WHERE tx."type" IS NULL
      GROUP BY "date1"
      ),
    addr_stats AS (SELECT
      t."date" "date",
      COUNT(DISTINCT t."addr") "cnt"
      FROM (
        SELECT
          txq."date" "date",
          txq."from" "addr"
          FROM txq
        UNION ALL
        SELECT
          txq."date" "date",
          txq."to" "addr"
          FROM txq
        ) t
      GROUP BY t."date"),
    blocks_stats AS (
      SELECT
        DATE_TRUNC('day', TO_TIMESTAMP(ledger."closeTime")) "date1",
        COUNT(*) "cnt"
      FROM ripple ledger
      GROUP BY "date1"
      ),
    missing_blocks AS (
      SELECT
        q.n "index",
        block_prev."index" "prev_index",
        DATE_TRUNC('day', TO_TIMESTAMP(block_prev."closeTime")) "prev_date",
        block_next."index" "next_index",
        DATE_TRUNC('day', TO_TIMESTAMP(block_next."closeTime")) "next_date"
      FROM GENERATE_SERIES(32570, (SELECT MAX(index) FROM ripple)) q(n)
      LEFT JOIN ripple block ON q.n = block.index
      LEFT JOIN ripple block_prev ON q.n = block_prev.index + 1
      LEFT JOIN ripple block_next ON q.n + 1 = block_next.index
      WHERE block.index IS NULL AND (block_prev.index IS NOT NULL OR block_next.index IS NOT NULL)
      ),
    missing_blocks_dates AS (
      SELECT
        l."index" "begin",
        l."prev_date" "begin_date",
        (SELECT MIN(r."index") FROM missing_blocks r WHERE r."index" >= l."index" AND r."next_index" IS NOT NULL) "end",
        (SELECT MIN(r."next_date") FROM missing_blocks r WHERE r."index" >= l."index" AND r."next_index" IS NOT NULL) "end_date"
      FROM missing_blocks l
      WHERE l."prev_index" IS NOT NULL
      ),
    missing_blocks_ranges AS (
      SELECT
        q."begin_date" "begin_date",
        MAX(q."end_date") "end_date",
        SUM(q."end" - q."begin" + 1) "cnt"
      FROM missing_blocks_dates q
      GROUP BY q."begin_date"
      )
    SELECT
      txs."date" "date",
      txs."cnt" "cnt",
      txs."value" "value",
      txs."fee" "fee",
      txs."from_cnt" "from_cnt",
      txs."to_cnt" "to_cnt",
      addr_stats."cnt" "addr_cnt",
      blocks_stats."cnt" "block_cnt",
      missing_blocks_ranges."cnt" "missing_block_cnt",
      total_txs."cnt" "total_cnt",
      missing_txs."cnt" "missing_cnt"
    FROM txs
    LEFT JOIN addr_stats ON txs."date" = addr_stats."date"
    LEFT JOIN blocks_stats ON txs."date" = blocks_stats."date1"
    LEFT JOIN missing_blocks_ranges ON txs."date" = missing_blocks_ranges."begin_date"
    LEFT JOIN total_txs ON txs."date" = total_txs."date1"
    LEFT JOIN missing_txs ON txs."date" = missing_txs."date1"
    ORDER BY txs."date"
  ) WITH NO DATA;
