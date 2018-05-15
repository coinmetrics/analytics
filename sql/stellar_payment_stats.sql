CREATE MATERIALIZED VIEW stellar_payment_stats AS (
  WITH
    tx1 AS (
      SELECT
        DATE_TRUNC('day', TO_TIMESTAMP(ledger."closeTime")) "date",
        SUM(COALESCE(op."amount", 0) + COALESCE(op."startingBalance", 0)) OVER w :: NUMERIC * 0.0000001 "value",
        ROW_NUMBER() OVER w op_index,
        tx."fee" :: NUMERIC * 0.0000001 "fee",
        COALESCE((op."asset")."assetCode", '') "asset"
      FROM stellar ledger, UNNEST(ledger.transactions) WITH ORDINALITY tx, UNNEST(tx.operations) op
      WHERE (op."type" = 0 OR op."type" = 1) AND (op."sourceAccount" IS NOT NULL OR tx."sourceAccount" <> op."destination") AND op."sourceAccount" <> op."destination"
      WINDOW w AS (PARTITION BY ledger."sequence", tx."ordinality")
      ),
    txs AS (
      SELECT
        tx1."date" "date",
        tx1."asset" "asset",
        COUNT(*) "cnt",
        SUM(tx1."value") "value",
        SUM(tx1."fee") "fees"
      FROM tx1
      WHERE "op_index" = 1
      GROUP BY "date", "asset"
      ),
    addr_stats AS (
      SELECT
        DATE_TRUNC('day', TO_TIMESTAMP(ledger."closeTime")) "date",
        COALESCE((op."asset")."assetCode", '') "asset",
        COUNT(DISTINCT op."sourceAccount") "from_cnt",
        COUNT(DISTINCT op."destination") "to_cnt"
      FROM stellar ledger, UNNEST(ledger.transactions) WITH ORDINALITY tx, UNNEST(tx.operations) op
      WHERE (op."type" = 0 OR op."type" = 1) AND (op."sourceAccount" IS NOT NULL OR tx."sourceAccount" <> op."destination") AND op."sourceAccount" <> op."destination"
      GROUP BY "date", (op."asset")."assetCode"
      ),
    addr_stats2 AS (
      SELECT
        t."date" "date",
        t."asset" "asset",
        COUNT(DISTINCT t."addr") "cnt"
      FROM (
        SELECT
          DATE_TRUNC('day', TO_TIMESTAMP(ledger."closeTime")) "date",
          COALESCE((op."asset")."assetCode", '') "asset",
          op."sourceAccount" "addr"
          FROM stellar ledger, UNNEST(ledger.transactions) WITH ORDINALITY tx, UNNEST(tx.operations) op
          WHERE (op."type" = 0 OR op."type" = 1) AND (op."sourceAccount" IS NOT NULL OR tx."sourceAccount" <> op."destination") AND op."sourceAccount" <> op."destination"
        UNION ALL
        SELECT
          DATE_TRUNC('day', TO_TIMESTAMP(ledger."closeTime")) "date",
          COALESCE((op."asset")."assetCode", '') "asset",
          op."destination" "addr"
          FROM stellar ledger, UNNEST(ledger.transactions) WITH ORDINALITY tx, UNNEST(tx.operations) op
          WHERE (op."type" = 0 OR op."type" = 1) AND (op."sourceAccount" IS NOT NULL OR tx."sourceAccount" <> op."destination") AND op."sourceAccount" <> op."destination"
        ) t
      GROUP BY t."date", t."asset"
      )
    SELECT
      txs."date" "date",
      txs."asset" "asset",
      txs."cnt" "cnt",
      txs."value" "value",
      txs."fees" "fees",
      addr_stats."from_cnt" "from_cnt",
      addr_stats."to_cnt" "to_cnt",
      addr_stats2."cnt" "addr_cnt"
    FROM txs
    LEFT JOIN addr_stats ON txs."date" = addr_stats."date" AND txs."asset" = addr_stats."asset"
    LEFT JOIN addr_stats2 ON txs."date" = addr_stats2."date" AND txs."asset" = addr_stats2."asset"
    ORDER BY txs."date", txs."asset"
  ) WITH NO DATA;
