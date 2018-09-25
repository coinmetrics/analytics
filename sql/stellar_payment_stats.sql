CREATE MATERIALIZED VIEW stellar_payment_stats AS (
  WITH
    op_stats AS (
      SELECT
        DATE_TRUNC('day', TO_TIMESTAMP(ledger."closeTime")) "date",
        COUNT(DISTINCT (ledger."sequence", tx."ordinality")) "cnt",
        SUM((COALESCE(op."amount", 0) + COALESCE(op."startingBalance", 0)) :: NUMERIC * 0.0000001) "value",
        COALESCE((op."asset")."assetCode", '') "asset"
      FROM stellar ledger, UNNEST(ledger.transactions) WITH ORDINALITY tx, UNNEST(tx.operations) op
      WHERE (op."type" = 0 OR op."type" = 1) AND (op."sourceAccount" IS NOT NULL OR tx."sourceAccount" <> op."destination") AND op."sourceAccount" <> op."destination"
      GROUP BY "date", COALESCE((op."asset")."assetCode", '')
      ),
    tx_stats AS (
      SELECT
        DATE_TRUNC('day', TO_TIMESTAMP(ledger."closeTime")) "date",
        SUM(tx."fee" :: NUMERIC * 0.0000001) "fees"
      FROM stellar ledger, UNNEST(ledger.transactions) WITH ORDINALITY tx
      GROUP BY "date"
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
      op."date" "date",
      op."asset" "asset",
      op."cnt" "cnt",
      op."value" "value",
      tx."fees" "fees",
      addr_stats."from_cnt" "from_cnt",
      addr_stats."to_cnt" "to_cnt",
      addr_stats2."cnt" "addr_cnt"
    FROM op_stats op
    LEFT JOIN tx_stats tx ON op."date" = tx."date"
    LEFT JOIN addr_stats ON op."date" = addr_stats."date" AND op."asset" = addr_stats."asset"
    LEFT JOIN addr_stats2 ON op."date" = addr_stats2."date" AND op."asset" = addr_stats2."asset"
    ORDER BY op."date", op."asset"
  ) WITH NO DATA;
