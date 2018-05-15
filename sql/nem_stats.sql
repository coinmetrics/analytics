-- nemesis block time is 2015-03-29 00:06:25
CREATE MATERIALIZED VIEW nem_stats AS (
  WITH
    block_stats AS (
      SELECT
        DATE_TRUNC('day', TIMESTAMP '2015-03-29 00:06:25' + block."timeStamp" * INTERVAL '1 second') "date",
        COUNT(*) "cnt",
        SUM(tx."fee") :: NUMERIC * 0.000001 "fees",
        SUM(COALESCE(tx."amount", 0) :: NUMERIC + COALESCE((tx."otherTrans")."amount", 0)) * 0.000001 "value",
        COUNT(DISTINCT tx."signer") "from_cnt",
        COUNT(DISTINCT COALESCE(tx."recipient", (tx."otherTrans")."recipient")) "to_cnt"
      FROM nem block, UNNEST(block.transactions) tx
      GROUP BY "date"
      ),
    addr_stats AS (
      SELECT
        t."date" "date",
        COUNT(DISTINCT t."addr") "cnt"
      FROM (
        SELECT
          DATE_TRUNC('day', TIMESTAMP '2015-03-29 00:06:25' + block."timeStamp" * INTERVAL '1 second') "date",
          tx."signerAddress" "addr"
          FROM nem block, UNNEST(block.transactions) tx
        UNION ALL
        SELECT
          DATE_TRUNC('day', TIMESTAMP '2015-03-29 00:06:25' + block."timeStamp" * INTERVAL '1 second') "date",
          COALESCE(tx."recipient", (tx."otherTrans")."recipient") "addr"
          FROM nem block, UNNEST(block.transactions) tx
        ) t
      GROUP BY t."date"
      )
    SELECT
      block_stats."date" "date",
      block_stats."cnt" "cnt",
      block_stats."fees" "fees",
      block_stats."value" "value",
      block_stats."from_cnt" "from_cnt",
      block_stats."to_cnt" "to_cnt",
      addr_stats."cnt" "addr_cnt"
      FROM block_stats LEFT JOIN addr_stats ON block_stats."date" = addr_stats."date"
      ORDER BY block_stats."date"
  ) WITH NO DATA;
