-- Lisk stats
-- genesis block time is 2016-05-24 20:00:00
CREATE MATERIALIZED VIEW lisk_stats AS (
  WITH
    blocks_stats AS (SELECT
      DATE_TRUNC('day', TIMESTAMP '2016-05-24 20:00:00' + block."timestamp" * INTERVAL '1 second') "date",
      SUM(block."numberOfTransactions") "cnt",
      SUM(block."totalAmount") :: NUMERIC * 0.00000001 "value",
      SUM(block."totalFee") :: NUMERIC * 0.00000001 "fees"
      FROM blocks block
      GROUP BY "date"
      ),
    tx_stats AS (SELECT
      DATE_TRUNC('day', TIMESTAMP '2016-05-24 20:00:00' + tx."timestamp" * INTERVAL '1 second') "date",
      PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tx."amount") "med_value",
      PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tx."fee") "med_fees",
      COUNT(DISTINCT tx."senderId") "from_cnt",
      COUNT(DISTINCT tx."recipientId") "to_cnt"
      FROM trs tx
      GROUP BY "date"
      ),
    addrs AS (
      (SELECT
        DATE_TRUNC('day', TIMESTAMP '2016-05-24 20:00:00' + tx."timestamp" * INTERVAL '1 second') "date",
        tx."senderId" "addr"
      FROM trs tx)
      UNION ALL
      (SELECT
        DATE_TRUNC('day', TIMESTAMP '2016-05-24 20:00:00' + tx."timestamp" * INTERVAL '1 second') "date",
        tx."recipientId" "addr"
      FROM trs tx)
      ),
    addr_stats AS (SELECT
      addr."date" "date",
      COUNT(DISTINCT addr."addr") "addr_cnt"
      FROM addrs addr
      GROUP BY "date"
      )
    SELECT
      blocks_stats."date" "date",
      blocks_stats."cnt" "cnt",
      blocks_stats."value" "value",
      blocks_stats."fees" "fees",
      tx_stats."med_value" "med_value",
      tx_stats."med_fees" "med_fees",
      tx_stats."from_cnt" "from_cnt",
      tx_stats."to_cnt" "to_cnt",
      addr_stats."addr_cnt" "addr_cnt"
    FROM blocks_stats
    LEFT JOIN tx_stats ON blocks_stats."date" = tx_stats."date"
    LEFT JOIN addr_stats ON blocks_stats."date" = addr_stats."date"
    ORDER BY blocks_stats."date"
  ) WITH NO DATA;
