-- Lisk stats
-- genesis block time is 2016-05-24 20:00:00
CREATE MATERIALIZED VIEW lisk_stats AS (
  WITH
    tx_stats AS (SELECT
      DATE_TRUNC('day', TIMESTAMP '2016-05-24 20:00:00' + tx."timestamp" * INTERVAL '1 second') "date",
      COUNT(*) "cnt",
      SUM(tx."amount") :: NUMERIC * 0.00000001 "value",
      SUM(tx."fee") :: NUMERIC * 0.00000001 "fees",
      PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tx."amount" :: NUMERIC * 0.00000001) "med_value",
      PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tx."fee" :: NUMERIC * 0.00000001) "med_fees",
      COUNT(DISTINCT tx."senderId") "from_cnt",
      COUNT(DISTINCT tx."recipientId") "to_cnt",
      SUM(CASE WHEN tx."type" = 0 THEN 1 ELSE 0 END) "payment_cnt"
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
      tx_stats."date" "date",
      tx_stats."cnt" "cnt",
      tx_stats."value" "value",
      tx_stats."fees" "fees",
      tx_stats."med_value" "med_value",
      tx_stats."med_fees" "med_fees",
      tx_stats."from_cnt" "from_cnt",
      tx_stats."to_cnt" "to_cnt",
      tx_stats."payment_cnt" "payment_cnt",
      addr_stats."addr_cnt" "addr_cnt"
    FROM tx_stats
    LEFT JOIN addr_stats ON tx_stats."date" = addr_stats."date"
    ORDER BY tx_stats."date"
  ) WITH NO DATA;
