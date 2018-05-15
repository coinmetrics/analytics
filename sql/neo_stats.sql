CREATE MATERIALIZED VIEW neo_stats AS (
  WITH
    txo AS (SELECT
      block."index" blk,
      tx."txid" "txid",
      vout."ordinality" - 1 "voutnum",
      vout."asset" "asset",
      vout."value" "value",
      vout."address" "address"
      FROM neo block, UNNEST(block.tx) tx, UNNEST(tx.vout) WITH ORDINALITY vout
      ),
    txi AS (SELECT
      block."index" blk,
      tx."txid" "txid",
      txo."asset" "asset",
      -txo."value" "value",
      txo."address" "address"
      FROM neo block, UNNEST(block.tx) tx, UNNEST(tx.vin) vin INNER JOIN txo
      ON vin."txid" = txo."txid" AND vin."vout" = txo."voutnum"
      ),
    txio AS (SELECT
      "txid",
      "asset",
      GREATEST(SUM("value"), 0) "value"
      FROM (
        SELECT txi."blk", txi."txid", txi."asset" "asset", txi."value" "value", txi."address" "address"
        FROM txi
        UNION ALL
        SELECT txo."blk", txo."txid", txo."asset" "asset", txo."value" "value", txo."address" "address"
        FROM txo
        ) t
      GROUP BY "blk", "txid", "asset", "address"
      ),
    blocks_stats AS (SELECT
      DATE_TRUNC('day', TO_TIMESTAMP(block."time")) "date",
      txio."asset" "asset",
      COUNT(*) "cnt",
      SUM(tx."sys_fee" + tx."net_fee") "fees",
      SUM(txio."value") "value"
      FROM neo block, UNNEST(block.tx) tx INNER JOIN txio ON tx."txid" = txio."txid"
      GROUP BY "date", "asset" ORDER BY "date", "asset"
      ),
    in_addr_stats AS (SELECT
      DATE_TRUNC('day', TO_TIMESTAMP(block."time")) "date",
      txi."asset" "asset",
      COUNT(DISTINCT txi."address") "from_cnt"
      FROM neo block
      INNER JOIN txi ON block."index" = txi."blk"
      GROUP BY "date", txi."asset"
      ),
    out_addr_stats AS (SELECT
      DATE_TRUNC('day', TO_TIMESTAMP(block."time")) "date",
      txo."asset" "asset",
      COUNT(DISTINCT txo."address") "to_cnt"
      FROM neo block
      INNER JOIN txo ON block."index" = txo."blk"
      GROUP BY "date", txo."asset"
      ),
    addrs AS (
      (SELECT txi."blk" "blk", txi."address" "addr", txi."asset" "asset" FROM txi)
      UNION ALL
      (SELECT txo."blk" "blk", txo."address" "addr", txo."asset" "asset" FROM txo)
      ),
    addr_stats AS (SELECT
      DATE_TRUNC('day', TO_TIMESTAMP(block."time")) "date",
      addr."asset" "asset",
      COUNT(DISTINCT addr."addr") "addr_cnt"
      FROM neo block INNER JOIN addrs addr ON block."index" = addr."blk"
      GROUP BY "date", addr."asset"
      )
    SELECT
      blocks_stats."date" "date",
      blocks_stats."asset" "asset",
      blocks_stats."cnt" "cnt",
      blocks_stats."fees" "fees",
      blocks_stats."value" "value",
      in_addr_stats."from_cnt" "from_cnt",
      out_addr_stats."to_cnt" "to_cnt",
      addr_stats."addr_cnt" "addr_cnt"
    FROM blocks_stats
    INNER JOIN in_addr_stats ON blocks_stats."date" = in_addr_stats."date" AND blocks_stats."asset" = in_addr_stats."asset"
    INNER JOIN out_addr_stats ON blocks_stats."date" = out_addr_stats."date" AND blocks_stats."asset" = out_addr_stats."asset"
    INNER JOIN addr_stats ON blocks_stats."date" = addr_stats."date" AND blocks_stats."asset" = addr_stats."asset"
    ORDER BY blocks_stats."date", blocks_stats."asset"
  ) WITH NO DATA;
