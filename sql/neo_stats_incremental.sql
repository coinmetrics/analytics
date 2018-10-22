CREATE TABLE IF NOT EXISTS neo_stats (
  "date" TIMESTAMP NOT NULL,
  "asset" BYTEA,
  "cnt" BIGINT,
  "fees" NUMERIC,
  "med_fees" DOUBLE PRECISION,
  "value" NUMERIC,
  "med_value" DOUBLE PRECISION,
  "from_cnt" BIGINT,
  "to_cnt" BIGINT,
  "addr_cnt" BIGINT
);

-- better to have the following index:
-- CREATE INDEX ON neo (DATE_TRUNC('day', TIMESTAMP 'epoch' + "time" * INTERVAL '1 second'));

CREATE TEMPORARY TABLE neo_txo (
  "date" TIMESTAMP NOT NULL,
  "txid_voutnum" TEXT NOT NULL,
  "txid" BYTEA NOT NULL,
  "asset" BYTEA NOT NULL,
  "value" NUMERIC NOT NULL,
  "address" TEXT NOT NULL
);

INSERT INTO neo_txo SELECT
  DATE_TRUNC('day', TIMESTAMP 'epoch' + block."time" * INTERVAL '1 second') "date",
  tx."txid" || ((vout."ordinality" - 1) :: TEXT) "txid_voutnum",
  tx."txid" "txid",
  vout."asset" "asset",
  vout."value" "value",
  vout."address" "address"
  FROM neo block, UNNEST(block.tx) tx, UNNEST(tx.vout) WITH ORDINALITY vout
;

CREATE INDEX ON neo_txo USING hash ("txid_voutnum");
CREATE INDEX ON neo_txo("date");

ANALYZE neo_txo;

DO $$DECLARE
  begin_day TIMESTAMP;
  cur_day TIMESTAMP;
BEGIN
  SELECT COALESCE(MAX(stat."date"), TO_TIMESTAMP(0)) INTO STRICT begin_day FROM neo_stats stat;
  DELETE FROM neo_stats WHERE "date" >= begin_day;
  FOR cur_day IN SELECT
    DATE_TRUNC('day', TIMESTAMP 'epoch' + block."time" * INTERVAL '1 second') "date"
    FROM neo block
    WHERE DATE_TRUNC('day', TIMESTAMP 'epoch' + block."time" * INTERVAL '1 second') >= begin_day
    GROUP BY "date" ORDER BY "date" LOOP
      RAISE NOTICE 'calculating %', cur_day;
      WITH
        vins AS (SELECT
          DATE_TRUNC('day', TIMESTAMP 'epoch' + block."time" * INTERVAL '1 second') "date",
          tx."txid" "txid",
          vin."txid" "vout_txid",
          vin."vout" "vout_num"
          FROM neo block, UNNEST(block.tx) tx, UNNEST(tx.vin) vin
          WHERE DATE_TRUNC('day', TIMESTAMP 'epoch' + block."time" * INTERVAL '1 second') = cur_day
          ),
        txi AS (SELECT
          vin."date" "date",
          vin."txid" "txid",
          txo."asset" "asset",
          -txo."value" "value",
          txo."address" "address"
          FROM vins vin INNER JOIN neo_txo txo ON vin."vout_txid" || (vin."vout_num" :: TEXT) = txo."txid_voutnum"
          ),
        txio AS (SELECT
          "date",
          "txid",
          "asset",
          GREATEST(SUM("value"), 0) "value"
          FROM (
            SELECT txi."date", txi."txid", txi."asset" "asset", txi."value" "value", txi."address" "address"
            FROM txi
            UNION ALL
            SELECT txo."date", txo."txid", txo."asset" "asset", txo."value" "value", txo."address" "address"
            FROM neo_txo txo
            WHERE txo."date" = cur_day
            ) t
          GROUP BY "date", "txid", "asset", "address"
          ),
        blocks_stats AS (SELECT
          txio."date" "date",
          txio."asset" "asset",
          COUNT(*) "cnt",
          SUM(tx."sys_fee" + tx."net_fee") "fees",
          PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ((tx."sys_fee" + tx."net_fee") :: DOUBLE PRECISION)) "med_fees",
          SUM(txio."value") "value",
          PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (txio."value" :: DOUBLE PRECISION)) "med_value"
          FROM neo block, UNNEST(block.tx) tx INNER JOIN txio ON tx."txid" = txio."txid"
          WHERE DATE_TRUNC('day', TIMESTAMP 'epoch' + block."time" * INTERVAL '1 second') = cur_day
          GROUP BY txio."date", txio."asset"
          ),
        in_addr_stats AS (SELECT
          txi."date" "date",
          txi."asset" "asset",
          COUNT(DISTINCT txi."address") "from_cnt"
          FROM txi
          GROUP BY txi."date", txi."asset"
          ),
        out_addr_stats AS (SELECT
          txo."date" "date",
          txo."asset" "asset",
          COUNT(DISTINCT txo."address") "to_cnt"
          FROM neo_txo txo
          WHERE txo."date" = cur_day
          GROUP BY txo."date", txo."asset"
          ),
        addrs AS (
          (SELECT txi."date" "date", txi."address" "addr", txi."asset" "asset" FROM txi)
          UNION ALL
          (SELECT txo."date" "date", txo."address" "addr", txo."asset" "asset" FROM neo_txo txo WHERE txo."date" = cur_day)
          ),
        addr_stats AS (SELECT
          addr."date" "date",
          addr."asset" "asset",
          COUNT(DISTINCT addr."addr") "addr_cnt"
          FROM addrs addr
          GROUP BY addr."date", addr."asset"
          )
        INSERT INTO neo_stats SELECT
          blocks_stats."date" "date",
          blocks_stats."asset" "asset",
          blocks_stats."cnt" "cnt",
          blocks_stats."fees" "fees",
          blocks_stats."med_fees" "med_fees",
          blocks_stats."value" "value",
          blocks_stats."med_value" "med_value",
          in_addr_stats."from_cnt" "from_cnt",
          out_addr_stats."to_cnt" "to_cnt",
          addr_stats."addr_cnt" "addr_cnt"
        FROM blocks_stats
        INNER JOIN in_addr_stats ON blocks_stats."date" = in_addr_stats."date" AND blocks_stats."asset" = in_addr_stats."asset"
        INNER JOIN out_addr_stats ON blocks_stats."date" = out_addr_stats."date" AND blocks_stats."asset" = out_addr_stats."asset"
        INNER JOIN addr_stats ON blocks_stats."date" = addr_stats."date" AND blocks_stats."asset" = addr_stats."asset"
        ;
  END LOOP;
END$$ LANGUAGE PLPGSQL;
