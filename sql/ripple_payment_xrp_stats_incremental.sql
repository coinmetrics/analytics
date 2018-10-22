CREATE TABLE IF NOT EXISTS ripple_payment_xrp_stats (
  "date" TIMESTAMP NOT NULL,
  "cnt" BIGINT,
  "fee" NUMERIC,
  "med_fee" DOUBLE PRECISION,
  "from_cnt" BIGINT,
  "to_cnt" BIGINT,
  "addr_cnt" BIGINT,
  "payment_cnt" BIGINT,
  "payment_value" NUMERIC,
  "med_payment_value" DOUBLE PRECISION,
  "block_cnt" BIGINT,
  "missing_block_cnt" NUMERIC,
  "missing_cnt" BIGINT
);

-- better to have the following index:
-- CREATE INDEX ON ripple (DATE_TRUNC('day', TIMESTAMP 'epoch' + "closeTime" * INTERVAL '1 second'));

-- TEMP: clear stats
DELETE FROM ripple_payment_xrp_stats;

DO $$DECLARE
  begin_day TIMESTAMP;
  cur_day TIMESTAMP;
BEGIN
  SELECT COALESCE(MAX(stat."date"), TO_TIMESTAMP(0)) INTO STRICT begin_day FROM ripple_payment_xrp_stats stat;
  DELETE FROM ripple_payment_xrp_stats WHERE "date" >= begin_day;
  FOR cur_day IN SELECT
    DATE_TRUNC('day', TIMESTAMP 'epoch' + ledger."closeTime" * INTERVAL '1 second') "date"
    FROM ripple ledger
    WHERE DATE_TRUNC('day', TIMESTAMP 'epoch' + ledger."closeTime" * INTERVAL '1 second') >= begin_day
    GROUP BY "date" ORDER BY "date" LOOP
      RAISE NOTICE 'calculating %', cur_day;
      WITH
        txs AS (
          SELECT
            DATE_TRUNC('day', TO_TIMESTAMP(tx."date")) "date",
            tx."fee" * 0.000001 "fee",
            tx."account" "from",
            tx."destination" "to"
          FROM ripple ledger, UNNEST(ledger.transactions) tx
          WHERE DATE_TRUNC('day', TIMESTAMP 'epoch' + ledger."closeTime" * INTERVAL '1 second') = cur_day
          AND tx."result" = 'tesSUCCESS'
          ),
        payments AS (
          SELECT
            DATE_TRUNC('day', TO_TIMESTAMP(tx."date")) "date",
            tx."amount" * 0.000001 "amount"
          FROM ripple ledger, UNNEST(ledger.transactions) tx
          WHERE DATE_TRUNC('day', TIMESTAMP 'epoch' + ledger."closeTime" * INTERVAL '1 second') = cur_day
          AND tx."result" = 'tesSUCCESS' AND tx."type" = 'Payment' AND tx."currency" IS NULL AND tx."account" <> tx."destination"
          ),
        tx_stats AS (
          SELECT
            tx."date" "date",
            COUNT(*) "cnt",
            SUM(tx."fee") "fee",
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tx."fee") "med_fee",
            COUNT(DISTINCT tx."from") "from_cnt",
            COUNT(DISTINCT tx."to") "to_cnt"
          FROM txs tx
          GROUP BY tx."date"
          ),
        payment_stats AS (
          SELECT
            payment."date" "date",
            COUNT(*) "cnt",
            SUM(payment."amount") "value",
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY payment."amount") "med_value"
          FROM payments payment
          GROUP BY payment."date"
          ),
        missing_txs AS (
          SELECT
            DATE_TRUNC('day', TO_TIMESTAMP(ledger."closeTime")) "date1",
            COUNT(*) "cnt"
          FROM ripple ledger, UNNEST(ledger.transactions) tx
          WHERE DATE_TRUNC('day', TIMESTAMP 'epoch' + ledger."closeTime" * INTERVAL '1 second') = cur_day
          AND tx."type" IS NULL
          GROUP BY "date1"
          ),
        addr_stats AS (SELECT
          t."date" "date",
          COUNT(DISTINCT t."addr") "cnt"
          FROM (
            SELECT
              tx."date" "date",
              tx."from" "addr"
              FROM txs tx
            UNION ALL
            SELECT
              tx."date" "date",
              tx."to" "addr"
              FROM txs tx
            ) t
          GROUP BY t."date"
          ),
        blocks_stats AS (
          SELECT
            DATE_TRUNC('day', TO_TIMESTAMP(ledger."closeTime")) "date1",
            COUNT(*) "cnt",
            MAX(ledger."index") - MIN(ledger."index") + 1 - COUNT(*) "missing_cnt"
          FROM ripple ledger
          WHERE DATE_TRUNC('day', TIMESTAMP 'epoch' + ledger."closeTime" * INTERVAL '1 second') = cur_day
          GROUP BY "date1"
          )
        INSERT INTO ripple_payment_xrp_stats SELECT
          tx_stats."date" "date",
          tx_stats."cnt" "cnt",
          tx_stats."fee" "fee",
          tx_stats."med_fee" "med_fee",
          tx_stats."from_cnt" "from_cnt",
          tx_stats."to_cnt" "to_cnt",
          addr_stats."cnt" "addr_cnt",
          payment_stats."cnt" "payment_cnt",
          payment_stats."value" "payment_value",
          payment_stats."med_value" "med_payment_value",
          blocks_stats."cnt" "block_cnt",
          blocks_stats."missing_cnt" "missing_block_cnt",
          missing_txs."cnt" "missing_cnt"
        FROM tx_stats
        LEFT JOIN payment_stats ON tx_stats."date" = payment_stats."date"
        LEFT JOIN addr_stats ON tx_stats."date" = addr_stats."date"
        LEFT JOIN blocks_stats ON tx_stats."date" = blocks_stats."date1"
        LEFT JOIN missing_txs ON tx_stats."date" = missing_txs."date1"
        ;
  END LOOP;
END$$ LANGUAGE PLPGSQL;
