BEGIN;

CREATE TABLE IF NOT EXISTS ethereum_stats_traced (
  "date" TIMESTAMP NOT NULL,
  "tx_cnt" BIGINT,
  "action_cnt" BIGINT,
  "sum_value" NUMERIC,
  "med_value" DOUBLE PRECISION,
  "sum_action_value" NUMERIC,
  "med_action_value" DOUBLE PRECISION,
  "avg_difficulty" NUMERIC,
  "avg_tx_size" NUMERIC,
  "sum_fee" NUMERIC,
  "block_cnt" BIGINT,
  "sum_size" NUMERIC,
  "med_fee" DOUBLE PRECISION,
  "reward" DOUBLE PRECISION,
  "from_cnt" BIGINT,
  "to_cnt" BIGINT,
  "addr_cnt" BIGINT,
  "action_from_cnt" BIGINT,
  "action_to_cnt" BIGINT,
  "addr_action_cnt" BIGINT,
  "payment_cnt" BIGINT,
  "contract_cnt" BIGINT,
  "create_contract_cnt" BIGINT,
  "payment_value" NUMERIC,
  "contract_value" NUMERIC,
  "create_contract_value" NUMERIC,
  "short_cnt" BIGINT,
  "short_value" NUMERIC
);

-- better to have the following index:
-- CREATE INDEX ON ethereum (DATE_TRUNC('day', TIMESTAMP 'epoch' + "timestamp" * INTERVAL '1 second'));

DO $$DECLARE
  begin_day TIMESTAMP;
  cur_day TIMESTAMP;
BEGIN
  SELECT COALESCE(MAX(stat."date"), TO_TIMESTAMP(0)) INTO STRICT begin_day FROM ethereum_stats_traced stat;
  DELETE FROM ethereum_stats_traced WHERE "date" >= begin_day;
  FOR cur_day IN SELECT
    DATE_TRUNC('day', TIMESTAMP 'epoch' + block."timestamp" * INTERVAL '1 second') "date"
    FROM ethereum block
    WHERE DATE_TRUNC('day', TIMESTAMP 'epoch' + block."timestamp" * INTERVAL '1 second') >= begin_day
    GROUP BY "date" ORDER BY "date" LOOP
      RAISE NOTICE 'calculating %', cur_day;
      WITH
        ethereum_tx AS (
          SELECT
            block."timestamp" "time",
            DATE_TRUNC('day', TIMESTAMP 'epoch' + block."timestamp" * INTERVAL '1 second') "date",
            tx."from" "from",
            tx."to" "to",
            tx."value" * 1e-18 "value",
            (tx."gasUsed" * tx."gasPrice") * 1e-18 "fee"
          FROM ethereum block, UNNEST(block.transactions) tx
          WHERE DATE_TRUNC('day', TIMESTAMP 'epoch' + block."timestamp" * INTERVAL '1 second') = cur_day
          ),
        ethereum_actions AS (
          SELECT
            block."timestamp" "time",
            DATE_TRUNC('day', TIMESTAMP 'epoch' + block."timestamp" * INTERVAL '1 second') "date",
            action."from" "from",
            action."to" "to",
            action."value" * 1e-18 "value"
          FROM ethereum block, UNNEST(block.transactions) tx, UNNEST(tx.actions) action
          WHERE DATE_TRUNC('day', TIMESTAMP 'epoch' + block."timestamp" * INTERVAL '1 second') = cur_day AND action."accounted"
          ),
        txs_stats AS (
          SELECT
            tx."date" "date",
            COUNT(*) "cnt",
            SUM(tx."value") "sum_value",
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tx."value") "med_value",
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tx."fee") "med_fee",
            SUM(tx."fee") "sum_fee",
            COUNT(DISTINCT tx."from") "from_cnt",
            COUNT(DISTINCT tx."to") "to_cnt"
          FROM ethereum_tx tx GROUP BY tx."date"
          ),
        actions_stats AS (
          SELECT
            action."date" "date",
            COUNT(*) "cnt",
            SUM(action."value") "sum_value",
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY action."value") "med_value",
            COUNT(DISTINCT action."from") "from_cnt",
            COUNT(DISTINCT action."to") "to_cnt"
          FROM ethereum_actions action GROUP BY action."date"
          ),
        addr_stats AS (
          SELECT
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
          GROUP BY "date"
          ),
        addr_actions_stats AS (
          SELECT
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
          GROUP BY "date"
          ),
        blocks AS (
          SELECT
            DATE_TRUNC('day', TIMESTAMP 'epoch' + block."timestamp" * INTERVAL '1 second') "date",
            CASE WHEN block."number" < 4370000 THEN 5 ELSE 3 END "reward",
            block."number" "number",
            block."difficulty" "difficulty",
            block."size" "size",
            ARRAY_LENGTH(block."transactions", 1) "tx_cnt",
            block."uncles" "uncles"
          FROM ethereum block
          WHERE DATE_TRUNC('day', TIMESTAMP 'epoch' + block."timestamp" * INTERVAL '1 second') = cur_day
          ),
        blocks_stats AS (
          SELECT
            block."date" "date",
            AVG(block."difficulty") "avg_difficulty",
            SUM(block."size") / SUM(block."tx_cnt") "avg_tx_size",
            COUNT(*) "cnt",
            SUM(block."size") "sum_size",
            SUM(block."reward") "reward"
          FROM blocks block GROUP BY block."date"
          ),
        uncles_stats AS (
          SELECT
            block."date" "date",
            SUM((((uncle."number" - block."number") :: NUMERIC) * 0.125 + 1.03125) * block."reward") "reward"
          FROM blocks block, UNNEST(block."uncles") uncle GROUP BY block."date"
          ),
        payments_stats AS (
          SELECT
            tx."date" "date",
            SUM(CASE WHEN tx."to" IS NOT NULL AND contract."address" IS     NULL THEN 1 ELSE 0 END) "payment_cnt",
            SUM(CASE WHEN tx."to" IS NOT NULL AND contract."address" IS NOT NULL THEN 1 ELSE 0 END) "contract_cnt",
            SUM(CASE WHEN tx."to" IS     NULL                                    THEN 1 ELSE 0 END) "create_contract_cnt",
            SUM(CASE WHEN tx."to" IS NOT NULL AND contract."address" IS     NULL THEN tx."value" ELSE 0 END) "payment_value",
            SUM(CASE WHEN tx."to" IS NOT NULL AND contract."address" IS NOT NULL THEN tx."value" ELSE 0 END) "contract_value",
            SUM(CASE WHEN tx."to" IS     NULL                                    THEN tx."value" ELSE 0 END) "create_contract_value"
          FROM ethereum_tx tx LEFT JOIN ethereum_contracts contract ON tx."to" = contract."address"
          GROUP BY tx."date"
          ),
        short_stats AS (
          SELECT
            addr."min_date" "date",
            SUM(addr."cnt") "cnt",
            SUM(addr."positive_value") "value"
          FROM ethereum_short_addrs addr
          WHERE addr."min_date" = cur_day
          GROUP BY addr."min_date"
          )
        INSERT INTO ethereum_stats_traced SELECT
          block."date" "date",
          tx."cnt" "tx_cnt",
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
          addr_action."cnt" "addr_action_cnt",
          payment."payment_cnt" "payment_cnt",
          payment."contract_cnt" "contract_cnt",
          payment."create_contract_cnt" "create_contract_cnt",
          payment."payment_value" "payment_value",
          payment."contract_value" "contract_value",
          payment."create_contract_value" "create_contract_value",
          short."cnt" "short_cnt",
          short."value" "short_value"
        FROM blocks_stats block
        LEFT JOIN uncles_stats uncle ON block."date" = uncle."date"
        LEFT JOIN txs_stats tx ON block."date" = tx."date"
        LEFT JOIN addr_stats addr ON block."date" = addr."date"
        LEFT JOIN actions_stats action ON block."date" = action."date"
        LEFT JOIN addr_actions_stats addr_action ON block."date" = addr_action."date"
        LEFT JOIN payments_stats payment ON block."date" = payment."date"
        LEFT JOIN short_stats short ON block."date" = short."date"
        ;
  END LOOP;
END$$ LANGUAGE PLPGSQL;

COMMIT;
