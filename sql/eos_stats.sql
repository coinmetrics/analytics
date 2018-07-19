CREATE MATERIALIZED VIEW eos_stats AS (
	WITH
		actions AS (
			SELECT
				DATE_TRUNC('day', TO_TIMESTAMP(block."timestamp")) "date",
				SUBSTRING(action."data" FOR 8) "from",
				SUBSTRING(action."data" FROM 9 FOR 8) "to",
				-- ENCODE(TRIM(E'\\x00'::BYTEA FROM SUBSTRING(action."data" FROM 26 FOR 7)), 'escape') "asset",
				reverse_bit64(('X' || ENCODE(SUBSTRING(action."data" FROM 17 FOR 8), 'hex'))::BIT(64))::BIGINT::NUMERIC "value"
			FROM eos block, UNNEST(block.transactions) tx, UNNEST(tx.actions) action
			WHERE action."name" = 'transfer'
			AND ENCODE(TRIM(E'\\x00'::BYTEA FROM SUBSTRING(action."data" FROM 26 FOR 7)), 'escape') = 'EOS'
		),
		tx_stats AS (
			SELECT
				DATE_TRUNC('day', TO_TIMESTAMP(block."timestamp")) "date",
				COUNT(*) "cnt"
			FROM eos block, UNNEST(block.transactions) tx
			GROUP BY "date"
		),
		action_stats AS (
			SELECT
				action."date" "date",
				COUNT(*) "cnt",
				SUM(action."value" * 0.0001) "value",
				PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY action."value" * 0.0001) "med_value",
				COUNT(DISTINCT action."from") "from_cnt",
				COUNT(DISTINCT action."to") "to_cnt"
			FROM actions action
			GROUP BY action."date"
		),
		addr_stats AS (
			SELECT
				"date",
				COUNT(DISTINCT "addr") "cnt"
			FROM
				(
					(SELECT "date", "from" "addr" FROM actions)
					UNION ALL
					(SELECT "date", "to" "addr" FROM actions)
				) t
			GROUP BY "date"
		),
		block_stats AS (
			SELECT
				DATE_TRUNC('day', TO_TIMESTAMP(block."timestamp")) "date",
				COUNT(*) "cnt"
			FROM eos block
			GROUP BY "date"
		)
	SELECT
		block."date" "date",
		block."cnt" "block_cnt",
		tx."cnt" "tx_cnt",
		action."cnt" "action_cnt",
		action."value" "value",
		action."med_value" "med_value",
		action."from_cnt" "from_cnt",
		action."to_cnt" "to_cnt",
		addr."cnt" "addr_cnt"
	FROM block_stats block
	LEFT JOIN tx_stats tx ON block."date" = tx."date"
	LEFT JOIN action_stats action ON block."date" = action."date"
	LEFT JOIN addr_stats addr ON block."date" = addr."date"
	ORDER BY block."date"
) WITH NO DATA
;
