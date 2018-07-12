CREATE MATERIALIZED VIEW waves_stats AS (
	WITH
		txs AS (
			SELECT
				DATE_TRUNC('day', TO_TIMESTAMP(tx.timestamp * 0.001)) "date",
				tx."fee" * 0.00000001 :: NUMERIC "fees"
			FROM waves block, UNNEST(block.transactions) tx
			WHERE (tx."type" = 4 OR tx."type" = 11) AND tx."assetId" IS NULL
		),
		transfers AS (
			SELECT
				t."date" "date",
				t."from" "from",
				t."to" "to",
				t."value" "value"
			FROM (
				(
					SELECT
						DATE_TRUNC('day', TO_TIMESTAMP(tx.timestamp * 0.001)) "date",
						tx."sender" "from",
						tx."recipient" "to",
						tx."amount" * 0.00000001 :: NUMERIC "value"
					FROM waves block, UNNEST(block.transactions) tx
					WHERE tx."type" = 4 AND tx."assetId" IS NULL
				)
				UNION ALL
				(
					SELECT
						DATE_TRUNC('day', TO_TIMESTAMP(tx.timestamp * 0.001)) "date",
						tx."sender" "from",
						transfer."recipient" "to",
						transfer."amount" * 0.00000001 :: NUMERIC "value"
					FROM waves block, UNNEST(block.transactions) tx, UNNEST(tx.transfers) transfer
					WHERE tx."type" = 11 AND tx."assetId" IS NULL
				)
			) t
		),
		tx_stats AS (
			SELECT
				"date",
				COUNT(*) "cnt",
				SUM("fees") "fees",
				PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY "fees") "med_fees"
			FROM txs
			GROUP BY "date"
		),
		transfer_stats AS (
			SELECT
				"date",
				SUM("value") "value",
				PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY "value") "med_value",
				COUNT(DISTINCT "from") "from_cnt",
				COUNT(DISTINCT "to") "to_cnt"
			FROM transfers
			GROUP BY "date"
		),
		addr_stats AS (
			SELECT
				"date",
				COUNT(DISTINCT "addr") "cnt"
			FROM
				(
					(SELECT "date", "from" "addr" FROM transfers)
					UNION ALL
					(SELECT "date", "to" "addr" FROM transfers)
				) t
			GROUP BY "date"
		)
	SELECT
		tx_stats."date" "date",
		tx_stats."cnt" "cnt",
		transfer_stats."value" "value",
		transfer_stats."med_value" "med_value",
		tx_stats."fees" "fees",
		tx_stats."med_fees" "med_fees",
		transfer_stats."from_cnt" "from_cnt",
		transfer_stats."to_cnt" "to_cnt",
		addr_stats."cnt" "addr_cnt"
	FROM tx_stats
	LEFT JOIN transfer_stats ON tx_stats."date" = transfer_stats."date"
	LEFT JOIN addr_stats ON tx_stats."date" = addr_stats."date"
	ORDER BY "date"
) WITH NO DATA
;
