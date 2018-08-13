CREATE MATERIALIZED VIEW stellar_stats AS (
  WITH
    tx_stats AS (SELECT
      DATE_TRUNC('day', TO_TIMESTAMP(ledger."closeTime")) "date",
      COUNT(*) "cnt",
      SUM(tx."fee" :: NUMERIC * 0.0000001) "fees"
      FROM stellar ledger, UNNEST(ledger.transactions) tx
      GROUP BY "date"
      ),
    payment_stats AS (SELECT
      "date" "date",
      SUM("cnt") FILTER (WHERE "asset" = '') "paymentTxCount(XRP)",
      SUM("value") FILTER (WHERE "asset" = '') "paymentTxVolume(XRP)",
      SUM("fees") FILTER (WHERE "asset" = '') "paymentFees(XRP)",
      SUM("cnt") FILTER (WHERE "asset" = 'ETH') "paymentTxCount(ETH)",
      SUM("value") FILTER (WHERE "asset" = 'ETH') "paymentTxVolume(ETH)",
      SUM("fees") FILTER (WHERE "asset" = 'ETH') "paymentFees(ETH)",
      SUM("cnt") FILTER (WHERE "asset" = 'BTC') "paymentTxCount(BTC)",
      SUM("value") FILTER (WHERE "asset" = 'BTC') "paymentTxVolume(BTC)",
      SUM("fees") FILTER (WHERE "asset" = 'BTC') "paymentFees(BTC)"
      FROM stellar_payment_stats GROUP BY "date" ORDER BY "date"
      )
    SELECT
      SUBSTRING(tx_stats."date"::TEXT FOR 10) "date",
      tx_stats."cnt" "txCount",
      tx_stats."fees" "fees",
      payment_stats."paymentTxCount(XRP)" "paymentTxCount(XRP)",
      payment_stats."paymentTxVolume(XRP)" "paymentTxVolume(XRP)",
      payment_stats."paymentFees(XRP)" "paymentFees(XRP)",
      payment_stats."paymentTxCount(ETH)" "paymentTxCount(ETH)",
      payment_stats."paymentTxVolume(ETH)" "paymentTxVolume(ETH)",
      payment_stats."paymentFees(ETH)" "paymentFees(ETH)",
      payment_stats."paymentTxCount(BTC)" "paymentTxCount(BTC)",
      payment_stats."paymentTxVolume(BTC)" "paymentTxVolume(BTC)",
      payment_stats."paymentFees(BTC)" "paymentFees(BTC)"
    FROM tx_stats LEFT JOIN payment_stats ON tx_stats."date" = payment_stats."date"
  ) WITH NO DATA;
