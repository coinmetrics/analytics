CREATE MATERIALIZED VIEW stellar_payment_stats_prep AS (
  SELECT
  SUBSTRING("date"::TEXT FOR 10) "date",
  SUM("cnt") FILTER (WHERE "asset" = '') "txCount(XRP)",
  SUM("value") FILTER (WHERE "asset" = '') "txVolume(XRP)",
  SUM("fees") FILTER (WHERE "asset" = '') "fees(XRP)",
  SUM("cnt") FILTER (WHERE "asset" = 'ETH') "txCount(ETH)",
  SUM("value") FILTER (WHERE "asset" = 'ETH') "txVolume(ETH)",
  SUM("fees") FILTER (WHERE "asset" = 'ETH') "fees(ETH)",
  SUM("cnt") FILTER (WHERE "asset" = 'BTC') "txCount(BTC)",
  SUM("value") FILTER (WHERE "asset" = 'BTC') "txVolume(BTC)",
  SUM("fees") FILTER (WHERE "asset" = 'BTC') "fees(BTC)"
  FROM stellar_payment_stats GROUP BY "date" ORDER BY "date"
  ) WITH NO DATA;
