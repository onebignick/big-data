-- BQ1 association analysis: laundering rate by group (train + test days; the tail is excluded).
CREATE OR REPLACE VIEW bda8_aml.v_rate_by_payment_format AS
SELECT payment_format, count(*) AS txns, sum(is_laundering) AS laundering,
       1e4 * avg(is_laundering) AS rate_bp
FROM bda8_aml.curated_transactions WHERE split <> 'tail' GROUP BY 1;
;;
CREATE OR REPLACE VIEW bda8_aml.v_rate_by_currency AS
SELECT payment_currency, count(*) AS txns, sum(is_laundering) AS laundering,
       1e4 * avg(is_laundering) AS rate_bp
FROM bda8_aml.curated_transactions WHERE split <> 'tail' GROUP BY 1;
;;
CREATE OR REPLACE VIEW bda8_aml.v_rate_by_format_currency AS
SELECT payment_format, payment_currency, count(*) AS txns, sum(is_laundering) AS laundering,
       1e4 * avg(is_laundering) AS rate_bp
FROM bda8_aml.curated_transactions WHERE split <> 'tail' GROUP BY 1, 2;
;;
CREATE OR REPLACE VIEW bda8_aml.v_rate_by_cross_flags AS
SELECT is_cross_bank, is_cross_currency, is_self_transfer, count(*) AS txns,
       sum(is_laundering) AS laundering, 1e4 * avg(is_laundering) AS rate_bp
FROM bda8_aml.curated_transactions WHERE split <> 'tail' GROUP BY 1, 2, 3;
;;
CREATE OR REPLACE VIEW bda8_aml.v_rate_by_hour AS
SELECT hour_of_day, count(*) AS txns, sum(is_laundering) AS laundering,
       1e4 * avg(is_laundering) AS rate_bp
FROM bda8_aml.curated_transactions WHERE split <> 'tail' GROUP BY 1;
;;
CREATE OR REPLACE VIEW bda8_aml.v_rate_by_bank_pair AS
SELECT from_bank, to_bank, count(*) AS txns, sum(is_laundering) AS laundering,
       1e4 * avg(is_laundering) AS rate_bp
FROM bda8_aml.curated_transactions WHERE split <> 'tail' AND from_bank <> to_bank
GROUP BY 1, 2 HAVING count(*) >= 1000;
