-- -------------------------------------------------------------------------------------
drop materialized view if exists mv_aggregated_transactions_by_month;
drop materialized view if exists mv_most_expensive_articles;
drop materialized view if exists mv_most_active_month_decades;
drop view if exists v_transactions_part_date_full;
-- -------------------------------------------------------------------------------------