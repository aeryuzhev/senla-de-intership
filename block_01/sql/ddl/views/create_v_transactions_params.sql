-- -------------------------------------------------------------------------------------
drop view if exists v_transactions_params;
-- --------------------------------------------------------------------------------------
create view v_transactions_params as 
    select
        -- The first day of the month from part_date param.
        date_trunc('month', (:part_date)::date)::date as date_start, 
        -- The last day of the month from part_date param.
        (date_trunc('month', (:part_date)::date) + interval '1 month - 1 day')::date as date_end,
        -- The first day of the month which is obtained after substracting 2 months from the part_date param.
        (date_trunc('month', (:part_date)::date) - interval '2 month')::date as prev_date_start,
        -- The last day of the previous month before part_date param.
        (date_trunc('month', (:part_date)::date) - interval '1 day')::date as prev_date_end;
-- -------------------------------------------------------------------------------------