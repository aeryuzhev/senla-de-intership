-- -------------------------------------------------------------------------------------
drop view if exists v_prior_months_transactions;
-- --------------------------------------------------------------------------------------
create view v_prior_months_transactions as 
    select
        t.customer_id,
        count(distinct extract('month' from t.t_dat)) as prior_months_count
    from
        stg_transactions t
    where        
        t.t_dat >= (select prev_date_start from v_transactions_params)        
        and t.t_dat <= (select prev_date_end from v_transactions_params)
    group by
        t.customer_id; 
-- -------------------------------------------------------------------------------------