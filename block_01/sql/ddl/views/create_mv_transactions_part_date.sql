-- -------------------------------------------------------------------------------------
drop materialized view if exists mv_transactions_part_date;
-- --------------------------------------------------------------------------------------
create materialized view mv_transactions_part_date as
    select      
        t.t_dat as transaction_date,
        t.customer_id,
        t.article_id,
        t.price,
        date_trunc('month', t.t_dat)::date as part_date,
        case    
            when extract('day' from t.t_dat) <= 10 then 1
            when extract('day' from t.t_dat) <= 20 then 2
            when extract('day' from t.t_dat) <= 31 then 3
        end as month_decade
    from
        stg_transactions t
    where       
        t.t_dat >= (select date_start from v_transactions_params)        
        and t.t_dat <= (select date_end from v_transactions_params);  
-- -------------------------------------------------------------------------------------