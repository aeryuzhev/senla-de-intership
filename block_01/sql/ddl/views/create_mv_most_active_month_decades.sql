-- -------------------------------------------------------------------------------------
drop materialized view if exists mv_most_active_month_decades;
-- -------------------------------------------------------------------------------------
create materialized view mv_most_active_month_decades as 
    with cte_aggregated_transactions_by_month_decade as (
        select
            t.part_date,
            t.month_decade,
            t.customer_id,
            sum(coalesce(t.price, 0)) as transaction_amount
        from
            v_transactions_part_date t
        group by
            t.part_date,
            t.month_decade,
            t.customer_id    
    ),
    cte_ranked_month_decades as (
        select
            t.part_date,
            t.month_decade,
            t.customer_id,
            row_number() over (
                partition by t.part_date, t.customer_id
                order by t.transaction_amount desc, t.month_decade
            ) as month_decade_rank      
        from
            cte_aggregated_transactions_by_month_decade t     
    )
    select
        r.part_date,
        r.month_decade,
        r.customer_id
    from
        cte_ranked_month_decades r   
    where
        month_decade_rank = 1;
-- -------------------------------------------------------------------------------------