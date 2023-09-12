-- -------------------------------------------------------------------------------------
drop table if exists dm_transactions_by_month;
-- -------------------------------------------------------------------------------------
create table dm_transactions_by_month as 
    with cte_enriched_transactions as (
        select
            t.part_date,
            t.customer_id,
            case 
                when c.age < 23 then 'S'
                when c.age < 60 then 'A'
                else 'R'
            end as customer_group_by_age, 
            t.transaction_amount,
            count(t.customer_id) over (
                partition by t.customer_id 
                order by t.part_date 
                range between interval '2' month preceding and interval '1' month preceding
            ) as prior_months_count,
            min(t.part_date) over () as min_part_date,
            a.article_id as most_exp_article_id,
            t.number_of_articles,
            t.number_of_product_groups,
            d.month_decade as most_active_decade    
        from
            mv_aggregated_transactions_by_month t
            join stg_customers c on c.customer_id = t.customer_id
            join mv_most_expensive_articles a on a.customer_id = t.customer_id
                and a.part_date = t.part_date
            join mv_most_active_month_decades d on d.customer_id = t.customer_id
                and d.part_date = t.part_date
    )
    select
        (t.part_date + interval '1 month - 1 day')::date as part_date,
        t.customer_id,
        t.customer_group_by_age, 
        t.transaction_amount,
        t.most_exp_article_id,
        t.number_of_articles,
        t.number_of_product_groups,
        t.most_active_decade,
        case 
            when t.prior_months_count = 2 
                then 1        
            when t.part_date = t.min_part_date 
                then 1
            when (t.part_date - interval '1 month')::date = t.min_part_date
                then coalesce(t.prior_months_count, 0)
            else 0
        end as customer_loyalty  
    from
        cte_enriched_transactions t   
-- -------------------------------------------------------------------------------------