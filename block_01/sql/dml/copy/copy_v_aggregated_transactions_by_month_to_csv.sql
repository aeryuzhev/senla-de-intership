-- -------------------------------------------------------------------------------------
copy (
    select
        (t.part_date + interval '1 month - 1 day')::date as part_date,
        t.customer_id,
        case 
            when c.age < 23 then 'S'
            when c.age < 60 then 'A'
            else 'R'
        end as customer_group_by_age, 
        t.transaction_amount,
        a.article_id as most_exp_article_id,
        t.number_of_articles,
        t.number_of_product_groups,
        d.month_decade as most_active_decade,
        case 
            when p.prior_months_count = 2 
                then 1        
            when t.part_date = m.min_part_date 
                then 1
            when (t.part_date - interval '1 month')::date = m.min_part_date
                then coalesce(p.prior_months_count, 0)
            else 0
        end as customer_loyalty
    from
        v_aggregated_transactions_by_month t
        cross join v_min_part_date m            
        join v_ranked_articles a on a.customer_id = t.customer_id
        join v_ranked_month_decades d on d.customer_id = t.customer_id
        join stg_customers c on c.customer_id = t.customer_id
        left join v_prior_months_transactions p on p.customer_id = t.customer_id        
) to '/home/aerik/learning/code/senla-de-intership/block_01/data/data_mart.csv' csv header;
-- -------------------------------------------------------------------------------------