-- -------------------------------------------------------------------------------------
drop materialized view if exists mv_most_expensive_articles;
-- --------------------------------------------------------------------------------------
create materialized view mv_most_expensive_articles as 
    with cte_ranked_articles as (
        select
            t.part_date,
            t.customer_id,
            t.article_id,
            row_number() over (
                partition by t.part_date, t.customer_id 
                order by t.price desc, t.transaction_date
            ) as price_rank
        from
            v_transactions_part_date t
    )
    select
        r.part_date,
        r.customer_id,
        r.article_id   
    from
        cte_ranked_articles r
    where
        r.price_rank = 1;
-- -------------------------------------------------------------------------------------