-- -------------------------------------------------------------------------------------
drop view if exists v_ranked_articles;
-- --------------------------------------------------------------------------------------
create view v_ranked_articles as 
    with cte_ranked_articles as (
        select
            t.customer_id,
            t.article_id,
            row_number() over (
                partition by t.customer_id 
                order by t.price desc, t.transaction_date
            ) as price_rank
        from
            mv_transactions_part_date t
    )
    select
        r.customer_id,
        r.article_id   
    from
        cte_ranked_articles r
    where
        r.price_rank = 1;
-- -------------------------------------------------------------------------------------