-- -------------------------------------------------------------------------------------
drop view if exists v_aggregated_transactions_by_month;
-- --------------------------------------------------------------------------------------
create view v_aggregated_transactions_by_month as 
    select
        t.part_date,
        t.customer_id,
        sum(coalesce(t.price, 0)) as transaction_amount,
        count(t.article_id) as number_of_articles,
        count(distinct a.product_group_name) as number_of_product_groups  
    from
        mv_transactions_part_date t
        join stg_articles a on a.article_id = t.article_id
    group by
        t.part_date,
        t.customer_id;
-- -------------------------------------------------------------------------------------