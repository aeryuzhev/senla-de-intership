-- -------------------------------------------------------------------------------------
copy (
    select
        t.part_date,
        t.customer_id,
        t.customer_group_by_age, 
        t.transaction_amount,
        t.most_exp_article_id,
        t.number_of_articles,
        t.number_of_product_groups,
        t.most_active_decade,
        t.customer_loyalty      
    from
        dm_transactions_by_month t
    where
        t.part_date = (:part_date)::date
) to '/home/aerik/learning/code/senla-de-intership/block_01/data/data_mart.csv' csv header;
-- -------------------------------------------------------------------------------------