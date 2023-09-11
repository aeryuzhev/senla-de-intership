-- -------------------------------------------------------------------------------------
drop materialized view if exists mv_aggregated_transactions_by_month;
drop materialized view if exists mv_most_expensive_articles;
drop materialized view if exists mv_most_active_month_decades;
drop view if exists v_transactions_part_date;
-- -------------------------------------------------------------------------------------
drop table if exists stg_articles;
-- -------------------------------------------------------------------------------------
create table stg_articles (
    article_id                    integer,
    product_code                  integer,
    prod_name                     text,
    product_type_no               smallint,
    product_type_name             text,
    product_group_name            text,
    graphical_appearance_no       integer,
    graphical_appearance_name     text,
    colour_group_code             smallint,
    colour_group_name             text,
    perceived_colour_value_id     smallint,
    perceived_colour_value_name   text,
    perceived_colour_master_id    smallint,
    perceived_colour_master_name  text,
    department_no                 smallint,
    department_name               text,
    index_code                    text, 
    index_name                    text,
    index_group_no                smallint,
    index_group_name              text, 
    section_no                    smallint,
    section_name                  text,  
    garment_group_no              smallint,
    garment_group_name            text,
    detail_desc                   text
);
-- -------------------------------------------------------------------------------------
drop table if exists stg_customers;
-- -------------------------------------------------------------------------------------
create table stg_customers (
    customer_id             text,
    fn                      numeric(2, 1),
    active                  numeric(2, 1),
    club_member_status      text,
    fashion_news_frequency  text,
    age                     smallint,
    postal_code             text
);
-- -------------------------------------------------------------------------------------
drop table if exists stg_transactions;
-- -------------------------------------------------------------------------------------
create table stg_transactions (
    t_dat             date,
    customer_id       text,
    article_id        integer,
    price             numeric(22, 20),
    sales_channel_id  smallint
);
-- -------------------------------------------------------------------------------------
copy stg_articles
from '/home/aerik/learning/code/senla-de-intership/block_01/data/articles.csv'
with (
    format csv,
    delimiter ',',
    header true
);
-- -------------------------------------------------------------------------------------
copy stg_customers
from '/home/aerik/learning/code/senla-de-intership/block_01/data/customers.csv'
with (
    format csv,
    delimiter ',',
    header true
);
-- -------------------------------------------------------------------------------------
copy stg_transactions
from '/home/aerik/learning/code/senla-de-intership/block_01/data/transactions_train.csv'
with (
    format csv,
    delimiter ',',
    header true
);
-- --------------------------------------------------------------------------------------
create view v_transactions_part_date as
    select      
        t.t_dat as transaction_date,
        t.customer_id,
        t.article_id,
        t.price,
        (date_trunc('month', t.t_dat) + interval '1 month - 1 day')::date as part_date,
        case    
            when extract('day' from t.t_dat) <= 10 then 1
            when extract('day' from t.t_dat) <= 20 then 2
            when extract('day' from t.t_dat) <= 31 then 3
        end as month_decade
    from
        stg_transactions t;
-- -------------------------------------------------------------------------------------
create materialized view mv_aggregated_transactions_by_month as 
    select
        t.part_date,
        t.customer_id,
        sum(coalesce(t.price, 0))::numeric(22, 20) as transaction_amount,
        count(t.article_id) as number_of_articles,
        count(distinct a.product_group_name) as number_of_product_groups  
    from
        v_transactions_part_date t
        join stg_articles a on a.article_id = t.article_id
    group by
        t.part_date,
        t.customer_id;
-- -------------------------------------------------------------------------------------
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
                rows between 2 preceding and 1 preceding
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
        t.part_date,
        t.customer_id,
        t.customer_group_by_age, 
        t.transaction_amount,
        t.most_exp_article_id,
        t.number_of_articles,
        t.number_of_product_groups,
        t.most_active_decade,
        case 
            when t.part_date = t.min_part_date 
                then 1
            when date_part('month', t.part_date) - date_part('month', t.min_part_date) = 1 
                then coalesce(t.prior_months_count, 0)
            when t.prior_months_count = 2 
                then 1
            else 0
        end as customer_loyalty  
    from
        cte_enriched_transactions t;   
-- -------------------------------------------------------------------------------------
drop materialized view if exists mv_aggregated_transactions_by_month;
drop materialized view if exists mv_most_expensive_articles;
drop materialized view if exists mv_most_active_month_decades;
drop view if exists v_transactions_part_date;
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
        t.part_date = '2018-10-31'
) to '/home/aerik/learning/code/senla-de-intership/block_01/data/data_mart.csv' csv header;
-- -------------------------------------------------------------------------------------