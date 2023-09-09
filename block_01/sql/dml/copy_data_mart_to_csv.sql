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
    price             numeric(10, 6),
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
-- -------------------------------------------------------------------------------------
copy (
    with 
    cte_filtered_transactions as (
        select
            t.t_dat as transaction_date,
            (date_trunc('month', t.t_dat) + interval '1 month - 1 day')::date as part_date,
            case    
                when extract('day' from t.t_dat) <= 10 then 1
                when extract('day' from t.t_dat) <= 20 then 2
                when extract('day' from t.t_dat) <= 31 then 3
            end as month_decade,
            t.customer_id,
            t.article_id,
            t.price
        from
            stg_transactions t
        where
            t.t_dat >= '2018-10-01'
            and t.t_dat <= '2018-10-31'
    ),
    -- -------------------------------
    cte_prior_months_transactions as (
        select
            t.customer_id,
            count(distinct extract('month' from t.t_dat)) as count_transaction_month
        from
            stg_transactions t
        where
            t.t_dat >= '2018-08-01'
            and t.t_dat <= '2018-09-30'
        group by
            customer_id
    ),    
    -- -------------------------------
    cte_ranked_articles as (
        select
            t.customer_id,
            t.article_id,
            row_number() over (
                partition by t.customer_id 
                order by t.price desc, t.transaction_date
            ) as price_rank
        from
            cte_filtered_transactions t
    ),
    -- -------------------------------
    cte_aggregated_transactions_by_decade as (
        select
            t.part_date,
            t.month_decade,
            t.customer_id,        
            sum(coalesce(t.price, 0)) as transaction_amount     
        from
            cte_filtered_transactions t 
        group by
            t.part_date,
            t.month_decade,
            t.customer_id        
    ),
    -- -------------------------------
    cte_ranked_transactions_by_decade as (
        select
            t.part_date,
            t.month_decade,
            t.customer_id,
            row_number() over (
                partition by t.customer_id
                order by t.transaction_amount desc, t.month_decade
            ) as month_decade_rank      
        from
            cte_aggregated_transactions_by_decade t 
    ),
    -- -------------------------------
    cte_aggregated_transactions_by_month as (
        select
            t.part_date,
            t.customer_id,
            case 
                when c.age < 23 then 'S'
                when c.age < 60 then 'A'
                else 'R'
            end as customer_group_by_age,   
            sum(coalesce(t.price, 0)) as transaction_amount,
            count(t.article_id) as number_of_articles,
            count(distinct a.product_group_name) as number_of_product_groups  
        from
            cte_filtered_transactions t 
            join stg_articles a on a.article_id = t.article_id
            join stg_customers c on c.customer_id = t.customer_id
        group by
            t.part_date,
            t.customer_id,
            customer_group_by_age
    ),
    -- -------------------------------
    cte_min_part_date as (
        select
            (date_trunc('month', min(st.t_dat)) + interval '1 month - 1 day')::date as min_part_date
        from
            stg_transactions st
    )  
    select
        t.part_date,
        t.customer_id,
        t.customer_group_by_age,
        t.transaction_amount,
        a.article_id as most_exp_article_id,
        t.number_of_articles,
        t.number_of_product_groups,
        d.month_decade as most_active_decade,
        case 
            when t.part_date = m.min_part_date then 1
            when date_part('month', t.part_date) - date_part('month', m.min_part_date) = 1 then coalesce(p.count_transaction_month, 0)
            when p.count_transaction_month = 2 then 1
            else 0
        end as customer_loyalty  
    from
        cte_aggregated_transactions_by_month t
        cross join cte_min_part_date m
        join cte_ranked_articles a on a.customer_id = t.customer_id
            and a.price_rank = 1
        join cte_ranked_transactions_by_decade d on d.customer_id = t.customer_id
            and d.month_decade_rank = 1
        left join cte_prior_months_transactions p on p.customer_id = t.customer_id
) to '/home/aerik/learning/code/senla-de-intership/block_01/data/data_mart.csv' csv header; 
-- -------------------------------------------------------------------------------------