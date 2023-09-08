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
    fn                      text,
    active                  text,
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
    article_id        text,
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