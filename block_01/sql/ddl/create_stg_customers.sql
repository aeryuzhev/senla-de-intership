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