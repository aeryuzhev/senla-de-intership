-- -------------------------------------------------------------------------------------
drop table if exists stg_transactions;
-- -------------------------------------------------------------------------------------
create table stg_transactions (
    t_dat             date,
    customer_id       text,
    article_id        integer,
    price             numeric(19, 18),
    sales_channel_id  smallint
);
-- -------------------------------------------------------------------------------------