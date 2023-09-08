select
    (date_trunc('month', t.t_dat) + interval '1 month - 1 day')::date as part_date,
    t.customer_id,
    case 
        when c.age < 23 then 'S'
        when c.age between 23 and 59 then 'A'
        else 'R'
    end as customer_group_by_age,
    sum(coalesce(t.price, 0)) as transaction_amount,
    count(t.article_id) as number_of_articles,
    count(distinct a.product_group_name) as number_of_product_groups
from
    stg_transactions t
    join stg_customers c on c.customer_id = t.customer_id
    join stg_articles a on a.article_id = t.article_id
where
    t.t_dat >= '2018-09-01'
    and t.t_dat < '2018-10-01'
group by
    part_date,
    t.customer_id,
    customer_group_by_age