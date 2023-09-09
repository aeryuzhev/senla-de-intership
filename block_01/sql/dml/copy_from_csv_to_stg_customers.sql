-- -------------------------------------------------------------------------------------
truncate stg_customers;
-- -------------------------------------------------------------------------------------
copy stg_customers
from '/home/aerik/learning/code/senla-de-intership/block_01/data/customers.csv'
with (
    format csv,
    delimiter ',',
    header true
);
-- -------------------------------------------------------------------------------------