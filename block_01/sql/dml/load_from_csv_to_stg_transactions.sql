-- -------------------------------------------------------------------------------------
truncate stg_transactions
-- -------------------------------------------------------------------------------------
copy stg_transactions
from '/home/aerik/learning/code/senla-de-intership/block_01/data/transactions_train.csv'
with (
    format csv,
    delimiter ',',
    header true
);
-- -------------------------------------------------------------------------------------