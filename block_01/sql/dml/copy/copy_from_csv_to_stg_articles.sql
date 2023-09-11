-- -------------------------------------------------------------------------------------
truncate stg_articles;
-- -------------------------------------------------------------------------------------
copy stg_articles
from '/home/aerik/learning/code/senla-de-intership/block_01/data/articles.csv'
with (
    format csv,
    delimiter ',',
    header true
);
-- -------------------------------------------------------------------------------------