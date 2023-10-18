# Block 01 - SQL

## Final Task

### Description

The final task is to create a data mart using only SQL. It must consists of the following columns:

| Field name               | Description                                            |
| ------------------------ | ------------------------------------------------------ |
| part_date                | The last day of the month                              |
| customer_id              | Customer ID                                            |
| customer_group_by_age    | Customers age classification                           |
| transaction_amount       | Total sum of purchases                                 |
| most_exp_article_id      | The most expensive article                             |
| number_of_articles       | The number of articles                                 |
| number_of_product_groups | The number of product groups                           |
| most_active_decade       | The part of the month with the most purchases          |
| customer_loyalty         | Customers loyality based on purchases in prior months |

### Technologies

- PostgreSQL

### Data sources

Kaggle - H&M Personalized Fashion Recommendations

- [articles.csv](https://www.kaggle.com/competitions/h-and-m-personalized-fashion-recommendations/data?select=articles.csv)
- [customers.csv](https://www.kaggle.com/competitions/h-and-m-personalized-fashion-recommendations/data?select=articles.csv)
- [transactions_train.csv](https://www.kaggle.com/competitions/h-and-m-personalized-fashion-recommendations/data?select=articles.csv)

### Scripts

- [month_elt.sql](sql/month_elt.sql) - creates a csv file with data for one month.
- [full_elt.sql](sql/full_elt.sql) - creates a data mart table for all period.

### Usage

```bash
psql -h <host> -U <user> -p <port> -d <database> -a -f month_elt.sql -v part_date="'<date>'"
```

```bash
psql -h <host> -U <user> -p <port> -d <database> -a -f full_elt.sql
```

### Result

- [data_mart.csv](data/data_mart.csv)

- [table dm_transactions_by_month](sql/ddl/tables/create_dm_transactions_by_month.sql)
