# Block 03 - Pandas

## Final Task

### Description

The final task is to create a data mart using only Pandas. It must consists of the following columns:

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

- Python
- Pandas

### Data sources

Kaggle - H&M Personalized Fashion Recommendations

- [articles.csv](https://www.kaggle.com/competitions/h-and-m-personalized-fashion-recommendations/data?select=articles.csv)
- [customers.csv](https://www.kaggle.com/competitions/h-and-m-personalized-fashion-recommendations/data?select=articles.csv)
- [transactions_train.csv](https://www.kaggle.com/competitions/h-and-m-personalized-fashion-recommendations/data?select=articles.csv)

### Scripts

- [task_01.py](task_01.py) - creates a csv file with data for the one month.

### Usage

```bash
    python \
        task_01.py \
            --part-date="2018-10-31" \
            --data-dir="/home/jovyan/work/data"
```

### Result

- [data_mart.csv](data/data_mart.csv)
