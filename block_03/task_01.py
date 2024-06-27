"""
This Python module extracts data from the following data sources:
    - articles.csv
    - customers.csv
    - transactions_train.csv

Link to the data sources:
    https://www.kaggle.com/competitions/h-and-m-personalized-fashion-recommendations

Period for transactions is based on the month of the --part-date parameter (i.e. when
--part-date = 2018-12-16 then the period for transactions will be from 2018-12-01
to 2018-12-31 inclusively).

After extracting it transforms the data and loads it into a data_mart.csv with
the following fields:
    | Field name                   | Description                        |
    | ---------------------------- | -----------------------------------|
    | part_date                    | The last day of the month          |
    | customer_id                  | Customer ID                        |
    | customer_group_by_age        | Customers age classification       |
    | transaction_amount           | Total sum of purchases             |
    | most_exp_article_id          | The most expensive article         |
    | number_of_articles           | The number of articles             |
    | number_of_product_groups     | The number of product groups       |
    | most_active_decade           | The most active decade of month    |
    | customer_loyality            | Customer loyality [1, 0]           |

Args:
    --part-date: Date in YYYY-MM-DD format. Period for transactions data will be formed
                 based on the month of this date.
    --data-dir: Path to the directory with source files (transactions_train.csv,
                articles.csv, customers.csv) and for the output file.

Example:
    python \
        task_01.py \
            --part-date="2018-10-31" \
            --data-dir="/home/jovyan/work/data"
"""

import argparse
from datetime import datetime

import pandas as pd
from dateutil.relativedelta import relativedelta

ARTICLES_SCHEMA = {
    'article_id': 'Int64',
    'product_group_name': 'object',
}
CUSTOMERS_SCHEMA = {
    'customer_id': 'object',
    'age': 'Int64',
}
TRANSACTIONS_SCHEMA = {
    't_dat': 'object',
    'customer_id': 'object',
    'article_id': 'Int64',
    'price': 'float64',
}
RESULT_COLUMNS_ORDER = [
    'part_date',
    'customer_id',
    'customer_group_by_age',
    'transaction_amount',
    'most_exp_article_id',
    'number_of_articles',
    'number_of_product_groups',
    'most_active_decade',
    'customer_loyalty'
]
MONTHS_BEFORE = 2


def main():
    args = get_args()

    # Add the last and the first day from the date param to the args dict.
    date_param = datetime.strptime(args["part_date"], "%Y-%m-%d")
    args["date_start"] = date_param + relativedelta(day=1)
    args["date_end"] = date_param + relativedelta(day=31)
    # Add the last and the first day for the previous period to the args dict.
    args["prev_date_start"] = args["date_start"] - relativedelta(months=MONTHS_BEFORE) + relativedelta(day=1)
    args["prev_date_end"] = args["date_start"] - relativedelta(months=1) + relativedelta(day=31)

    # Add filepaths for the input and output files to the args dict.
    data_dir_path = args["data_dir"]
    args["transactions_filepath"] = f"{data_dir_path}/transactions_train.csv"
    args["articles_filepath"] = f"{data_dir_path}/articles.csv"
    args["customers_filepath"] = f"{data_dir_path}/customers.csv"
    args["data_mart_filepath"] = f"{data_dir_path}/data_mart.csv"

    transactions_df = pd.read_csv(
        args["transactions_filepath"],
        parse_dates=['t_dat'],
        dtype=TRANSACTIONS_SCHEMA,
        usecols=TRANSACTIONS_SCHEMA.keys()
    )
    articles_df = pd.read_csv(
        args["articles_filepath"],
        dtype=ARTICLES_SCHEMA,
        usecols=ARTICLES_SCHEMA.keys()
    )
    customers_df = pd.read_csv(
        args["customers_filepath"],
        dtype=CUSTOMERS_SCHEMA,
        usecols=CUSTOMERS_SCHEMA.keys()
    )

    # Filter previous transactions for calculating loyalty.
    prev_transactions_df = transactions_df[
        (transactions_df['t_dat'] >= args["prev_date_start"])
        & (transactions_df['t_dat'] <= args["prev_date_end"])
    ]
    # Filter transactions for the full month of the --part_date param.
    transactions_df = transactions_df[
        (transactions_df['t_dat'] >= args["date_start"])
        & (transactions_df['t_dat'] <= args["date_end"])
    ]

    # Add 'part_date', 'month_decade' and 'price_rank'
    transactions_df['part_date'] = transactions_df['t_dat'] + pd.offsets.MonthEnd(0)
    transactions_df['month_decade'] = transactions_df['t_dat'].dt.day.apply(classify_by_day)
    transactions_df['price_rank'] = (
        transactions_df
        .sort_values('t_dat', ascending=True)
        .groupby(['part_date', 'customer_id'])['price']
        .rank(method="first", ascending=False)
    )

    # Add 'transaction_amount', 'number_of_articles' and 'number_of_product_groups'.
    transactions_merged_df = transactions_df.merge(articles_df, on='article_id', how='inner')
    transactions_aggregated_df = (
        transactions_merged_df
        .groupby(['part_date', 'customer_id'])
        .aggregate({
            'price': 'sum',
            'article_id': 'count',
            'product_group_name': pd.Series.nunique
        })
        .reset_index()
        .rename(
            columns={
                'price': 'transaction_amount',
                'article_id': 'number_of_articles',
                'product_group_name': 'number_of_product_groups',
            }
        )
    )

    # Add 'most_exp_article_id'.
    ranked_articles_df = transactions_df[transactions_df['price_rank'] == 1][['customer_id', 'article_id']]
    ranked_articles_df.rename(columns={'article_id': 'most_exp_article_id'}, inplace=True)
    transactions_aggregated_df = transactions_aggregated_df.merge(ranked_articles_df, on='customer_id', how='inner')

    # Add 'most_active_decade'.
    ranked_month_decades_df = (
        transactions_df
        .groupby(['month_decade', 'customer_id'])
        ['price'].sum()
        .reset_index()
    )
    ranked_month_decades_df = (
        ranked_month_decades_df
        .sort_values(by=['price', 'month_decade'], ascending=[False, True])
        .groupby(['customer_id'])
        .first()
        .reset_index()
        [['customer_id', 'month_decade']]
    )
    ranked_month_decades_df.rename(columns={'month_decade': 'most_active_decade'}, inplace=True)
    transactions_aggregated_df = transactions_aggregated_df.merge(
        ranked_month_decades_df, on='customer_id', how='inner'
    )

    # Add 'customer_group_by_age'.
    customers_df['age'].fillna(0, inplace=True)
    customers_df['customer_group_by_age'] = customers_df['age'].apply(classify_by_age)
    customers_df.drop(['age'], axis='columns', inplace=True)
    transactions_aggregated_df = transactions_aggregated_df.merge(customers_df, on='customer_id', how='inner')

    # Add 'customer_loyalty'.
    prev_transactions_df['part_date'] = prev_transactions_df['t_dat'] + pd.offsets.MonthEnd(0)
    prev_transactions_df.drop(['t_dat', 'article_id', 'price'], axis='columns', inplace=True)
    prev_month_count = prev_transactions_df['part_date'].nunique()
    prev_transactions_df = (
        prev_transactions_df
        .drop_duplicates()
        .groupby(['customer_id'])
        .count()
        .reset_index()
        .rename(columns={'part_date': 'month_count'})
        .sort_values('month_count', ascending=False)
    )
    transactions_aggregated_df = transactions_aggregated_df.merge(prev_transactions_df, on='customer_id', how='left')
    transactions_aggregated_df['month_count'].fillna(0, inplace=True)
    transactions_aggregated_df['customer_loyalty'] = (
        (transactions_aggregated_df['month_count'] == prev_month_count).astype(int)
    )

    transactions_aggregated_df.to_csv(
        args['data_mart_filepath'],
        sep=',',
        header=True,
        index=False,
        columns=RESULT_COLUMNS_ORDER
    )


def classify_by_age(age: int) -> str:
    if age < 23:
        return 'S'
    elif age < 60:
        return 'A'
    else:
        return 'R'


def classify_by_day(day: int) -> int:
    if day <= 10:
        return 1
    elif day <= 20:
        return 2
    elif day <= 31:
        return 3


def get_args() -> dict:
    """Obtain and return arguments from the calling of the script.

    Returns:
        Dictionary with arguments.
    """
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--part-date",
        type=str,
        required=True,
        metavar="<part_date>",
        help=(
            "Date in YYYY-MM-DD format. Period for transactions will be formed based on the month of this date"
        ),
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        required=True,
        metavar="<data_dir_path>",
        help=(
            "Path to the directory with source files (transactions_train.csv, "
            "articles.csv, customers.csv) and for the output file."
        ),
    )

    return vars(parser.parse_args())


if __name__ == "__main__":
    main()
