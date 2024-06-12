"""
This Python module extracts data from the following data sources:
    - articles.csv
    - customers.csv
    - transactions_train_with_currency.csv

Link to the data sources:
    https://www.kaggle.com/competitions/h-and-m-personalized-fashion-recommendations

Period for transactions is based on the month of the --part-date parameter (i.e. when
--part-date = 2018-12-16 then the period for transactions will be from 2018-12-01
to 2018-12-31 inclusively).

After extracting it transforms the data and loads it into a dm_transactions_<date>.csv
with the following fields:
    | Field name                   | Description                        |
    | ---------------------------- | -----------------------------------|
    | part_date                    | The last day of the month          |
    | customer_id                  | Customer ID                        |
    | customer_group_by_age        | Customers age classification       |
    | transaction_amount           | Total sum of purchases             |
    | dm_currency                  | Currency type for price conversion |
    | most_exp_article_id          | The most expensive article         |
    | number_of_articles           | The number of articles             |
    | number_of_product_groups     | The number of product groups       |
    | most_freq_product_group_name | The most expensive group name      |
    | loyal_months_nr              | The number of active monhts        |
    | customer_loyality            | Customer loyality [1, 0]           |
    | offer                        | Offer for a client [1, 0]          |


Args:
    --part-date: Date in YYYY-MM-DD format. Period for transactions data will be formed
                 based on the month of this date.
    --dm-currency: Currency type for price conversion.
    --loyality-level: Count of previous months to calculate loyality.
    --data-dir: Path to the directory with source files (transactions_train.csv,
                articles.csv, customers.csv) and for the output file.
    --output-dir: Path to the output directory.

Example:
    ${SPARK_HOME}/bin/spark-submit \
        --py-files "/home/jovyan/work/scripts/task_05/transactions_etl.py" \
        /home/jovyan/work/scripts/task_05/transactions_init.py \
            --part-date="2019-12-31" \
            --dm-currency="BYN" \
            --loyality-level=3 \
            --data-dir="/home/jovyan/work/data" \
            --output-dir="/home/jovyan/work/data/dm_transactions"
"""
import argparse
from datetime import datetime

from dateutil.relativedelta import relativedelta
from transactions_etl import transactions_etl_job


def run_jobs() -> None:
    """Main function for creating a data mart csv file.
    Also creates missing files for the previous months.

    Returns:
        None.
    """
    args = get_args()
    initial_part_date_arg = args["part_date"]

    prev_month_filepaths = run_prev_months_jobs(args)

    args["part_date"] = initial_part_date_arg
    transactions_etl_job(args, prev_month_filepaths)


def run_prev_months_jobs(args: dict) -> list:
    """Create data mart files for the previous months and return its filepaths.

    Args:
        args: Dictionary of arguments.
    """
    prev_month_filepaths = []

    for _ in range(1, args["loyality_level"]):
        # Get the last day of the previous month.
        prev_month_last_date = (
            datetime.strptime(args["part_date"], "%Y-%m-%d")
            - relativedelta(months=1)
            + relativedelta(day=31)
        )
        prev_month_last_date = prev_month_last_date.strftime("%Y-%m-%d")

        # Update the part_date in the args dict for execution with the date
        # of the previous month.
        args["part_date"] = prev_month_last_date

        # A path to the previous month data mart csv file.
        prev_month_filepaths.append(f"{args['output_dir']}/{prev_month_last_date}.csv")

        transactions_etl_job(args)

    return prev_month_filepaths


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
            "Date in YYYY-MM-DD format. Period for transactions will be formed based on "
            "the month of this date."
        ),
    )
    parser.add_argument(
        "--dm-currency",
        type=str,
        required=True,
        choices=["USD", "EUR", "BYN", "PLN"],
        metavar="<dm_currency>",
        help="Currency type for price conversion.",
    )
    parser.add_argument(
        "--loyality-level",
        type=int,
        required=True,
        metavar="<loyality_level>",
        help="Number of monthes to process data.",
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        required=True,
        metavar="<data_dir_path>",
        help=(
            "Path to the directory with source files "
            "(transactions_train_with_currency.csv, articles.csv, customers.csv) "
            "and for the output file."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        required=True,
        metavar="<output_dir_path>",
        help="Path to the output directory.",
    )

    return vars(parser.parse_args())


if __name__ == "__main__":
    run_jobs()
