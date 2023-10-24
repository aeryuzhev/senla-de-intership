"""
This python module launches a pyspark script for creating a data mart csv file.
Also creates missing data mart files for the previous months (if they do not exist).

Args:
    --part-date: Date in YYYY-MM-DD format. Period for transactions data will be formed
                 based on the month of this date.
    --dm-currency: Currency type for price conversion.
    --data-dir: Path to the directory with source files (transactions_train.csv,
                articles.csv, customers.csv) and for the output file.
    --output-dir: Path to the output directory.

Example:
    ${SPARK_HOME}/bin/spark-submit \
        --py-files "/home/jovyan/work/scripts/task_04/transactions_etl.py" \
        /home/jovyan/work/scripts/task_04/transactions_init.py \
            --part-date="2018-12-31" \
            --dm-currency="BYN" \
            --data-dir="/home/jovyan/work/data" \
            --output-dir="/home/jovyan/work/data/dm_transactions"
"""
from pathlib import Path
from dateutil.relativedelta import relativedelta
from datetime import datetime

from transactions_etl import transactions_etl_job, get_args

# A number of previous months for creating data mart files.
NUMBER_OF_PREV_MONTHS = 2


def run_jobs() -> None:
    """Main function for creating a data mart csv file.
    Also creates missing files for the previous months.

    Returns:
        None.
    """
    args = get_args()
    initial_part_date_arg = args["part_date"]

    run_prev_months_jobs(args, NUMBER_OF_PREV_MONTHS)

    args["part_date"] = initial_part_date_arg
    transactions_etl_job(args)


def run_prev_months_jobs(args: dict, number_of_months: int) -> None:
    """Create data mart files for the previous months if they do not exist.

    Args:
        args: Dictionary of arguments.
        number_of_months: A number of previous months for creating data mart files.
    """
    for _ in range(1, number_of_months + 1):
        # Get the last day of the previous month.
        prev_month_last_date = (
            datetime.strptime(args["part_date"], "%Y-%m-%d")
            + relativedelta(day=31)
            - relativedelta(months=1)
        )
        prev_month_last_date = prev_month_last_date.strftime("%Y-%m-%d")

        # Update the part_date in the args dict for execution with the date
        # of the previous month.
        args["part_date"] = prev_month_last_date

        # A path to the previous month data mart csv file.
        prev_month_filepath = Path(f"{args['output_dir']}/{prev_month_last_date}.csv")

        if not prev_month_filepath.exists():
            transactions_etl_job(args)


if __name__ == "__main__":
    run_jobs()
