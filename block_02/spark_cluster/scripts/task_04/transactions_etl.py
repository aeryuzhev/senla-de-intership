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
    | Field name               | Description                        |
    | ------------------------ | -----------------------------------|
    | part_date                | The last day of the month          |
    | customer_id              | Customer ID                        |
    | customer_group_by_age    | Customers age classification       |
    | transaction_amount       | Total sum of purchases             |
    | most_exp_article_id      | The most expensive article         |
    | number_of_articles       | The number of articles             |
    | number_of_product_groups | The number of product groups       |
    | dm_currency              | Currency type for price conversion |

Args:
    --part-date: Date in YYYY-MM-DD format. Period for transactions data will be formed
                 based on the month of this date.
    --dm-currency: Currency type for price conversion.
    --data-dir: Path to the directory with source files (
                transactions_train_with_currency.csv, articles.csv, customers.csv)
                and for the output file.
    --output-dir: Path to the output directory.

Example:
    ${SPARK_HOME}/bin/spark-submit \
        /home/jovyan/work/scripts/task_04/transactions_etl.py \
            --part-date="2018-12-31" \
            --dm-currency="BYN" \
            --data-dir="/home/jovyan/work/data" \
            --output-dir="/home/jovyan/work/data/dm_transactions"
"""
import sys
from pathlib import Path
from datetime import datetime
from dateutil.relativedelta import relativedelta
import argparse

import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    DateType,
    StringType,
    IntegerType,
    DecimalType,
)
from pyspark.sql.functions import (
    col,
    when,
    first,
    last_day,
    sum,
    count,
    countDistinct,
    from_json,
    lit,
)
from pyspark.sql.pandas.functions import pandas_udf
from pyspark.sql.window import Window

PRICE_PRECISION, PRICE_SCALE = 22, 16
RATE_PRECISION, RATE_SCALE = 6, 2


def transactions_etl_job(spark=None, args=None) -> None:
    """Main ETL function.

    Args:
        spark: SparkSession object.
        args: Dictionary of arguments. Defaults to None.

    Returns:
        None.
    """

    if not args:
        args = get_args()

    # Add the last and the first day from the date param to the args dict.
    date_param = datetime.strptime(args["part_date"], "%Y-%m-%d")
    args["date_start"] = date_param + relativedelta(day=1)
    args["date_end"] = date_param + relativedelta(day=31)

    # Add filepaths for the input and output files to the args dict.
    data_dir_path = args["data_dir"]
    output_dir_path = args["output_dir"]
    date_end_str = args['date_end'].strftime('%Y-%m-%d')

    args["transactions_filepath"] = (
        f"{data_dir_path}/transactions_train_with_currency.csv"
    )
    args["articles_filepath"] = f"{data_dir_path}/articles.csv"
    args["customers_filepath"] = f"{data_dir_path}/customers.csv"
    args["data_mart_filepath"] = f"{output_dir_path}/{date_end_str}.csv"

    check_paths(args)

    if not spark:
        spark = initialize_spark()
        is_external_spark_session = False
    else:
        is_external_spark_session = True

    transactions_df = extract_transactions(spark, args)
    articles_df = extract_articles(spark, args)
    customers_df = extract_customers(spark, args)

    filtered_transactions_df = filter_transactions(spark, transactions_df, args)
    transformed_data = transform_data(
        filtered_transactions_df,
        articles_df,
        customers_df,
        args,
    )

    load_data(transformed_data, args)

    if not is_external_spark_session:
        spark.stop()


def check_paths(args: dict) -> None:
    """Check for the source files and for the output file directory.
    Exit if files do not exist.

    Args:
        args: Dictionary of arguments.

    Returns:
        None.
    """
    filepaths = (
        args["transactions_filepath"],
        args["articles_filepath"],
        args["customers_filepath"],
        args["output_dir"],
    )

    for filepath in filepaths:
        if not Path(filepath).exists():
            sys.exit(f"Path does not exist: {str(filepath)}")


def initialize_spark() -> SparkSession:
    """Initialize a new SparkSession.

    Returns:
        SparkSession object.
    """
    spark = (
        SparkSession.builder
        .master("spark://spark-master:7077")
        .appName("transactions_etl")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel('ERROR')

    return spark


def extract_transactions(spark: SparkSession, args: dict) -> DataFrame:
    """Extract the csv file with transactions data into the Spark dataframe.

    Args:
        spark: SparkSession object.
        args: Dictionary of arguments.

    Returns:
        Spark dataframe.
    """
    schema = StructType(
        [
            StructField('id', IntegerType(), True),
            StructField('t_dat', DateType(), True),
            StructField('customer_id', StringType(), True),
            StructField('article_id', IntegerType(), True),
            StructField('price', DecimalType(PRICE_PRECISION, PRICE_SCALE), True),
            StructField('sales_channel_id', IntegerType(), True),
            StructField('currency', StringType(), True),
            StructField('current_exchange_rate', StringType(), True)
        ]
    )

    df = (
        spark.read
        .format("csv")
        .schema(schema)
        .option("header", "true")
        .option("delimiter", ",")
        .load(args["transactions_filepath"])
    )

    return df


def extract_articles(spark: SparkSession, args: dict) -> DataFrame:
    """Extract the csv file with articles data into the Spark dataframe.

    Args:
        spark: SparkSession object.
        args: Dictionary of arguments.

    Returns:
        Spark dataframe.
    """
    schema = StructType(
        [
            StructField('article_id', IntegerType(), True),
            StructField('product_code', IntegerType(), True),
            StructField('prod_name', StringType(), True),
            StructField('product_type_no', IntegerType(), True),
            StructField('product_type_name', StringType(), True),
            StructField('product_group_name', StringType(), True),
            StructField('graphical_appearance_no', IntegerType(), True),
            StructField('graphical_appearance_name', StringType(), True),
            StructField('colour_group_code', IntegerType(), True),
            StructField('colour_group_name', StringType(), True),
            StructField('perceived_colour_value_id', IntegerType(), True),
            StructField('perceived_colour_value_name', StringType(), True),
            StructField('perceived_colour_master_id', IntegerType(), True),
            StructField('perceived_colour_master_name', StringType(), True),
            StructField('department_no', IntegerType(), True),
            StructField('department_name', StringType(), True),
            StructField('index_code', StringType(), True),
            StructField('index_name', StringType(), True),
            StructField('index_group_no', IntegerType(), True),
            StructField('index_group_name', StringType(), True),
            StructField('section_no', IntegerType(), True),
            StructField('section_name', StringType(), True),
            StructField('garment_group_no', IntegerType(), True),
            StructField('garment_group_name', StringType(), True),
            StructField('detail_desc', StringType(), True)
        ]
    )

    df = (
        spark.read
        .format("csv")
        .schema(schema)
        .option("header", "true")
        .option("delimiter", ",")
        .load(args["articles_filepath"])
    )

    return df


def extract_customers(spark: SparkSession, args: dict) -> DataFrame:
    """Extract the csv file with customers data into the Spark dataframe.

    Args:
        spark: SparkSession object.
        args: Dictionary of arguments.

    Returns:
        Spark dataframe.
    """
    schema = StructType(
        [
            StructField('customer_id', StringType(), True),
            StructField('FN', DecimalType(2, 1), True),
            StructField('Active', DecimalType(2, 1), True),
            StructField('club_member_status', StringType(), True),
            StructField('fashion_news_frequency', StringType(), True),
            StructField('age', IntegerType(), True),
            StructField('postal_code', StringType(), True)
        ]
    )

    df = (
        spark.read
        .format("csv")
        .schema(schema)
        .option("header", "true")
        .option("delimiter", ",")
        .load(args["customers_filepath"])
    )

    return df


def filter_transactions(
    spark: SparkSession,
    transactions_df: DataFrame,
    args: dict
) -> DataFrame:
    """Filter data. Exit if no data founded.

    Args:
        spark: SparkSession object.
        transactions_df: Spark dataframe with transactions data.
        args: Dictionary of arguments.

    Returns:
        Spark dataframe.
    """
    filtered_transactions_df = (
        transactions_df
        .where(
            (col("t_dat") >= args["date_start"]) &
            (col("t_dat") <= args["date_end"])
        )
    )

    if filtered_transactions_df.isEmpty():
        spark.stop()
        sys.exit(
            f"No transactions data for the period "
            f"from {args['date_start'].strftime('%Y-%m-%d')} "
            f"to {args['date_end'].strftime('%Y-%m-%d')}"
        )

    return filtered_transactions_df


@pandas_udf(StringType())  # type: ignore
def to_upper_case(value: pd.Series) -> pd.Series:
    return value.str.upper()


@pandas_udf(DecimalType(PRICE_PRECISION, PRICE_SCALE))  # type: ignore
def convert_price(
    rates_map: pd.Series,
    dm_currency: pd.Series,
    price: pd.Series
) -> pd.Series:
    rate = rates_map.apply(lambda row: row.get(dm_currency[0], 1))
    return price * rate


def transform_data(
    filtered_transactions_df: DataFrame,
    articles_df: DataFrame,
    customers_df: DataFrame,
    args: dict,
) -> DataFrame:
    """Enrich, transform and aggregate data.

    Args:
        filtered_transactions_df: Spark dataframe with transactions data.
        articles_df: Spark dataframe with articles data.
        customers_df: Spark dataframe with customers data.
        args: Dictionary of arguments.

    Returns:
        Spark dataframe.
    """
    exchange_rate_schema = f"map<string, decimal({RATE_PRECISION}, {RATE_SCALE})>"

    converted_price_transactions_df = (
        filtered_transactions_df
        .withColumn("dm_currency", lit(args["dm_currency"]))
        .withColumn(
            "current_exchange_rate",
            to_upper_case(col("current_exchange_rate"))  # type: ignore
        )
        .withColumn(
            "current_exchange_rate",
            from_json(col("current_exchange_rate"), exchange_rate_schema)
        )
        .withColumn(
            "price",
            when(
                col("currency") != col("dm_currency"),
                convert_price(
                    col("current_exchange_rate"),  # type: ignore
                    col("dm_currency"),  # type: ignore
                    col("price")  # type: ignore
                )
            )
            .otherwise(col("price"))
        )
    )

    enriched_transactions_df = (
        converted_price_transactions_df
        .join(customers_df, "customer_id", "inner")
        .join(articles_df, "article_id", "inner")
        .select(
            "t_dat",
            "customer_id",
            "article_id",
            "price",
            "age",
            "product_group_name",
            "dm_currency",
        )
    )

    window_spec_most_expensive_article = (
        Window
        .partitionBy("customer_id")
        .orderBy(
            col("price").desc(),
            col("t_dat").asc()
        )
    )

    transformed_transactions_df = (
        enriched_transactions_df
        .withColumn(
            "customer_group_by_age",
            when(col("age") < 23, "S")
            .when(col("age") < 60, "A")
            .otherwise("R"),
        )
        .withColumn(
            "most_exp_article_id",
            first("article_id")
            .over(window_spec_most_expensive_article),
        )
        .withColumn("part_date", last_day(col("t_dat")))
        .select(
            "part_date",
            "customer_id",
            "article_id",
            "price",
            "product_group_name",
            "customer_group_by_age",
            "most_exp_article_id",
            "dm_currency",
        )
    )

    aggregated_transactions_df = (
        transformed_transactions_df
        .groupBy(
            "part_date",
            "customer_id",
            "customer_group_by_age",
            "most_exp_article_id",
            "dm_currency",
        )
        .agg(
            sum("price").alias("transaction_amount"),
            count("article_id").alias("number_of_articles"),
            countDistinct("product_group_name").alias("number_of_product_groups"),
        )
        .select(
            "part_date",
            "customer_id",
            "customer_group_by_age",
            "transaction_amount",
            "most_exp_article_id",
            "number_of_articles",
            "number_of_product_groups",
            "dm_currency",
        )
    )

    return aggregated_transactions_df


def load_data(transformed_df: DataFrame, args: dict) -> None:
    """Load transformed data to a data_mart.csv file.

    Args:
        transformed_df: Spark dataframe with transformed data.
        args: Dictionary of arguments.

    Returns:
        None.
    """
    (
        transformed_df
        .toPandas()
        .to_csv(args["data_mart_filepath"], sep=",", index=False)
    )


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
    transactions_etl_job()
