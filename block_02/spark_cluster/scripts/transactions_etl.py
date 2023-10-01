"""
This Python module extracts data from the following data sources:
    - articles.csv
    - customers.csv
    - transactions_train.csv

Link to the data sources:
    https://www.kaggle.com/competitions/h-and-m-personalized-fashion-recommendations

Period for transactions is based on the month of the --date parameter (i.e. when
--date = 2018-12-16 then the period for transactions will be from 2018-12-01
to 2018-12-31 inclusively).

After extracting it transforms the data and loads it into a data_mart.csv with
the following fields:
    | Field name               | Description                  |
    | ------------------------ | -----------------------------|
    | part_date                | The last day of the month    |
    | customer_id              | Customer ID                  |
    | customer_group_by_age    | Customers age classification |
    | transaction_amount       | Total sum of purchases       |
    | most_exp_article_id      | The most expensive article   |
    | number_of_articles       | The number of articles       |
    | number_of_product_groups | The number of product groups |

Args:
    --date: Date in YYYY-MM-DD format. Period for transactions data will be formed based
            on the month of this date.
    --data-dir: Path to the directory with source files (transactions_train.csv,
                articles.csv, customers.csv) and for the output file.

Example:
    ${SPARK_HOME}/bin/spark-submit \
        --master spark://spark-master:7077 \
        /home/jovyan/work/scripts/transactions_etl.py \
            --date="2018-12-31" \
            --data-dir="/home/jovyan/work/data"
"""

from datetime import datetime
from dateutil.relativedelta import relativedelta
import argparse

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def main() -> None:
    """Main ETL function.

    Returns:
        None.
    """
    args = get_args()

    spark = initialize_spark()

    transactions_df = extract_transactions(spark, args)
    articles_df = extract_articles(spark, args)
    customers_df = extract_customers(spark, args)

    transformed_data = transform_data(transactions_df, articles_df, customers_df, args)

    load_data(transformed_data, args)

    spark.stop()


def initialize_spark() -> SparkSession:
    """Initialize a new SparkSession.

    Returns:
        SparkSession object.
    """
    spark = (
        SparkSession.builder
        .master("spark://spark-master:7077")
        .appName("transactions_etl")
        .getOrCreate()
    )

    return spark


def extract_transactions(spark: SparkSession, args: dict) -> DataFrame:
    """Extract the csv file with transactions data into the Spark dataframe.

    Args:
        spark: SparkSession object.
        args: Dictionary of arguments (from the get_args function).

    Returns:
        Spark dataframe.
    """
    schema = T.StructType(
        [
            T.StructField("t_dat", T.DateType(), True),
            T.StructField("customer_id", T.StringType(), True),
            T.StructField("article_id", T.IntegerType(), True),
            T.StructField("price", T.DecimalType(22, 20), True),
            T.StructField("sales_channel_id", T.IntegerType(), True),
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
        args: Dictionary of arguments (from the get_args function).

    Returns:
        Spark dataframe.
    """
    schema = T.StructType(
        [
            T.StructField("article_id", T.IntegerType(), True),
            T.StructField("product_code", T.IntegerType(), True),
            T.StructField("prod_name", T.StringType(), True),
            T.StructField("product_type_no", T.IntegerType(), True),
            T.StructField("product_type_name", T.StringType(), True),
            T.StructField("product_group_name", T.StringType(), True),
            T.StructField("graphical_appearance_no", T.IntegerType(), True),
            T.StructField("graphical_appearance_name", T.StringType(), True),
            T.StructField("colour_group_code", T.IntegerType(), True),
            T.StructField("colour_group_name", T.StringType(), True),
            T.StructField("perceived_colour_value_id", T.IntegerType(), True),
            T.StructField("perceived_colour_value_name", T.StringType(), True),
            T.StructField("perceived_colour_master_id", T.IntegerType(), True),
            T.StructField("perceived_colour_master_name", T.StringType(), True),
            T.StructField("department_no", T.IntegerType(), True),
            T.StructField("department_name", T.StringType(), True),
            T.StructField("index_code", T.StringType(), True),
            T.StructField("index_name", T.StringType(), True),
            T.StructField("index_group_no", T.IntegerType(), True),
            T.StructField("index_group_name", T.StringType(), True),
            T.StructField("section_no", T.IntegerType(), True),
            T.StructField("section_name", T.StringType(), True),
            T.StructField("garment_group_no", T.IntegerType(), True),
            T.StructField("garment_group_name", T.StringType(), True),
            T.StructField("detail_desc", T.StringType(), True),
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
        args: Dictionary of arguments (from the get_args function).

    Returns:
        Spark dataframe.
    """
    schema = T.StructType(
        [
            T.StructField("customer_id", T.StringType(), True),
            T.StructField("FN", T.DecimalType(2, 1), True),
            T.StructField("Active", T.DecimalType(2, 1), True),
            T.StructField("club_member_status", T.StringType(), True),
            T.StructField("fashion_news_frequency", T.StringType(), True),
            T.StructField("age", T.IntegerType(), True),
            T.StructField("postal_code", T.StringType(), True),
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


def transform_data(
    transactions_df: DataFrame,
    articles_df: DataFrame,
    customers_df: DataFrame,
    args: dict,
) -> DataFrame:
    """Filter, enrich, transform and aggregate data.

    Args:
        transactions_df: Spark dataframe with transactions data.
        articles_df: Spark dataframe with articles data.
        customers_df: Spark dataframe with customers data.
        args: Dictionary of arguments (from the get_args function).

    Returns:
        Spark dataframe.
    """
    filtered_transactions_df = transactions_df.where(
        (F.col("t_dat") >= args["date_start"]) &
        (F.col("t_dat") <= args["date_end"])
    )

    enriched_transactions_df = (
        filtered_transactions_df
        .join(customers_df, "customer_id", "inner")
        .join(articles_df, "article_id", "inner")
        .select(
            "t_dat",
            "customer_id",
            "article_id",
            "price",
            "age",
            "product_group_name"
        )
    )

    window_spec_most_expensive_article = (
        Window
        .partitionBy("customer_id")
        .orderBy(
            F.col("price").desc(),
            F.col("t_dat").asc()
        )
    )

    transformed_transactions_df = (
        enriched_transactions_df
        .withColumn(
            "customer_group_by_age",
            F.when(F.col("age") < 23, "S")
            .when(F.col("age") < 60, "A")
            .otherwise("R"),
        )
        .withColumn(
            "most_exp_article_id",
            F.first("article_id")
            .over(window_spec_most_expensive_article),
        )
        .withColumn("part_date", F.last_day(F.col("t_dat")))
        .select(
            "part_date",
            "customer_id",
            "article_id",
            "price",
            "product_group_name",
            "customer_group_by_age",
            "most_exp_article_id",
        )
    )

    aggregated_transactions_df = (
        transformed_transactions_df
        .groupBy(
            "part_date",
            "customer_id",
            "customer_group_by_age",
            "most_exp_article_id"
        )
        .agg(
            F.sum("price").alias("transaction_amount"),
            F.count("article_id").alias("number_of_articles"),
            F.countDistinct("product_group_name").alias("number_of_product_groups"),
        )
        .select(
            "part_date",
            "customer_id",
            "customer_group_by_age",
            "transaction_amount",
            "most_exp_article_id",
            "number_of_articles",
            "number_of_product_groups",
        )
    )

    return aggregated_transactions_df


def load_data(
    transformed_df: DataFrame, args: dict, to_single_file: bool = True
) -> None:
    """Load transformed data to a data_mart.csv file.

    Args:
        transformed_df: Spark dataframe with transformed data.
        args: Dictionary of arguments (from the get_args function).
        to_single_file: False for creating a spark-like directory.
                        True for creating a single file. Defaults to True.
    Returns:
        None.
    """
    if to_single_file:
        (
            transformed_df
            .toPandas()
            .to_csv(args["data_mart_filepath"], sep=",", index=False)
        )
    else:
        (
            transformed_df
            .coalesce(1)
            .write.mode("overwrite")
            .format("csv")
            .option("header", "true")
            .option("delimiter", ",")
            .save(args["data_mart_filepath"])
        )


def get_args() -> dict:
    """Obtain arguments from the calling of the module. Use these arguments to create
    a dictionary with the following arguments:
        data_dir: Path to the directory with source files.
        date_start: Start of the period for the transactions data.
        date_end: End of the period for the transactions data.
        transactions_filepath: Path to the source file with transactions data.
        articles_filepath: Path to the source file with articles data.
        customers_filepath: Path to the source file with customers data.
        data_mart_filepath: Path to the output data_mart file.

    Returns:
        Dictionary with arguments.
    """
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--date",
        type=str,
        required=True,
        metavar="<date>",
        help=(
            "Date in YYYY-MM-DD format. Period for transactions will be formed based on "
            "the month of this date"
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

    args = vars(parser.parse_args())
    print(args)

    # Get the last and the first day from the date param.
    date_param = datetime.strptime(args["date"], "%Y-%m-%d")
    args["date_start"] = date_param + relativedelta(day=1)
    args["date_end"] = date_param + relativedelta(day=31)

    data_dir_path = args["data_dir"]
    args["transactions_filepath"] = f"{data_dir_path}/transactions_train.csv"
    args["articles_filepath"] = f"{data_dir_path}/articles.csv"
    args["customers_filepath"] = f"{data_dir_path}/customers.csv"
    args["data_mart_filepath"] = f"{data_dir_path}/data_mart.csv"

    return args


if __name__ == "__main__":
    main()
