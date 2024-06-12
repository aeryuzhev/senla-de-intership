import sys
from datetime import datetime
from pathlib import Path

from dateutil.relativedelta import relativedelta
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    count,
    countDistinct,
    expr,
    first,
    last_day,
    lit,
    sum,
    upper,
    when,
)
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

PRICE_PRECISION, PRICE_SCALE = 22, 16
RATE_PRECISION, RATE_SCALE = 6, 2


def transactions_etl_job(args: dict, prev_month_filepaths: list = None) -> None:
    """Main ETL function.

    Args:
        args: Dictionary of arguments. Defaults to None.

    Returns:
        None.
    """
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

    spark = initialize_spark()

    transactions_df = extract_transactions(spark, args)
    articles_df = extract_articles(spark, args)
    customers_df = extract_customers(spark, args)

    filtered_transactions_df = filter_transactions(spark, transactions_df, args)
    transformed_df = transform_data(
        filtered_transactions_df,
        articles_df,
        customers_df,
        args,
    )

    if prev_month_filepaths:
        transformed_df = join_prev_months(spark, transformed_df, prev_month_filepaths)

    loyality_df = update_loyality(transformed_df, args)

    load_data(loyality_df, args)

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


def transform_data(
    transactions_df: DataFrame,
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
    get_converted_price = "get_json_object(current_exchange_rate, concat('$.', dm_currency)) * price"

    converted_price_transactions_df = (
        transactions_df
        .withColumn("dm_currency", lit(args["dm_currency"]))
        .withColumn("current_exchange_rate", upper(col("current_exchange_rate")))
        .withColumn("price", coalesce(expr(get_converted_price), col("price")))
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
            "fashion_news_frequency",
            "club_member_status",
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

    window_spec_most_freq_product_group_name = (
        Window
        .partitionBy("customer_id")
        .orderBy(col("product_group_name_count").desc())
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
            first("article_id").over(window_spec_most_expensive_article),
        )
        .withColumn(
            "product_group_name_count",
            count("product_group_name").over(Window.partitionBy("customer_id", "product_group_name"))
        )
        .withColumn(
            "most_freq_product_group_name",
            first("product_group_name").over(window_spec_most_freq_product_group_name),
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
            "most_freq_product_group_name",
            "fashion_news_frequency",
            "club_member_status",
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
            "most_freq_product_group_name",
            "fashion_news_frequency",
            "club_member_status",
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
            "dm_currency",
            "most_exp_article_id",
            "number_of_articles",
            "number_of_product_groups",
            "most_freq_product_group_name",
            "fashion_news_frequency",
            "club_member_status",
        )
        .withColumn("loyal_months_nr", lit(1))
    )

    return aggregated_transactions_df


def join_prev_months(spark: SparkSession, transformed_df: DataFrame, prev_month_filepaths: list) -> DataFrame:
    prev_months_df = (
        spark.read
        .format('csv')
        .option('header', 'true')
        .option('delimiter', ',')
        .load(prev_month_filepaths)
    )

    prev_months_count_df = (
        prev_months_df
        .groupBy("customer_id")
        .agg(count("customer_id").alias("month_count"))
    )

    joined_df = (
        transformed_df
        .join(prev_months_count_df, "customer_id", "left")
        .withColumn("loyal_months_nr", col("loyal_months_nr") + coalesce(col("month_count"), lit(0)))
        .drop("month_count")
    )

    return joined_df


def update_loyality(transformed_df: DataFrame, args: dict) -> DataFrame:
    loyality_df = (
        transformed_df
        .withColumn(
            "customer_loyality",
            when(col("loyal_months_nr") >= lit(args["loyality_level"]), 1)
            .otherwise(0)
        )
        .withColumn(
            "offer",
            when(
                (col("customer_loyality") == 1) &
                (col("club_member_status") == "ACTIVE") &
                (col("fashion_news_frequency") == "Regularly"),
                1
            )
            .otherwise(0)
        )
        .select(
            "part_date",
            "customer_id",
            "customer_group_by_age",
            "transaction_amount",
            "dm_currency",
            "most_exp_article_id",
            "number_of_articles",
            "number_of_product_groups",
            "most_freq_product_group_name",
            "loyal_months_nr",
            "customer_loyality",
            "offer",
        )
    )

    return loyality_df


def load_data(df: DataFrame, args: dict) -> None:
    """Load transformed data to a data_mart.csv file.

    Args:
        transformed_df: Spark dataframe with transformed data.
        args: Dictionary of arguments.

    Returns:
        None.
    """
    df = df.drop("fashion_news_frequency", "club_member_status")

    (
        df
        .repartition(1)
        .write
        .mode("overwrite")
        .format("csv")
        .option("header", "true")
        .option("delimiter", ",")
        .save(args["data_mart_filepath"])
    )


if __name__ == "__main__":
    transactions_etl_job()
