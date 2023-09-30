from datetime import datetime
from dateutil.relativedelta import relativedelta

from pyspark.sql import SparkSession
from pyspark.sql import types as T
from pyspark.sql import functions as F
from pyspark.sql.window import Window

DATE_PARAM = "2018-12-31"
TRANSACTIONS_FILE = "/home/jovyan/work/data/transactions_train.csv"
ARTICLES_FILE = "/home/jovyan/work/data/articles.csv"
CUSTOMERS_FILE = "/home/jovyan/work/data/customers.csv"
OUTPUT_FILE = "/home/jovyan/work/data/data_mart.csv"


filter_date = datetime.strptime(DATE_PARAM, "%Y-%m-%d")
date_begin = filter_date + relativedelta(day=1)
date_end = filter_date + relativedelta(day=31)

spark = (
    SparkSession.builder
    .master("spark://spark-master:7077")
    .appName("transactions-data-mart")
    .getOrCreate()
)

transactions_schema = T.StructType(
    [
        T.StructField("t_dat", T.DateType(), True),
        T.StructField("customer_id", T.StringType(), True),
        T.StructField("article_id", T.IntegerType(), True),
        T.StructField("price", T.DecimalType(22, 20), True),
        T.StructField("sales_channel_id", T.IntegerType(), True)
    ]
)

articles_schema = T.StructType(
    [
        T.StructField('article_id', T.IntegerType(), True),
        T.StructField('product_code', T.IntegerType(), True),
        T.StructField('prod_name', T.StringType(), True),
        T.StructField('product_type_no', T.IntegerType(), True),
        T.StructField('product_type_name', T.StringType(), True),
        T.StructField('product_group_name', T.StringType(), True),
        T.StructField('graphical_appearance_no', T.IntegerType(), True),
        T.StructField('graphical_appearance_name', T.StringType(), True),
        T.StructField('colour_group_code', T.IntegerType(), True),
        T.StructField('colour_group_name', T.StringType(), True),
        T.StructField('perceived_colour_value_id', T.IntegerType(), True),
        T.StructField('perceived_colour_value_name', T.StringType(), True),
        T.StructField('perceived_colour_master_id', T.IntegerType(), True),
        T.StructField('perceived_colour_master_name', T.StringType(), True),
        T.StructField('department_no', T.IntegerType(), True),
        T.StructField('department_name', T.StringType(), True),
        T.StructField('index_code', T.StringType(), True),
        T.StructField('index_name', T.StringType(), True),
        T.StructField('index_group_no', T.IntegerType(), True),
        T.StructField('index_group_name', T.StringType(), True),
        T.StructField('section_no', T.IntegerType(), True),
        T.StructField('section_name', T.StringType(), True),
        T.StructField('garment_group_no', T.IntegerType(), True),
        T.StructField('garment_group_name', T.StringType(), True),
        T.StructField('detail_desc', T.StringType(), True)
    ]
)

customers_schema = T.StructType(
    [
        T.StructField('customer_id', T.StringType(), True),
        T.StructField('FN', T.DecimalType(2, 1), True),
        T.StructField('Active', T.DecimalType(2, 1), True),
        T.StructField('club_member_status', T.StringType(), True),
        T.StructField('fashion_news_frequency', T.StringType(), True),
        T.StructField('age', T.IntegerType(), True),
        T.StructField('postal_code', T.StringType(), True)
    ]
)

transactions_df = (
    spark.read
    .format("csv")
    .schema(transactions_schema)
    .option("header", "true")
    .option("delimiter", ",")
    .load(TRANSACTIONS_FILE)
)

articles_df = (
    spark.read
    .format("csv")
    .schema(articles_schema)
    .option("header", "true")
    .option("delimiter", ",")
    .load(ARTICLES_FILE)
)

customers_df = (
    spark.read
    .format("csv")
    .schema(customers_schema)
    .option("header", "true")
    .option("delimiter", ",")
    .load(CUSTOMERS_FILE)
)

filtered_transactions_df = (
    transactions_df
    .where(
        (F.col("t_dat") >= date_begin) &
        (F.col("t_dat") <= date_end)
    )
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
        F.col("t_dat")
    )
)

transformed_transactions_df = (
    enriched_transactions_df
    .withColumn(
        "customer_group_by_age",
        F.when(F.col("age") < 23, "S")
        .when(F.col("age") < 60, "A")
        .otherwise("R")
    )
    .withColumn(
        "most_exp_article_id",
        F.first("article_id")
        .over(window_spec_most_expensive_article)
    )
    .withColumn(
        "part_date",
        F.last_day(F.col("t_dat"))
    )
    .select(
        "part_date",
        "customer_id",
        "article_id",
        "price",
        "product_group_name",
        "customer_group_by_age",
        "most_exp_article_id"
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
        F.countDistinct("product_group_name").alias("number_of_product_groups")
    )
    .select(
        "part_date",
        "customer_id",
        "customer_group_by_age",
        "transaction_amount",
        "most_exp_article_id",
        "number_of_articles",
        "number_of_product_groups"
    )
)

# (
#     aggregated_transactions
#     .coalesce(1)
#     .write
#     .mode("overwrite")
#     .format("csv")
#     .option("header", "true")
#     .option("delimiter", ",")
#     .save(OUTPUT_FILE)
# )

(
    aggregated_transactions_df
    .toPandas()
    .to_csv(OUTPUT_FILE, sep=",", index=False)
)

spark.stop()
