"""
This Python module extracts data from multiple text files into the Spark dataframe,
splits the data into separate words, creates a dataframe with bigrams, counts unique
bigrams and outputs the result into stdout.

Link to the data sources:
    https://www.kaggle.com/datasets/paultimothymooney/poetry/data

Args:
    --data-dir: Path to the directory with source files.
    --output-file: Path to the output file.

Example:
    ${SPARK_HOME}/bin/spark-submit \
        /home/jovyan/work/scripts/task_03.py \
            --data-dir="/home/jovyan/work/data/song_lyrics" \
            --output-file="/home/jovyan/work/data/bigram_counts.csv"
"""

import argparse

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    explode,
    split,
    regexp_replace,
    lit,
    concat,
    lead,
    lower
)
from pyspark.sql import Window

SPLIT_PATTERN = r"[\s_]+|[^\w-'`*:$&.]+|\.\.\.*"
REPLACE_PATTERN = r"^\W+|\W+$"


def main() -> None:
    """Main ETL function.

    Returns:
        None.
    """
    args = get_args()

    spark = initialize_spark()

    song_lyrics_df = extract_song_lyrics(spark, args)

    bigrams_df = split_song_lyrics_into_bigrams(song_lyrics_df)
    counted_unique_bigrams_df = count_unique_bigrams(bigrams_df)

    load_data(bigrams_df, counted_unique_bigrams_df, args)

    spark.stop()


def initialize_spark() -> SparkSession:
    """Initialize a new SparkSession.

    Returns:
        SparkSession object.
    """
    spark = (
        SparkSession.builder
        .master("spark://spark-master:7077")
        .appName("bigram-count")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel('ERROR')

    return spark


def extract_song_lyrics(spark: SparkSession, args: dict) -> DataFrame:
    """Extract txt files data into the Spark dataframe.

    Args:
        spark: SparkSession object.
        args: Dictionary of arguments (from the get_args function).

    Returns:
        Spark dataframe.
    """

    df = (
        spark.read
        .format("text")
        .load(f"{args['data_dir']}/*.txt")
    )

    return df


def split_song_lyrics_into_bigrams(song_lyrics_df: DataFrame) -> DataFrame:
    """Split flat text into separate words. Remove all non-alphanumeric characters
    from the beginning and end of the word. Create a dataframe with bigrams.

    Args:
        song_lyrics_df: Spark dataframe with song lyrics data.

    Returns:
        Spark dataframe.
    """
    words_df = (
        song_lyrics_df
        .withColumn("word", explode(split(col("value"), SPLIT_PATTERN)))
        .withColumn("word", regexp_replace(col("word"), REPLACE_PATTERN, ""))
        .withColumn("word", lower(col("word")))
        .where(col("word") != "")
        .select("word")
    )

    window_spec_fake_order = Window.orderBy(lit(1))

    bigrams_df = (
        words_df
        .withColumn(
            "word",
            concat(
                col("word"),
                lit(" "),
                lead(col("word"), 1, None).over(window_spec_fake_order)
            )
        )
        .dropna()
    )

    return bigrams_df


def count_unique_bigrams(bigrams_df: DataFrame) -> DataFrame:
    """Count unique bigrams.

    Args:
        bigrams_df: Spark dataframe with bigrams.

    Returns:
        Spark dataframe.
    """
    counted_unique_bigrams_df = (
        bigrams_df
        .groupBy("word")
        .count()
        .orderBy(col("count").desc())
    )

    return counted_unique_bigrams_df


def load_data(
    bigrams_df: DataFrame,
    counted_unique_bigrams_df: DataFrame,
    args: dict
) -> None:
    """Load results into stdout and csv file.

    Args:
        words_df: Spark dataframe with separate words.
        counted_unique_words_df: Spark dataframe with counted unique words.
        show_count: Number of displayed rows of the dataframe. Default is 10.

    Returns:
        None.
    """
    print("total_bigrams")
    print(bigrams_df.count())
    print("bigram_counts")
    counted_unique_bigrams_df.show(10, False)

    (
        counted_unique_bigrams_df
        .toPandas()
        .to_csv(args["output_file"], sep=",", index=False)
    )


def get_args() -> dict:
    """Obtain and return arguments from the calling of the script.

    Returns:
        Dictionary with arguments.
    """
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--data-dir",
        type=str,
        required=True,
        metavar="<data_dir_path>",
        help="Path to the directory with source files.",
    )
    parser.add_argument(
        "--output-file",
        type=str,
        required=True,
        metavar="<output_file_path>",
        help="Path to the output file.",
    )

    return vars(parser.parse_args())


if __name__ == "__main__":
    main()
