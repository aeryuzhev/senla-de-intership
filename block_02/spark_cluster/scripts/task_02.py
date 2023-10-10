"""
This Python module extracts data from the multiple text files into the Spark dataframe,
splits the data into separate words, counts unique words and output the result into
stdout.

Link to the data sources:
    https://www.kaggle.com/datasets/paultimothymooney/poetry/data

Args:
    --data-dir: Path to the directory with source files.

Example:
    ${SPARK_HOME}/bin/spark-submit \
        /home/jovyan/work/scripts/task_02.py \
            --data-dir="/home/jovyan/work/data/song_lyrics"
"""

import argparse

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, explode, split, regexp_replace


def main() -> None:
    """Main ETL function.

    Returns:
        None.
    """
    args = get_args()

    spark = initialize_spark()

    song_lyrics_df = extract_song_lyrics(spark, args)

    words_df = split_song_lyrics_into_words(song_lyrics_df)
    counted_unique_words_df = count_unique_words(words_df)

    load_data(words_df, counted_unique_words_df)

    spark.stop()


def initialize_spark() -> SparkSession:
    """Initialize a new SparkSession.

    Returns:
        SparkSession object.
    """
    spark = (
        SparkSession.builder
        .master("spark://spark-master:7077")
        .appName("word-count")
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


def split_song_lyrics_into_words(song_lyrics_df: DataFrame) -> DataFrame:
    """Split the flat text into separate words. Remove all non-alphanumeric characters
    from the beginning and end of the word.

    Args:
        song_lyrics_df: Spark dataframe with song lyrics data.

    Returns:
        Spark dataframe.
    """
    words_df = (
        song_lyrics_df
        .withColumn("word", explode(split(col("value"), r"\s+")))
        .withColumn("word", regexp_replace(col("word"), r"^\W+|\W+$", ""))
        .where(col("word") != "")
    )

    return words_df


def count_unique_words(words_df: DataFrame) -> DataFrame:
    """Count unique words.

    Args:
        words_df: Spark dataframe with separate words.

    Returns:
        Spark dataframe.
    """
    counted_words_df = (
        words_df
        .groupBy("word")
        .count()
        .orderBy(col("count").desc())
    )

    return counted_words_df


def load_data(words_df: DataFrame, counted_unique_words_df: DataFrame) -> None:
    """Load result to stdout.

    Args:
        words_df: Spark dataframe with separate words.
        counted_unique_words_df: Spark dataframe with counted unique words.

    Returns:
        None.
    """
    print("total_words")
    print(words_df.count())

    print("word_counts")
    counted_unique_words_df.show(10, False)


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

    return vars(parser.parse_args())


if __name__ == "__main__":
    main()
