"""
This Python module extracts data from multiple text files into the Spark dataframe,
splits the data into separate words, counts unique words and output the result into
stdout.

Link to the data sources:
    https://www.kaggle.com/datasets/paultimothymooney/poetry/data

Args:
    --data-dir: Path to the directory with source files.
    --output-file: Path to the output file.

Example:
    ${SPARK_HOME}/bin/spark-submit \
        /home/jovyan/work/scripts/task_02.py \
            --data-dir="/home/jovyan/work/data/song_lyrics" \
            --output-file="/home/jovyan/work/data/word_counts.csv"
"""
import sys
from pathlib import Path
import argparse

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    explode,
    split,
    regexp_replace,
    lower
)

SPLIT_PATTERN = r"[\s_]+|[^\w-'`*:$&.]+|\.\.\.*"
REPLACE_PATTERN = r"^\W+|\W+$"


def main() -> None:
    """Main ETL function.

    Returns:
        None.
    """
    args = get_args()
    check_paths(args)

    spark = initialize_spark()

    song_lyrics_df = extract_song_lyrics(spark, args)

    words_df = split_song_lyrics_into_words(song_lyrics_df)
    counted_unique_words_df = count_unique_words(words_df)

    load_data(counted_unique_words_df, args)

    spark.stop()


def check_paths(args: dict) -> None:
    """Check for the existing of the source data directory and the output file directory.
    Exit if paths do not exist.

    Args:
        args: Dictionary of arguments (from the get_args function).

    Returns:
        None.
    """
    data_dir_path = Path(args["data_dir"])
    output_file_dir_path = Path(args["output_file"]).parent

    if not data_dir_path.exists():
        sys.exit(f"Path does not exist: {str(data_dir_path)}")

    if not output_file_dir_path.exists():
        sys.exit(f"Path does not exist: {str(output_file_dir_path)}")


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
    """Split flat text into separate words. Remove all non-alphanumeric characters
    from the beginning and end of the word.

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


def load_data(counted_unique_words_df: DataFrame, args: dict) -> None:
    """Load results into csv file.

    Args:
        counted_unique_words_df: Spark dataframe with counted unique words.
        args: Dictionary of arguments (from the get_args function).

    Returns:
        None.
    """
    (
        counted_unique_words_df
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
