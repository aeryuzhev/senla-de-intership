<!-- markdownlint-disable MD024 -->
# Block 02 - Hadoop, Spark, Docker

## Prerequisites

You can use Docker to create a work environment:

- [Spark in standalone cluster mode](spark_cluster/readme.md)
- [Spark in local mode](spark_local/readme.md)

## Task 01

### Description

The task is to create a data mart using only PySpark (without using Spark SQL queries). It must consists of the following columns:

| Field name               | Description                                            |
| ------------------------ | ------------------------------------------------------ |
| part_date                | The last day of the month                              |
| customer_id              | Customer ID                                            |
| customer_group_by_age    | Customers age classification                           |
| transaction_amount       | Total sum of purchases                                 |
| most_exp_article_id      | The most expensive article                             |
| number_of_articles       | The number of articles                                 |
| number_of_product_groups | The number of product groups                           |

### Technologies

- PySpark

### Scripts

- [task_01.py](spark_cluster/scripts/task_01.py)

### Data sources

Kaggle - H&M Personalized Fashion Recommendations:

- [articles.csv](https://www.kaggle.com/competitions/h-and-m-personalized-fashion-recommendations/data?select=articles.csv)
- [customers.csv](https://www.kaggle.com/competitions/h-and-m-personalized-fashion-recommendations/data?select=articles.csv)
- [transactions_train.csv](https://www.kaggle.com/competitions/h-and-m-personalized-fashion-recommendations/data?select=articles.csv)

### Usage

```bash
${SPARK_HOME}/bin/spark-submit \
    /home/jovyan/work/scripts/task_01.py \
        --part-date="2018-12-31" \
        --data-dir="/home/jovyan/work/data"
```

### Result

- [data_mart.csv](spark_cluster/data/data_mart.csv)

## Task 02

### Description

The task is to calculate unique words from multiple *.txt files with song lyrics.

### Technologies

- PySpark

### Data sources

Kaggle - Song Lyrics

- <https://www.kaggle.com/datasets/paultimothymooney/poetry/download>

### Scripts

- [task_02.py](spark_cluster/scripts/task_02.py)

### Usage

```bash
${SPARK_HOME}/bin/spark-submit \
    /home/jovyan/work/scripts/task_02.py \
        --data-dir="/home/jovyan/work/data/song_lyrics" \
        --output-file="/home/jovyan/work/data/word_counts.csv"
```

### Result

- [word_counts.csv](spark_cluster/data/word_counts.csv)

## Task 03

### Description

The task is to calculate unique bigrams (a pair of two consecutive words) from multiple *.txt files with song lyrics.

### Technologies

- PySpark

### Data sources

Kaggle - Song Lyrics

- <https://www.kaggle.com/datasets/paultimothymooney/poetry/download>

### Scripts

- [task_03.py](spark_cluster/scripts/task_03.py)

### Usage

```bash
${SPARK_HOME}/bin/spark-submit \
    /home/jovyan/work/scripts/task_03.py \
        --data-dir="/home/jovyan/work/data/song_lyrics" \
        --output-file="/home/jovyan/work/data/bigram_counts.csv"
```

### Result

- [bigram_counts.csv](spark_cluster/data/bigram_counts.csv)
