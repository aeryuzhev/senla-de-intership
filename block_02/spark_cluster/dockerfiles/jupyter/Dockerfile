FROM jupyter/pyspark-notebook:python-3.10

USER root

RUN wget https://archive.apache.org/dist/spark/spark-3.3.3/spark-3.3.3-bin-hadoop3.tgz \
    && tar zxvf spark-3.3.3-bin-hadoop3.tgz \
    && rm -rf spark-3.3.3-bin-hadoop3.tgz \
    && mv spark-3.3.3-bin-hadoop3/ /usr/local/ \
    && rm -rf /usr/local/spark \
    && rm -rf /usr/local/spark-3.3.0-bin-hadoop3 \
    && ln -s /usr/local/spark-3.3.3-bin-hadoop3 /usr/local/spark \
    && pip install pandas
