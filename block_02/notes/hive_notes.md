# Hive

## Cloudera quickstart docker

Import the Cloudera QuickStart image from Docker Hub:

```bash
docker pull cloudera/quickstart:latest
```

List docker images:

```bash
docker images
```

Take image ID:

```bash
REPOSITORY            TAG       IMAGE ID       CREATED       SIZE
cloudera/quickstart   latest    4239cd2958c6   7 years ago   6.34GB
```

Run docker image (includes Hadoop, Hive, Spark and HBase):

```bash
docker run --hostname=quickstart.cloudera --privileged=true -t -i -p 8888:8888 -p 7180:7180 -p 80:80 4239cd2958c6 /usr/bin/docker-quickstart
```

## HQL examples

First enter Hive CLI:

```bash
# Just print "hive"
hive
```

Then do some HQL commands (in Hive CLI):

```sql
-- -------------------------------------------------------------------------------------
-- Create tables.
create table transactions (
    t_dat             date,
    customer_id       string,
    article_id        int,
    price             decimal(22, 20),
    sales_channel_id  smallint
)
row format delimited
fields terminated by ','
stored as textfile;
-- -------------------------------
create table customers (
    customer_id             string,
    fn                      decimal(2, 1),
    active                  decimal(2, 1),
    club_member_status      string,
    fashion_news_frequency  string,
    age                     smallint,
    postal_code             string
)
row format delimited
fields terminated by ','
stored as textfile;
-- -------------------------------
create table customer_transactions (
    customer_id         string,
    age                 smallint,
    transactions_count  int
);
-- -------------------------------------------------------------------------------------
-- List tables.
show tables;
-- -------------------------------------------------------------------------------------
-- Detailed info about specific table.
describe transactions;
describe customers;
describe customer_transactions;
-- -------------------------------------------------------------------------------------
-- Load csv data into tables.
load data local inpath '/home/cloudera/transactions.csv'
overwrite into table transactions;
-- -------------------------------
load data local inpath '/home/cloudera/customers.csv'
overwrite into table customers;
-- -------------------------------------------------------------------------------------
-- Join transactions and customers. Count transactions for each customer.
-- Insert the result of the query into the customer_transactions table.
insert overwrite table 
    customer_transactions
select
    c.customer_id,
    c.age,
    count(1)
from
    customers c
    join transactions t on t.customer_id = c.customer_id
group by
    c.customer_id,
    c.age;
-- -------------------------------------------------------------------------------------   
```

Check the imported and joined data:

```bash
hdfs dfs -cat /user/hive/warehouse/transactions/transactions.csv | head
hdfs dfs -cat /user/hive/warehouse/customers/customers.csv | head
```

Finally do some HQL DML queries (in Hive CLI):

```sql
-- -------------------------------
select * from transactions limit 10;
-- -------------------------------
select * from customers limit 10;
-- -------------------------------
select * from customer_transactions limit 10;
-- -------------------------------
select count(1) from transactions;
-- -------------------------------
select
    *
from 
    transactions
where
    customer_id = '00007d2de826758b65a93dd24ce629ed66842531df6699338c5570910a014cc2'
limit
    3;
-- -------------------------------
```

## Word count example

```sql
-- -------------------------------------------------------------------------------------
create table words (
    line string
);
-- -------------------------------------------------------------------------------------
load data local inpath '/home/cloudera/words.txt'
overwrite into table words; 
-- -------------------------------------------------------------------------------------
create table word_counts as 
    select
        word,
        count(1) as count
    from (
        select
            explode(split(line, ' ')) as word
        from
            words
    ) w
    group by
        word
    order by 
        count desc;
-- -------------------------------------------------------------------------------------
select * from word_counts limit 10;
```
