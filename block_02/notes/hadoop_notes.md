# Hadoop

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

## HDFS commands

[Hadoop HDFS commands cheatsheet](https://medium.com/geekculture/hdfs-commands-cheat-sheet-1cd7bf22e795)

```bash
# List directory contents.
hdfs dfs -ls /
hdfs dfs -ls hdfs://localhost/user/
hdfs dfs -ls file:///home/cloudera/

# Hdfs file schema is by default.
hdfs dfs -ls /user/

# Make directory.
hdfs dfs -mkdir /learn.

# Copy small files.
hdfs dfs -cp /learn/file /dir/file

# Copy big files.
hdfs distcp /learn/file /dir/file

# Copy from local to hdfs.
hdfs dfs -put /home/user/file /learn/file

# Copy from hdfs to local.
hdfs dfs -get /learn/file /home/user/file

# Move or rename file.
hdfs dfs -mv /learn/file /learn/file_new

# Delete file to bin bucket.
hdfs dfs -rm /learn/file

# Delete directory recursively to bin bucket.
hdfs dfs -rm -r /learn

# File or directory size.
hdfs dfs -du -h /learn

# Check the health of the HDFS or HDFS directory.
hdfs fsck /
hdfs fsck /learn

# Help.
hdfs dfs -help <command>
```
