# Hadoop

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
