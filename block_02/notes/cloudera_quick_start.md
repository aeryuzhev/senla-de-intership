# Cloudera quickstart docker

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
