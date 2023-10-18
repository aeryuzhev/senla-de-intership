# Docker for Spark standalone cluster

## Description

A docker-compose file for creating an environment with:

- Spark master node
- Spark worker nodes
- Jupyter Notebook

## Usage

```bash
# Creates an environment with 1 master and 2 worker nodes.
docker-compose up --scale spark-worker=2 -d
```
