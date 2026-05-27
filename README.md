# Rock the JVM - Spark Streaming with Scala

This repository contains the code we wrote during  [Rock the JVM's Spark Streaming with Scala](https://rockthejvm.com/courses/apache-spark-streaming-with-scala). Unless explicitly mentioned, the code in this repository is exactly what was caught on camera.

## Prerequisites

- **Java 17+** (Java 21 also supported)
- **Scala 2.13**
- **SBT 1.10+**
- **Docker** and **Docker Compose**
- **IntelliJ IDEA** (recommended) with the Scala plugin
=======

## How to install

1. Clone the repo or download as zip
2. Open with IntelliJ as an SBT project
3. In a terminal, navigate to the repo folder and run `docker compose up -d` to start the infrastructure containers (Kafka, PostgreSQL, MongoDB)

## Infrastructure

The `docker-compose.yml` provides:
- **PostgreSQL 16** on port 5432 (user: `docker`, password: `docker`, database: `rtjvm`)
- **MongoDB 7** on port 27017 (database: `rtjvm`)
- **Apache Kafka** on port 9092 (with Zookeeper on port 2181)

## For questions or suggestions

If you have changes to suggest to this repo, either
- submit a GitHub issue
- tell me in the course Q/A forum
- submit a pull request!
