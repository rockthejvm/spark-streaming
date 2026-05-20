# Rock the JVM - Spark Streaming with Scala

Companion repository for the [Rock the JVM Spark Streaming with Scala](https://rockthejvm.com/courses/apache-spark-streaming-with-scala) course.

## Prerequisites

- **Java 17+** (Java 21 also supported)
- **Scala 2.13**
- **SBT 1.10+**
- **Docker** and **Docker Compose**
- **IntelliJ IDEA** (recommended) with the Scala plugin

## How to install

1. Clone the repo or download as zip
2. Open with IntelliJ as an SBT project
3. In a terminal, navigate to the repo folder and run `docker compose up -d` to start the infrastructure containers (Kafka, PostgreSQL, MongoDB)

## Infrastructure

The `docker-compose.yml` provides:
- **PostgreSQL 16** on port 5432 (user: `docker`, password: `docker`, database: `rtjvm`)
- **MongoDB 7** on port 27017 (database: `rtjvm`)
- **Apache Kafka** on port 9092 (with Zookeeper on port 2181)

## Course Structure

### Part 1: Recap
- 1.1 Scala Recap
- 1.2 Spark Recap

### Part 2: Structured Streaming Fundamentals
- 2.1 Streaming DataFrames
- 2.2 Streaming Aggregations
- 2.3 Streaming Datasets
- 2.4 Streaming Joins

### Part 3: Integrations
- 3.1 Kafka (Structured Streaming)
- 3.2 JDBC (PostgreSQL)
- 3.3 MongoDB

### Part 4: Advanced Streaming
- 4.1 Event Time Windows
- 4.2 Processing Time Windows
- 4.3 Watermarks
- 4.4 Transform With State (Spark 4 stateful API)
- 4.5 State Data Source (debugging streaming state)
- 4.6 Real-Time Mode (Spark 4.1 sub-millisecond latency)

### Part 5: Capstone Project — Real-Time GitHub Events Analytics
- 5.1 GitHub Events Simulator (Kafka producer)
- 5.2 Events Ingestion (Kafka to console/PostgreSQL)
- 5.3 Events Analytics (windows, watermarks, stream-stream joins)
- 5.4 Stateful Processing (transformWithState, timers, TTL, anomaly detection)

## For questions or suggestions

If you have changes to suggest to this repo, either
- submit a GitHub issue
- tell me in the course Q/A forum
- submit a pull request!
