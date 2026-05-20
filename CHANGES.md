# Course Update: Spark Streaming 3.0.2 → 4.1.1

## Overview

This update migrates the entire course from Spark 3.0.2 / Scala 2.12 / Java 8 to Spark 4.1.1 / Scala 2.13.17 / Java 17+. It removes all deprecated DStream-based content, dead integrations (Twitter, Cassandra, Akka), and replaces them with Spark 4's new features — `transformWithState`, State Data Source, Real-Time Mode — and a modern capstone project.

---

## Platform Changes

| Component | Before | After |
|-----------|--------|-------|
| Spark | 3.0.2 | 4.1.1 |
| Scala | 2.12.10 | 2.13.17 |
| Java | 8+ | 17+ (21 also supported) |
| SBT | 1.3.8 | 1.10.7 |
| Kafka images | wurstmeister | Confluent 7.6.0 |
| PostgreSQL | 9.6 | 16 |
| NoSQL | Cassandra 3.0.0 | MongoDB 7 (connector 10.4.0) |

---

## Removed Content

### Entire Sections Removed
- **Part 3: Low-Level DStreams** (3 lessons) — DStreams are deprecated in Spark 4
- **Part 5: Twitter Project** (5 files) — Twitter API is no longer free
- **Part 7: Science Project** (2 files) — replaced by new capstone

### Individual Files Removed
- `IntegratingKafkaDStreams.scala` — DStreams Kafka connector
- `IntegratingAkka.scala` — Akka Classic is end-of-life, integration removed
- `IntegratingCassandra.scala` — spark-cassandra-connector has no Spark 4 support
- `StatefulComputations.scala` — `mapGroupsWithState`/`flatMapGroupsWithState` replaced by `transformWithState`

### Deleted Resources
- `src/main/resources/akkaconfig/` — Akka config removed
- `src/main/resources/twitter4j.properties`
- `flume/` directory
- `cql.sh` (Cassandra setup script)

### Removed Dependencies
- `spark-streaming` (DStreams library)
- `spark-streaming-kafka-0-10` (DStreams Kafka)
- `akka-remote`, `akka-stream`, `akka-http` (Akka removed entirely)
- `spark-cassandra-connector`
- `twitter4j-core`, `twitter4j-stream`
- `stanford-corenlp` (sentiment analysis)
- `kafka` (Scala wrapper — only `kafka-clients` needed now)

---

## Structural Changes

### Old Structure (7 parts, 32 files)
```
Part 1: Recap (2 lessons)
Part 2: Structured Streaming (4 lessons)
Part 3: Low-Level DStreams (3 lessons)          ← REMOVED
Part 4: Integrations (5 lessons)
Part 5: Twitter Project (5 files)              ← REMOVED
Part 6: Advanced (4 lessons)
Part 7: Science Project (2 files)              ← REMOVED
```

### New Structure (5 parts, 18 lessons)
```
Part 1: Recap (2 lessons)
Part 2: Structured Streaming (4 lessons)
Part 3: Integrations (2 lessons)               ← renumbered, Kafka + JDBC kept, MongoDB new
Part 4: Advanced Streaming (6 lessons)         ← renumbered, 3 new Spark 4 lessons
Part 5: Capstone — GitHub Events (4 lessons)   ← ALL NEW
```

---

## Updated Files

### Part 2
- **StreamingDataFrames.scala** — updated trigger section: added `Trigger.AvailableNow()` (replaces deprecated `Trigger.Once()`), added `Trigger.RealTime()` reference (Spark 4.1 continuous processing)

### Playground
- **Playground.scala** — rewritten to use Structured Streaming (removed `StreamingContext` and DStream queue)

---

## New Files

### Part 3: Integrations
- **IntegratingMongoDB.scala** — replaces `IntegratingCassandra.scala`. Uses MongoDB Spark Connector 10.x. Shows two patterns: foreachBatch and native `format("mongodb")` streaming write.

### Part 4: Advanced Streaming
- **TransformWithState.scala** — flagship Spark 4 lesson. Covers the full `StatefulProcessor` API:
  - `ValueState` — basic accumulation
  - `ListState` — sliding window of recent records
  - `MapState` — counting by sub-category
  - TTL-based state expiration (auto-cleanup after 30s)
  - Processing-time timers (periodic summaries every 10s)
  - Includes `TransformWithStateSender` object for deterministic socket-based testing
- **StateDataSource.scala** — reading/debugging streaming state:
  - `spark.read.format("state-metadata")` — operator info, batch IDs
  - `spark.read.format("statestore")` — actual key-value state contents
  - Inspecting state at specific batch IDs
- **RealTimeMode.scala** — Spark 4.1 headline feature:
  - `Trigger.RealTime()` — continuous event-at-a-time processing (replaces experimental `Trigger.Continuous`)
  - Single-digit millisecond latency for stateless queries (vs ~100ms+ for micro-batch)
  - Exactly-once semantics (unlike old Continuous which was at-least-once)
  - Kafka-to-Kafka pipeline, ForeachWriter latency measurement, stateless filter
  - Side-by-side comparison with `Trigger.ProcessingTime`
  - Current limitations documented: Kafka only, stateless only, Update output mode

### Part 5: Capstone — Real-Time GitHub Events Analytics
- **GitHubEventsSimulator.scala** — standalone Kafka producer that generates realistic GitHub events (PushEvent, WatchEvent, ForkEvent, etc.) with controlled out-of-order timestamps for watermark demos. Includes a "bot burst" mode for anomaly detection testing.
- **GitHubEventsIngestion.scala** — Kafka → JSON parsing → typed Dataset → PostgreSQL via foreachBatch. Also demonstrates `Trigger.AvailableNow()` for backfill.
- **GitHubEventsAnalytics.scala** — event-time tumbling/sliding windows, watermarks, stream-stream joins (push-then-star correlation), event type distribution.
- **GitHubEventsStateful.scala** — full `transformWithState` with `ValueState` (total events), `ListState` (recent events), `MapState` (events by type), 10-minute TTL, periodic timer summaries, anomaly detection (bot flagging), MongoDB persistence for alerts.

### Supporting Files
- **common/GitHubEvent.scala** — domain model: `GitHubEvent`, `UserActivitySummary`, `GitHubAlert`
- **src/main/resources/data/github-events/** — sample JSON event files for file-source testing
- **sql/init.sql** — PostgreSQL table definitions for `cars` and `github_events`

---

## Docker Compose Changes

- **Removed**: Cassandra container
- **Added**: MongoDB 7 container (port 27017)
- **Updated**: Kafka/Zookeeper from wurstmeister to Confluent 7.6.0 images (better maintained, more reliable)
- **Updated**: PostgreSQL from 9.6 to 16
- **Simplified**: Kafka listener config (single PLAINTEXT listener)

---

## Key API Migrations

| Old API (Spark 3) | New API (Spark 4) |
|---|---|
| `StreamingContext` + `DStream` | Removed entirely |
| `Trigger.Once()` | `Trigger.AvailableNow()` |
| `Trigger.Continuous` (at-least-once) | `Trigger.RealTime()` (exactly-once) |
| `mapGroupsWithState` | `transformWithState` with `StatefulProcessor` |
| `flatMapGroupsWithState` | `transformWithState` with `StatefulProcessor` |
| `GroupState[S]` | `ValueState[T]`, `ListState[T]`, `MapState[K,V]` |
| Manual state timeout | TTL-based automatic expiration |
| No timer support | `registerTimer` / `handleExpiredTimer` |
| No state inspection | `state-metadata` / `statestore` data sources |

---

## Build Requirements

Students need:
- JDK 17 or 21 installed
- Docker and Docker Compose
- IntelliJ IDEA with Scala plugin (Scala 2.13 support)
- ~4GB RAM for Spark local mode + Docker containers
