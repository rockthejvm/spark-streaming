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
| Kafka images | wurstmeister (Zookeeper) | Confluent 7.9.0 (KRaft, no Zookeeper) |
| PostgreSQL | 9.6 | 17 |
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

- **Removed**: `version: '3'` top-level field (deprecated in Docker Compose v2)
- **Removed**: Cassandra container
- **Removed**: Zookeeper container (Kafka now uses KRaft mode)
- **Added**: MongoDB 7 container (port 27017)
- **Updated**: Kafka from wurstmeister to Confluent 7.9.0 with KRaft (single container, no Zookeeper needed)
- **Updated**: PostgreSQL from 9.6 to 17

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

---

## Video Production Plan

### Videos to DELETE (old lessons with no equivalent)

| Old Lesson | Reason |
|---|---|
| 3.1 DStreams | DStreams removed |
| 3.2 DStreams Transformations | DStreams removed |
| 3.3 DStreams Window Transformations | DStreams removed |
| 4.2 Kafka DStreams | DStreams removed |
| 4.4 Akka Integration | Akka removed, no replacement lesson |
| 4.5 Cassandra Integration | Cassandra removed, replaced by MongoDB (new video) |
| 5.1 Custom Socket Receiver | DStreams custom receiver removed |
| 5.2 Twitter Receiver | Twitter project removed |
| 5.3 Twitter Exercises | Twitter project removed |
| 5.4 Sentiment Analysis | Twitter project removed |
| 6.4 Stateful Computations | mapGroupsWithState replaced by transformWithState (new video) |
| 7.1 Science HTTP + Kafka | Science project removed |
| 7.2 Science Spark Aggregator | Science project removed |

**Total: 13 videos removed**

### Videos to KEEP AS-IS (code unchanged, lesson still valid)

| Lesson | File | Why it works |
|---|---|---|
| 1.1 Scala Recap | `ScalaRecap.scala` | Pure Scala, no API changes |
| 1.2 Spark Recap | `SparkRecap.scala` | Spark SQL basics unchanged |
| 2.2 Streaming Aggregations | `StreamingAggregations.scala` | Code identical in Spark 4 |
| 2.3 Streaming Datasets | `StreamingDatasets.scala` | Code identical in Spark 4 |
| 2.4 Streaming Joins | `StreamingJoins.scala` | Code identical in Spark 4 |
| 4.1 Event Time Windows | `EventTimeWindows.scala` | Code identical in Spark 4 |
| 4.2 Processing Time Windows | `ProcessingTimeWindows.scala` | Code identical in Spark 4 |
| 4.3 Watermarks | `Watermarks.scala` | Code identical in Spark 4 |

**Total: 8 videos kept**

### Videos to PATCH (minor edit in post, no re-record)

| Lesson | File | What to patch |
|---|---|---|
| 2.1 Streaming DataFrames | `StreamingDataFrames.scala` | Add a text overlay or voiceover on the triggers section: mention `Trigger.AvailableNow()` replaces `Trigger.Once()`, and `Trigger.RealTime()` is the new continuous mode. The rest of the lesson (socket source, file source, basic writeStream) is identical. |

**Total: 1 video patched**

### Videos to RE-RECORD (same topic, but code or context changed enough to need a new take)

| Lesson | File | What changed |
|---|---|---|
| 3.1 Kafka Integration | `IntegratingKafka.scala` | Code is identical, but the lesson number changed (was 4.1) and docker setup is different (KRaft, no Zookeeper). Re-record the setup portion showing the new docker-compose and Kafka without Zookeeper. The Spark code walkthrough can be the same. |
| 3.2 JDBC / PostgreSQL | `IntegratingJDBC.scala` | Code is identical, but lesson number changed (was 4.3) and PostgreSQL version upgraded. Brief re-record showing the new docker-compose and updated connection. |

**Total: 2 videos re-recorded**

### Videos to RECORD FROM SCRATCH (new lessons)

| Lesson | File | Content | Est. Duration |
|---|---|---|---|
| 3.3 MongoDB Integration | `IntegratingMongoDB.scala` | foreachBatch + native `format("mongodb")` write patterns | ~20 min |
| 4.4 Transform With State | `TransformWithState.scala` | StatefulProcessor, ValueState, ListState, MapState, TTL, timers — 5 progressive examples | ~35 min |
| 4.5 State Data Source | `StateDataSource.scala` | `state-metadata` and `statestore` formats, debugging checkpoint state | ~15 min |
| 4.6 Real-Time Mode | `RealTimeMode.scala` | `Trigger.RealTime()`, Kafka-to-Kafka pipeline, latency measurement, comparison with micro-batch | ~20 min |
| 5.1 GitHub Events Simulator | `GitHubEventsSimulator.scala` | Kafka producer, event generation, out-of-order timestamps, bot burst mode | ~20 min |
| 5.2 Events Ingestion | `GitHubEventsIngestion.scala` | Kafka source, JSON parsing, PostgreSQL sink, `Trigger.AvailableNow` backfill | ~20 min |
| 5.3 Events Analytics | `GitHubEventsAnalytics.scala` | Tumbling/sliding windows, watermarks, stream-stream join | ~25 min |
| 5.4 Stateful Processing | `GitHubEventsStateful.scala` | transformWithState with all state types, TTL, timers, anomaly detection, MongoDB sink | ~35 min |

**Total: 8 new videos (~3h 10min raw recording)**

### Summary

| Category | Count |
|---|---|
| Deleted | 13 |
| Kept as-is | 8 |
| Patched (minor edit) | 1 |
| Re-recorded | 2 |
| New from scratch | 8 |
| **Final course total** | **19 videos** |
