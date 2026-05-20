# Capstone Project Teaching Guide: Real-Time GitHub Events Analytics

## Project Concept

Students build a real-time analytics platform that ingests simulated GitHub activity events, processes them through multiple streaming stages, and produces live insights — trending repositories, user activity summaries, and bot detection alerts.

The project integrates nearly every concept from the course: Kafka, JSON parsing, typed Datasets, windowed aggregations, watermarks, stream-stream joins, `transformWithState` with all state types, TTL, timers, and multi-sink output (console, PostgreSQL, MongoDB).

---

## Prerequisites for Students

Before starting Part 5, students must have completed:
- Part 2 (Structured Streaming fundamentals)
- Part 3 (Kafka and JDBC integrations)
- Part 4 (Windows, watermarks, transformWithState)

Docker containers must be running: `docker compose up -d`

---

## Lesson 5.1: GitHub Events Simulator

### Teaching Goal
Show students how to build a data source that simulates a real-world public API and feeds a Kafka topic.

### Setup (do before recording)
- Docker containers running (Kafka on port 9092)
- Create the Kafka topic: `docker exec rockthejvm-sparkstreaming-kafka kafka-topics --create --topic github-events --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1`

### Step-by-Step Video Script

**1. Introduce the domain (2 min)**
- Show the real GitHub Events API: `https://api.github.com/events`
- Open it in a browser — show the JSON structure: id, type, actor.login, repo.name, created_at
- Explain: "We'll simulate this locally so it works offline and we control the rate"

**2. Create the GitHubEvent model (3 min)**
- Open `common/GitHubEvent.scala`
- Walk through the case classes: `GitHubEvent`, `UserActivitySummary`, `GitHubAlert`
- Explain why we flatten the nested JSON (actor.login → actorLogin) for easier Spark processing

**3. Build the simulator (10 min)**
- Open `GitHubEventsSimulator.scala`
- Start with the data generators:
  - `eventTypes` list — explain each GitHub event type briefly
  - `actors` and `repos` lists — our simulated users and repositories
  - `generateEvent()` — builds a JSON string matching the GitHub API format
- Show the jitter logic: "15% of events get a negative timestamp offset — this simulates network delay and out-of-order arrival, which we'll handle with watermarks later"

**4. Build the Kafka producer (5 min)**
- Walk through `createKafkaProducer()` — standard Kafka `Properties` setup
- Walk through `produceToKafka()`:
  - Configurable rate (events/second) and duration
  - Progress logging every 50 events
  - Proper cleanup with `flush()` and `close()`

**5. Demonstrate the bot burst mode (3 min)**
- Show `produceBotBurst()` — 50 events from one user in rapid succession
- Explain: "We'll use this in lesson 5.4 to test our anomaly detection"

**6. Run it live (3 min)**
- Run `GitHubEventsSimulator.main()` in IntelliJ
- Open a terminal and consume from Kafka to verify:
  ```
  docker exec rockthejvm-sparkstreaming-kafka kafka-console-consumer --topic github-events --bootstrap-server localhost:9092
  ```
- Show the JSON events flowing

### Key Teaching Points
- Kafka producers are simple: Properties + `ProducerRecord` + `send()`
- Simulating realistic data (out-of-order, bursts) makes demos more instructive than perfect data
- The simulator is a standalone `main` — students run it in a separate terminal/process while Spark consumes

---

## Lesson 5.2: Events Ingestion

### Teaching Goal
Connect Kafka to Spark Structured Streaming, parse JSON, and write to PostgreSQL. Also demonstrate `Trigger.AvailableNow()`.

### Setup
- Simulator running (from 5.1) or has already produced some events
- PostgreSQL running with the `github_events` table (from `sql/init.sql`)

### Step-by-Step Video Script

**1. Define the schema (3 min)**
- Open `GitHubEventsIngestion.scala`
- Walk through `gitHubEventSchema` — StructType matching the JSON structure
- Note the nested structs for `actor` and `repo`
- Explain: "Spark needs to know the schema upfront for JSON parsing"

**2. Build the Kafka reader (5 min)**
- Walk through `readFromKafka()`:
  - `format("kafka")` + bootstrap servers + topic subscription
  - `startingOffsets = "latest"` — only new events
  - Cast value bytes to String
  - `from_json()` with our schema
  - Flatten nested fields with `.select(col("event.actor.login").as("actorLogin"), ...)`
  - Cast to typed `Dataset[GitHubEvent]`
- Explain the Kafka DataFrame columns: key, value, topic, partition, offset, timestamp

**3. Basic console output (3 min)**
- Run `logEvents()` with the simulator running in another terminal
- Show events appearing in the console
- Show `logCodeEvents()` — filtering to just PushEvent and PullRequestEvent

**4. Write to PostgreSQL (5 min)**
- Walk through `writeToPostgres()`:
  - `foreachBatch` — explain "each micro-batch becomes a static DataFrame we can write anywhere"
  - JDBC options: driver, url, user, password, dbtable
  - `mode("append")`
- Run it, then query PostgreSQL:
  ```
  docker exec -it rockthejvm-sparkstreaming-postgres psql -U docker -d rtjvm -c "SELECT * FROM github_events LIMIT 10;"
  ```
- Show data landing in the database

**5. Backfill with Trigger.AvailableNow (5 min)**
- Explain the use case: "What if the streaming job was down and you need to catch up on all missed data?"
- Walk through `backfillFromKafka()`:
  - `startingOffsets = "earliest"` — read everything from the beginning
  - `Trigger.AvailableNow()` — processes all available data in micro-batches, then stops
  - Contrast with deprecated `Trigger.Once()` — AvailableNow handles multiple batches and advances watermarks correctly
- Run it and show it processes all events then terminates cleanly

### Key Teaching Points
- Kafka integration uses the same readStream/writeStream pattern as socket sources
- `from_json` + schema is the standard way to parse JSON in streaming
- `foreachBatch` is the universal adapter for writing to any system Spark doesn't natively support as a sink
- `Trigger.AvailableNow` is the Spark 4 way to do batch-style catchup on streaming sources

---

## Lesson 5.3: Events Analytics

### Teaching Goal
Apply windowed aggregations, watermarks, and stream-stream joins on real-ish streaming data.

### Setup
- Simulator running at a steady rate (5 events/sec)

### Step-by-Step Video Script

**1. Events per repo per window — tumbling windows (7 min)**
- Walk through `eventsPerRepoPerWindow()`:
  - `withWatermark("createdAt", "30 seconds")` — tolerate 30s of out-of-order data
  - `window(col("createdAt"), "5 minutes")` — 5-minute tumbling windows
  - Aggregate: `count("*")` and `countDistinct("actorLogin")`
  - Extract window start/end for readable output
- Run with simulator active — show windows filling up
- Point out: "The watermark means results appear after the window closes + 30 seconds. No output during the window because we're in update mode — it only emits when the counts change"
- Exercise suggestion: "Try changing to 1-minute windows and see results more frequently"

**2. Trending repos — sliding windows (5 min)**
- Walk through `trendingRepos()`:
  - Filter to `WatchEvent` only (stars)
  - `window(col("createdAt"), "1 hour", "5 minutes")` — 1-hour window sliding every 5 minutes
  - Complete output mode — show the full ranking each time
- Run briefly, then explain: "In production this would feed a dashboard showing which repos are getting the most stars right now"

**3. Push-then-star correlation — stream-stream join (8 min)**
- This is the most complex part — walk through `pushToStarCorrelation()`:
  - Two streams from the same source: pushEvents and starEvents
  - Each has its own watermark
  - Join condition: same repo AND star happens within 10 minutes after push
  - Explain the join semantics: "Spark buffers both sides within the watermark window and matches them"
- Run and show correlated pairs appearing
- Explain: "This answers: when someone pushes code, does it attract stars shortly after?"

**4. Event type distribution (3 min)**
- Walk through `eventTypeDistribution()` — simple groupBy with 2-minute windows
- "This is your streaming equivalent of a GROUP BY in SQL, but computed incrementally over time"

### Key Teaching Points
- Watermarks are essential for windowed aggregations — without them, Spark would buffer state forever
- Sliding windows generate more output than tumbling windows (overlapping windows)
- Stream-stream joins require watermarks on both sides and a time-bounded condition
- Output mode matters: `update` for changing aggregates, `append` for finalized windows, `complete` for full re-output

---

## Lesson 5.4: Stateful Processing

### Teaching Goal
Build a sophisticated stateful processor that uses all features of `transformWithState`: multiple state types, TTL, timers, and anomaly detection.

### Setup
- Simulator running
- MongoDB running (for alert persistence)

### Step-by-Step Video Script

**1. Recap transformWithState basics (2 min)**
- Brief reminder from Part 4.4: StatefulProcessor, init/handleInputRows/handleExpiredTimer
- "Now we'll use ALL the state types together in a real scenario"

**2. Design the processor (5 min)**
- Open `GitHubEventsStateful.scala`
- Explain the `UserActivityProcessor` design on a whiteboard or diagram:
  - Key: `actorLogin` (each user gets independent state)
  - `ValueState[Long]` for totalEvents and lastEventTime
  - `ListState[String]` for the 10 most recent events (sliding window)
  - `MapState[String, Long]` for event counts by type
  - All with 10-minute TTL — inactive users auto-expire
  - Timer every 2 minutes for periodic summaries
  - Anomaly threshold: >20 events in one batch = suspicious

**3. Walk through init() (3 min)**
- Show how each state variable is declared `@transient` and initialized in `init()`
- Explain TTL: `TTLConfig(Duration.ofMinutes(10))` — "If no updates for 10 minutes, Spark automatically clears this user's state"
- Explain why `@transient` — state handles are not serializable; they're re-initialized per partition

**4. Walk through handleInputRows() (10 min)**
- Step by step:
  - Read existing state (with `exists()` guards)
  - Count events in this batch
  - Update `lastEventTime` with each event
  - Append to `recentEvents` (ListState) — show the `appendValue` method
  - Update per-type counts in `eventsByType` (MapState) — show `containsKey` + `getValue` + `updateValue`
  - Update totalEvents
  - Trim recentEvents to last 10 (read all → clear → re-append)
  - Register first timer on initial data arrival
  - Anomaly check: if batchCount > threshold, emit an alert
- "This is the most code-dense method — it's where all the business logic lives"

**5. Walk through handleExpiredTimer() (3 min)**
- "Every 2 minutes, this fires for each user that has state"
- Read accumulated state, build the type breakdown from MapState
- Re-register the timer for next cycle
- Emit a summary record

**6. Wire it up and run (5 min)**
- Show `trackUserActivity()`:
  - `groupByKey(_.actorLogin)` — partition by user
  - `transformWithState(new UserActivityProcessor(), TimeMode.ProcessingTime(), OutputMode.Update())`
- Run with the simulator active
- Wait ~2 minutes and show timer-based summaries appearing
- Show state accumulating across batches

**7. Test anomaly detection (5 min)**
- In a separate terminal, run `GitHubEventsSimulator.produceBotBurst()`
- Show the ANOMALY summary appearing in the output
- "In production, this could page an oncall engineer or block the user"

**8. Persist alerts to MongoDB (5 min)**
- Walk through `trackAndPersist()`:
  - Same processor, but with `foreachBatch` sink
  - Filter anomalies and write to MongoDB
- Run it, trigger a bot burst, then verify in MongoDB:
  ```
  docker exec -it rockthejvm-sparkstreaming-mongodb mongosh rtjvm --eval "db.github_alerts.find()"
  ```
- Show the alert document in MongoDB

**9. Discuss TTL behavior (3 min)**
- "If we stop sending data for a user, after 10 minutes their state is automatically cleaned up"
- "No more manual `state.setTimeoutDuration()` — Spark handles it"
- Contrast with old API: "With mapGroupsWithState, you had to manually check timeouts and clear state in your code"

### Key Teaching Points
- `transformWithState` is more verbose than `mapGroupsWithState` but far more powerful
- Multiple state types in one processor = complex logic without external databases
- TTL prevents unbounded state growth (critical in production)
- Timers enable periodic output without waiting for new data
- The anomaly pattern (threshold on batch size) is a real-world technique for bot/abuse detection
- `foreachBatch` on the output of `transformWithState` lets you route results to multiple sinks

---

## Running the Full Pipeline

For a final demo or "putting it all together" moment at the end:

1. Start Docker: `docker compose up -d`
2. Terminal 1: Run `GitHubEventsSimulator.main()` — events flow into Kafka
3. Terminal 2: Run `GitHubEventsIngestion.writeToPostgres()` — raw events land in PostgreSQL
4. Terminal 3: Run `GitHubEventsAnalytics.eventsPerRepoPerWindow()` — windowed analytics on console
5. Terminal 4: Run `GitHubEventsStateful.trackAndPersist()` — stateful processing + MongoDB alerts

Then trigger a bot burst and watch the alert flow through the stateful processor to MongoDB.

This demonstrates a realistic multi-stage streaming architecture:
```
Data Source → Kafka → Ingestion (raw storage) 
                   → Analytics (windowed insights)
                   → Stateful (user tracking + alerting)
```

---

## Timing Estimates

| Lesson | Recording Time | Editing Target |
|--------|---------------|----------------|
| 5.1 Simulator | 25-30 min | 20 min |
| 5.2 Ingestion | 25-30 min | 20 min |
| 5.3 Analytics | 30-35 min | 25 min |
| 5.4 Stateful | 40-50 min | 35 min |
| **Total** | **~2.5 hours** | **~1h 40min** |

---

## Common Student Questions to Anticipate

**Q: Why not use the real GitHub API?**
A: Rate limits (60/hour without auth) make it impractical for a live demo. The simulator gives us full control over rate, out-of-order behavior, and burst patterns. Students can optionally point it at the real API with a free GitHub token.

**Q: Why do we need `@transient` on state variables?**
A: State handles are assigned by Spark during task execution and can't be serialized. The `@transient` annotation prevents serialization errors when the processor is shipped to executors. They're re-initialized via `init()` on each executor.

**Q: How is `transformWithState` different from `flatMapGroupsWithState`?**
A: Three key differences: (1) structured state types (Value/List/Map) vs. one opaque state blob, (2) built-in TTL vs. manual timeout management, (3) timer support for time-driven logic without waiting for data.

**Q: What happens if the streaming job crashes?**
A: Spark checkpoints the state to disk. On restart, it reads the checkpoint and resumes from where it left off. You can demonstrate this: run the stateful query, stop it, check the `checkpoints/` directory, restart and see state preserved.

**Q: Can I use `transformWithState` without Kafka?**
A: Yes — it works with any streaming source. The TransformWithState lesson (4.4) uses a plain socket source. Kafka is just the integration we chose for the capstone.
