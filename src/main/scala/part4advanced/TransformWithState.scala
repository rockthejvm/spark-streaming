package part4advanced

import java.io.PrintStream
import java.net.ServerSocket
import java.time.Duration

import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import org.apache.spark.sql.streaming._

/*
  transformWithState — Spark 4's stateful processing API (replaces mapGroupsWithState/flatMapGroupsWithState).

  How it works:
  1. You group a streaming Dataset by key (groupByKey), then call .transformWithState(processor, timeMode, outputMode).
  2. Spark creates one instance of your StatefulProcessor PER PARTITION (not per key).
     - init() is called once when the processor starts — this is where you obtain state handles.
     - handleInputRows(key, rows, timerValues) is called once per key that has data in the current micro-batch.
     - handleExpiredTimer(key, timerValues, expiredTimerInfo) is called when a registered timer fires.
     - close() is called when the query stops.

  State handles (ValueState, ListState, MapState):
  - Obtained in init() via getHandle.getValueState / getListState / getMapState.
  - Each handle is scoped to the CURRENT KEY — when handleInputRows is called for key "video",
    state.get() returns the value previously stored for "video", not for any other key.
  - State is automatically checkpointed and survives restarts.
  - ValueState[T]: holds one value per key. Good for counters, accumulators, last-seen timestamps.
  - ListState[T]: holds an ordered list per key. Good for recent-event buffers, sliding windows.
  - MapState[K,V]: holds a key-value map per key. Good for sub-category breakdowns, histograms.

  Output:
  - handleInputRows returns an Iterator of output records — these become rows in the result Dataset.
  - Returning Iterator.empty means "no output for this key in this batch."
  - handleExpiredTimer also returns an Iterator, so timers can produce output too.
*/
object TransformWithState {

  val spark = SparkSession.builder()
    .appName("Transform With State")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  case class SocialPostRecord(postType: String, count: Int, storageUsed: Int)
  case class AveragePostStorage(postType: String, averageStorage: Double)

  // postType,count,storageUsed
  def readSocialUpdates(): Dataset[SocialPostRecord] = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 12345)
    .load()
    .as[String]
    .map { line =>
      val tokens = line.split(",")
      SocialPostRecord(tokens(0), tokens(1).trim.toInt, tokens(2).trim.toInt)
    }

  // --- Example 1: ValueState (basic accumulation) ---
  // Goal: compute a running average storage per post type, accumulating across all batches.
  // ValueState holds a single value per key — perfect for counters and running totals.

  class AverageStorageProcessor extends StatefulProcessor[String, SocialPostRecord, AveragePostStorage] {
    @transient private var totalCount: ValueState[Long] = _
    @transient private var totalStorage: ValueState[Long] = _

    override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
      totalCount = getHandle.getValueState[Long]("totalCount", TTLConfig.NONE)
      totalStorage = getHandle.getValueState[Long]("totalStorage", TTLConfig.NONE)
    }

    override def handleInputRows(
      key: String,
      inputRows: Iterator[SocialPostRecord],
      timerValues: TimerValues
    ): Iterator[AveragePostStorage] = {
      var count = if (totalCount.exists()) totalCount.get() else 0L
      var storage = if (totalStorage.exists()) totalStorage.get() else 0L

      inputRows.foreach { record =>
        count += record.count
        storage += record.storageUsed
      }

      totalCount.update(count)
      totalStorage.update(storage)

      Iterator(AveragePostStorage(key, storage.toDouble / count))
    }
  }

  def averageStorageWithValueState(): Unit = {
    val socialStream = readSocialUpdates()

    val averageByPostType = socialStream
      .groupByKey(_.postType)
      .transformWithState(
        new AverageStorageProcessor(),
        // TimeMode.None() — we don't use timers or TTL, so no time tracking needed.
        //   TimeMode.ProcessingTime() — enables processing-time timers and TTL expiration.
        //   TimeMode.EventTime() — enables event-time timers that fire based on watermark progression.
        TimeMode.None(),
        // OutputMode.Update() — emit only rows whose state changed in this batch.
        //   OutputMode.Append() — emit rows only once, when their state is finalized (e.g. after a window closes).
        OutputMode.Update()
      )

    averageByPostType.writeStream
      .format("console")
      .outputMode("update")
      .start()
      .awaitTermination()
  }

  // --- Example 2: ListState (sliding window of recent records) ---
  // Goal: keep the last N records per post type, like a sliding window buffer.
  // ListState holds an ordered collection of values per key — useful for recent-history tracking.

  case class RecentActivity(postType: String, recentCounts: List[Int])

  class RecentActivityProcessor(windowSize: Int)
    extends StatefulProcessor[String, SocialPostRecord, RecentActivity] {

    @transient private var recentRecords: ListState[Int] = _

    override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
      recentRecords = getHandle.getListState[Int]("recentRecords", TTLConfig.NONE)
    }

    override def handleInputRows(
      key: String,
      inputRows: Iterator[SocialPostRecord],
      timerValues: TimerValues
    ): Iterator[RecentActivity] = {
      val existing = if (recentRecords.exists()) recentRecords.get().toList else List.empty[Int]
      val newCounts = inputRows.map(_.count).toList
      val allCounts = existing ++ newCounts
      val window = allCounts.takeRight(windowSize)

      recentRecords.clear()
      window.foreach(recentRecords.appendValue)

      Iterator(RecentActivity(key, window))
    }
  }

  def recentActivityWithListState(): Unit = {
    val socialStream = readSocialUpdates()

    val recentByPostType = socialStream
      .groupByKey(_.postType)
      .transformWithState(
        new RecentActivityProcessor(5),
        TimeMode.None(), // no timers or TTL needed — we manage the window size manually
        OutputMode.Update() // emit the updated window for a key every time new data arrives
      )

    recentByPostType.writeStream
      .format("console")
      .outputMode("update")
      .start()
      .awaitTermination()
  }

  // --- Example 3: MapState (count by sub-category) ---
  // Goal: for each post type, count how many posts fall into each size category (small/medium/large).
  // MapState holds a key-value map per grouping key — ideal for breakdowns and histograms.

  case class PostTypeBreakdown(postType: String, breakdown: Map[String, Long])

  class PostBreakdownProcessor extends StatefulProcessor[String, SocialPostRecord, PostTypeBreakdown] {
    @transient private var countsBySize: MapState[String, Long] = _

    override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
      countsBySize = getHandle.getMapState[String, Long]("countsBySize", TTLConfig.NONE)
    }

    private def sizeCategory(storageUsed: Int): String =
      if (storageUsed < 1000) "small"
      else if (storageUsed < 10000) "medium"
      else "large"

    override def handleInputRows(
      key: String,
      inputRows: Iterator[SocialPostRecord],
      timerValues: TimerValues
    ): Iterator[PostTypeBreakdown] = {
      inputRows.foreach { record =>
        val category = sizeCategory(record.storageUsed)
        val current = if (countsBySize.exists() && countsBySize.containsKey(category)) countsBySize.getValue(category) else 0L
        countsBySize.updateValue(category, current + record.count)
      }

      val breakdown: Map[String, Long] = if (countsBySize.exists()) {
        countsBySize.iterator().map(entry => entry._1 -> entry._2).toMap
      } else Map.empty[String, Long]

      Iterator(PostTypeBreakdown(key, breakdown))
    }
  }

  def postBreakdownWithMapState(): Unit = {
    val socialStream = readSocialUpdates()

    val breakdownByPostType = socialStream
      .groupByKey(_.postType)
      .transformWithState(
        new PostBreakdownProcessor(),
        TimeMode.None(), // no timers or TTL — the map grows indefinitely (fine for a small set of categories)
        OutputMode.Update() // emit the full breakdown for a key whenever it changes
      )

    breakdownByPostType.writeStream
      .format("console")
      .outputMode("update")
      .start()
      .awaitTermination()
  }

  // --- Example 4: TTL-based state expiration ---
  // Goal: same running average as Example 1, but state auto-expires after 30 seconds of inactivity.
  // TTL prevents unbounded state growth — if a post type stops receiving data, its state is cleaned up.
  // Requires TimeMode.ProcessingTime() so Spark can track when each key's state was last accessed.

  class AverageStorageWithTTLProcessor extends StatefulProcessor[String, SocialPostRecord, AveragePostStorage] {
    @transient private var totalCount: ValueState[Long] = _
    @transient private var totalStorage: ValueState[Long] = _

    override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
      val ttl = TTLConfig(Duration.ofSeconds(30))
      totalCount = getHandle.getValueState[Long]("totalCount", ttl)
      totalStorage = getHandle.getValueState[Long]("totalStorage", ttl)
    }

    override def handleInputRows(
      key: String,
      inputRows: Iterator[SocialPostRecord],
      timerValues: TimerValues
    ): Iterator[AveragePostStorage] = {
      var count = if (totalCount.exists()) totalCount.get() else 0L
      var storage = if (totalStorage.exists()) totalStorage.get() else 0L

      inputRows.foreach { record =>
        count += record.count
        storage += record.storageUsed
      }

      totalCount.update(count)
      totalStorage.update(storage)

      Iterator(AveragePostStorage(key, storage.toDouble / count))
    }
  }

  def averageStorageWithTTL(): Unit = {
    val socialStream = readSocialUpdates()

    val averageByPostType = socialStream
      .groupByKey(_.postType)
      .transformWithState(
        new AverageStorageWithTTLProcessor(),
        // TimeMode.ProcessingTime() — required for TTL to work. Spark tracks wall-clock time per key
        //   and automatically clears state that hasn't been updated within the TTL duration.
        //   TimeMode.None() would cause a runtime error because TTLConfig is set inside the processor.
        TimeMode.ProcessingTime(),
        OutputMode.Update()
      )

    averageByPostType.writeStream
      .format("console")
      .outputMode("update")
      .start()
      .awaitTermination()
  }

  // --- Example 5: Processing-time timers ---
  // Goal: emit periodic summaries every 10 seconds, even if no new data arrives.
  // Timers let you schedule a callback (handleExpiredTimer) at a future timestamp.
  // Use case: periodic reporting, session expiration, delayed alerts.

  case class PeriodicSummary(postType: String, totalCount: Long, totalStorage: Long, summaryType: String)

  class TimerBasedProcessor extends StatefulProcessor[String, SocialPostRecord, PeriodicSummary] {
    @transient private var totalCount: ValueState[Long] = _
    @transient private var totalStorage: ValueState[Long] = _

    override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
      totalCount = getHandle.getValueState[Long]("totalCount", TTLConfig.NONE)
      totalStorage = getHandle.getValueState[Long]("totalStorage", TTLConfig.NONE)
    }

    override def handleInputRows(
      key: String,
      inputRows: Iterator[SocialPostRecord],
      timerValues: TimerValues
    ): Iterator[PeriodicSummary] = {
      var count = if (totalCount.exists()) totalCount.get() else 0L
      var storage = if (totalStorage.exists()) totalStorage.get() else 0L

      val wasEmpty = count == 0L

      inputRows.foreach { record =>
        count += record.count
        storage += record.storageUsed
      }

      totalCount.update(count)
      totalStorage.update(storage)

      // register a timer 10 seconds from now (only when first data arrives)
      if (wasEmpty) {
        val expiryMs = timerValues.getCurrentProcessingTimeInMs() + 10000
        getHandle.registerTimer(expiryMs)
      }

      Iterator(PeriodicSummary(key, count, storage, "update"))
    }

    override def handleExpiredTimer(
      key: String,
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo
    ): Iterator[PeriodicSummary] = {
      val count = if (totalCount.exists()) totalCount.get() else 0L
      val storage = if (totalStorage.exists()) totalStorage.get() else 0L

      // register the next timer 10 seconds from now
      val nextExpiryMs = timerValues.getCurrentProcessingTimeInMs() + 10000
      getHandle.registerTimer(nextExpiryMs)

      Iterator(PeriodicSummary(key, count, storage, "timer-summary"))
    }
  }

  def timerBasedSummaries(): Unit = {
    val socialStream = readSocialUpdates()

    val summaries = socialStream
      .groupByKey(_.postType)
      .transformWithState(
        new TimerBasedProcessor(),
        // TimeMode.ProcessingTime() — required for registerTimer() / handleExpiredTimer().
        //   Timers fire based on wall-clock time: when the processing time exceeds the registered timestamp.
        //   TimeMode.EventTime() timers fire when the watermark advances past the registered timestamp —
        //   useful when you want timers tied to the data's own timestamps rather than the system clock.
        TimeMode.ProcessingTime(),
        // OutputMode.Update() — we emit both on data arrival AND on timer expiry.
        //   Append mode would also work here, but Update lets us see intermediate results on every batch.
        OutputMode.Update()
      )

    summaries.writeStream
      .format("console")
      .outputMode("update")
      .start()
      .awaitTermination()
  }

  /*
    Exercises

    1) Session window with timers:
       Group input by postType. When data arrives, accumulate counts.
       Register a timer 30 seconds in the future.
       When the timer fires without new data, emit the session total and clear state.

    2) Running statistics with MapState:
       For each postType, maintain min, max, and sum in a MapState.
       Emit the current min, max, and average after each batch.
   */

  // --- Exercise 1 solution: Session window with timers ---

  case class SessionResult(postType: String, sessionTotal: Long, resultType: String)

  class SessionWindowProcessor extends StatefulProcessor[String, SocialPostRecord, SessionResult] {
    @transient private var sessionCount: ValueState[Long] = _

    override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
      sessionCount = getHandle.getValueState[Long]("sessionCount", TTLConfig.NONE)
    }

    override def handleInputRows(
      key: String,
      inputRows: Iterator[SocialPostRecord],
      timerValues: TimerValues
    ): Iterator[SessionResult] = {
      var count = if (sessionCount.exists()) sessionCount.get() else 0L

      inputRows.foreach { record =>
        count += record.count
      }

      sessionCount.update(count)

      // every time new data arrives, (re)set the session timeout to 30s from now
      val expiryMs = timerValues.getCurrentProcessingTimeInMs() + 30000
      getHandle.registerTimer(expiryMs)

      Iterator(SessionResult(key, count, "update"))
    }

    override def handleExpiredTimer(
      key: String,
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo
    ): Iterator[SessionResult] = {
      // timer fired — no new data arrived within 30s, so the session is over
      val finalCount = if (sessionCount.exists()) sessionCount.get() else 0L
      sessionCount.clear()
      Iterator(SessionResult(key, finalCount, "session-closed"))
    }
  }

  def sessionWindowWithTimers(): Unit = {
    val socialStream = readSocialUpdates()

    val sessions = socialStream
      .groupByKey(_.postType)
      .transformWithState(
        new SessionWindowProcessor(),
        TimeMode.ProcessingTime(), // needed for registerTimer
        OutputMode.Update()
      )

    sessions.writeStream
      .format("console")
      .outputMode("update")
      .start()
      .awaitTermination()
  }

  // --- Exercise 2 solution: Running statistics with MapState ---

  case class RunningStats(postType: String, min: Int, max: Int, average: Double)

  class RunningStatsProcessor extends StatefulProcessor[String, SocialPostRecord, RunningStats] {
    @transient private var stats: MapState[String, Long] = _

    override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
      stats = getHandle.getMapState[String, Long]("stats", TTLConfig.NONE)
    }

    override def handleInputRows(
      key: String,
      inputRows: Iterator[SocialPostRecord],
      timerValues: TimerValues
    ): Iterator[RunningStats] = {
      var currentMin = if (stats.exists() && stats.containsKey("min")) stats.getValue("min") else Long.MaxValue
      var currentMax = if (stats.exists() && stats.containsKey("max")) stats.getValue("max") else Long.MinValue
      var currentSum = if (stats.exists() && stats.containsKey("sum")) stats.getValue("sum") else 0L
      var currentCount = if (stats.exists() && stats.containsKey("count")) stats.getValue("count") else 0L

      inputRows.foreach { record =>
        val v = record.storageUsed.toLong
        if (v < currentMin) currentMin = v
        if (v > currentMax) currentMax = v
        currentSum += v
        currentCount += 1
      }

      stats.updateValue("min", currentMin)
      stats.updateValue("max", currentMax)
      stats.updateValue("sum", currentSum)
      stats.updateValue("count", currentCount)

      Iterator(RunningStats(key, currentMin.toInt, currentMax.toInt, currentSum.toDouble / currentCount))
    }
  }

  def runningStatsWithMapState(): Unit = {
    val socialStream = readSocialUpdates()

    val statsByPostType = socialStream
      .groupByKey(_.postType)
      .transformWithState(
        new RunningStatsProcessor(),
        TimeMode.None(),
        OutputMode.Update()
      )

    statsByPostType.writeStream
      .format("console")
      .outputMode("update")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    averageStorageWithValueState()
  }
}

/*
  Test data (type into netcat: nc -lk 12345):

-- batch 1
text,3,3000
text,4,5000
video,1,500000
audio,3,60000
-- batch 2
text,1,2500
audio,2,40000
-- batch 3
video,2,300000
*/

// sends data to the socket automatically for deterministic testing
object TransformWithStateSender {
  val serverSocket = new ServerSocket(12345)
  val socket = serverSocket.accept()
  val printer = new PrintStream(socket.getOutputStream)

  println("socket accepted")

  def sendSocialData(): Unit = {
    Thread.sleep(5000)
    printer.println("text,3,3000")
    printer.println("text,4,5000")
    printer.println("video,1,500000")
    printer.println("audio,3,60000")
    Thread.sleep(3000)
    printer.println("text,1,2500")
    printer.println("audio,2,40000")
    Thread.sleep(3000)
    printer.println("video,2,300000")
  }

  def main(args: Array[String]): Unit = {
    sendSocialData()
  }
}
