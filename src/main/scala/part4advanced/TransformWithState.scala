package part4advanced

import java.io.PrintStream
import java.net.ServerSocket
import java.time.Duration

import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import org.apache.spark.sql.streaming._

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
        TimeMode.None(),
        OutputMode.Update()
      )

    averageByPostType.writeStream
      .format("console")
      .outputMode("update")
      .start()
      .awaitTermination()
  }

  // --- Example 2: ListState (sliding window of recent records) ---

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
        TimeMode.None(),
        OutputMode.Update()
      )

    recentByPostType.writeStream
      .format("console")
      .outputMode("update")
      .start()
      .awaitTermination()
  }

  // --- Example 3: MapState (count by sub-category) ---

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
        TimeMode.None(),
        OutputMode.Update()
      )

    breakdownByPostType.writeStream
      .format("console")
      .outputMode("update")
      .start()
      .awaitTermination()
  }

  // --- Example 4: TTL-based state expiration ---

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
        TimeMode.ProcessingTime(),
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
