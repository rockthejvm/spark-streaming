package part4advanced

import org.apache.spark.sql.classic.{Dataset, SparkSession}
import org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
import org.apache.spark.sql.streaming.{ExpiredTimerInfo, ListState, MapState, OutputMode, StatefulProcessor, TTLConfig, TimeMode, TimerValues, ValueState}

import java.time.Duration

object TransformWithState {

  val spark = SparkSession.builder()
    .appName("Stateful Computations")
    .master("local[2]")
    .config(
      "spark.sql.streaming.stateStore.providerClass",
      "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider" // necessary for multiple state variables per processor
    )
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  // post_type,n_posts,total_storage
  case class SocialPostRecord(postType: String, count: Int, storageUsed: Int)
  case class AveragePostStorage(postType: String, averageStorage: Double)

  /*
    input: Streaming Dataset grouped by key
      - batches
        - partitions
          - partition 1
            - key1: [....]
            - key2: [...]
          - partition 2
            - key3: [ ... ]
          - partition 3
            - key4: [...]
            - key5: [...]
            - key6: [....]

    - one StatefulProcessor instantiated PER PARTITION
      - spark will call init(), ONCE
      - FOR EVERY KEY on this partition, spark will call handleInputRows(key, all the rows)
        - produce one or more output rows as an iterator

      for every key { key =>
        // STATE
        var totalCount = 0
        var totalStorage = 0

        batches.foreach { rows =>
          rows.foreach { post =>
            totalCount += post.count
            totalStorage += post.storage
          }

          APS(key, totalStorage/totalCount)
        }
      }
   */
  class AverageStorageProcessor extends StatefulProcessor[String, SocialPostRecord, AveragePostStorage] {
    // @transient because all stateful processors are serializable and ValueStates are not
    @transient private var totalCount: ValueState[Long] = _ // HANDLE to a "distributed variable" that you can inspect, read and modify
    @transient private var totalStorage: ValueState[Long] = _

    override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
      val ttl =
        if (timeMode == TimeMode.None()) TTLConfig.NONE
        else TTLConfig(Duration.ofSeconds(15))

      // initialize your HANDLES to the state variables
      totalCount = getHandle.getValueState[Long]("totalCount", ttl) // TTLConfig specifies when the state variable is removed from the store, NONE = never remove
      totalStorage = getHandle.getValueState[Long]("totalStorage", ttl)
    }

    override def handleInputRows(key: String, inputRows: Iterator[SocialPostRecord], timerValues: TimerValues): Iterator[AveragePostStorage] = {
      var count = if (totalCount.exists()) totalCount.get() else 0L
      var storage = if (totalStorage.exists()) totalStorage.get() else 0L

      inputRows.foreach { record =>
        count += record.count
        storage += record.storageUsed
      }

      // update state variables
      totalCount.update(count)
      totalStorage.update(storage)

      // emit one or more output rows
      Iterator(
        AveragePostStorage(key, storage.toDouble / count)
      )
    }

    def handleInputRows_v2(key: String, inputRows: Iterator[SocialPostRecord], timerValues: TimerValues): Iterator[AveragePostStorage] = {
      val initialCount = if (totalCount.exists()) totalCount.get() else 0L
      val initialStorage = if (totalStorage.exists()) totalStorage.get() else 0L

      val (batchCount, batchStorage) = inputRows.foldLeft ((0L, 0L)) {
        case ((currentCount, currentStorage), SocialPostRecord(postType, count, storage)) =>
          (currentCount + count, currentStorage + storage)
      }

      // update state variables
      val newCount = initialCount + batchCount
      val newStorage = initialStorage + batchStorage

      totalCount.update(newCount)
      totalStorage.update(newStorage)

      // emit one or more output rows
      Iterator(
        AveragePostStorage(key, newStorage.toDouble / newCount)
      )
    }
  }

  // example 2 - keep the last N records per post type
  // LIST state

  case class RecentActivity(postType: String, recentCounts: List[Int])

  class RecentActivityProcessor(windowSize: Int)
    extends StatefulProcessor[String, SocialPostRecord, RecentActivity] {
    @transient private var recentRecords: ListState[Int] = _ // best for appending (O(1)), lazy streaming
    @transient private var recentRecords_v2: ValueState[List[Int]] = _ // best for the List API

    override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
      recentRecords = getHandle.getListState[Int]("recentRecords", TTLConfig.NONE)
      recentRecords_v2 = getHandle.getValueState[List[Int]]("recentRecords_v2", TTLConfig.NONE)
    }

    override def handleInputRows(key: String, inputRows: Iterator[SocialPostRecord], timerValues: TimerValues): Iterator[RecentActivity] = {
      val existingRecords = if (recentRecords.exists()) recentRecords.get() else Iterator.empty[Int]
      val newCounts = inputRows.map(_.count)
      val allCounts = (existingRecords.toList ++ newCounts).takeRight(windowSize)

      recentRecords.put(allCounts.toArray)

      Iterator(RecentActivity(key, allCounts))
    }

    def handleInputRows_v2(key: String, inputRows: Iterator[SocialPostRecord], timerValues: TimerValues): Iterator[RecentActivity] = {
      val existingRecords = if (recentRecords_v2.exists()) recentRecords_v2.get() else List()
      val newCounts = inputRows.map(_.count)
      val allCounts = existingRecords ++ newCounts

      recentRecords_v2.update(allCounts)

      Iterator(RecentActivity(key, allCounts))
    }
  }

  def readSocialUpdates(): Dataset[SocialPostRecord] =
    spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .as[String]
      .map { line =>
        val tokens = line.split(",")
        SocialPostRecord(tokens(0).trim, tokens(1).trim.toInt, tokens(2).trim.toInt)
      }

  // breakdown by category (small, medium, large)
  case class PostTypeBreakdown(postType: String, breakdown: Map[String, Long])

  class PostBreakdownProcessor
    extends StatefulProcessor[String, SocialPostRecord, PostTypeBreakdown] {
    @transient private var countsBySize: MapState[String, Long] = _
    // @transient private var countsBySize_v2: ValueState[Map[String, Long]] = _

    override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
      countsBySize = getHandle.getMapState[String, Long]("countsBySize", TTLConfig.NONE)
    }

    override def handleInputRows(key: String, inputRows: Iterator[SocialPostRecord], timerValues: TimerValues): Iterator[PostTypeBreakdown] = {
      inputRows.foreach { record =>
        val category = sizeCategory(record)
        val current = if (countsBySize.exists() && countsBySize.containsKey(category)) countsBySize.getValue(category) else 0L
        countsBySize.updateValue(category, current + 1)
      }

      val breakdown: Map[String, Long] =
        if (countsBySize.exists()) {
          countsBySize.iterator().toMap
        } else Map.empty

      Iterator(PostTypeBreakdown(key, breakdown))
    }

    private def sizeCategory(post: SocialPostRecord): String = {
      if (post.storageUsed < 1000) "small"
      else if (post.storageUsed < 20000) "medium"
      else "large"
    }
  }

  // example 4 - periodic summaries even if you don't get new data
  case class PeriodicSummary(postType: String, totalCount: Long, totalStorage: Long, summaryType: String)

  class TimeBasedProcessor
    extends StatefulProcessor[String, SocialPostRecord, PeriodicSummary] {
    @transient private var totalCount: ValueState[Long] = _
    @transient private var totalStorage: ValueState[Long] = _

    override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
      totalCount = getHandle.getValueState[Long]("totalCount", TTLConfig.NONE)
      totalStorage = getHandle.getValueState[Long]("totalStorage", TTLConfig.NONE)
    }

    override def handleInputRows(key: String, inputRows: Iterator[SocialPostRecord], timerValues: TimerValues): Iterator[PeriodicSummary] = {
      var count = if (totalCount.exists()) totalCount.get() else 0L
      var storage = if (totalStorage.exists()) totalStorage.get() else 0L
      val wasEmpty = count == 0L // track if we're at the beginning of data

      inputRows.foreach { record =>
        count += record.count
        storage += record.storageUsed
      }

      totalCount.update(count)
      totalStorage.update(storage)

      // schedule a timer the first time we see data
      if (wasEmpty) {
        getHandle.registerTimer(timerValues.getCurrentProcessingTimeInMs() + 10000)
      }

      Iterator(PeriodicSummary(key, count, storage, "data-update"))
    }

    override def handleExpiredTimer(key: String, timerValues: TimerValues, expiredTimerInfo: ExpiredTimerInfo): Iterator[PeriodicSummary] = {
      val count = if (totalCount.exists()) totalCount.get() else 0L
      val storage = if (totalStorage.exists()) totalStorage.get() else 0L

      // schedule next timer
      val nextExpiryMs = timerValues.getCurrentProcessingTimeInMs() + 10000
      getHandle.registerTimer(nextExpiryMs)

      Iterator(PeriodicSummary(key, count, storage, "timer-summary"))
    }
  }

  // -------------------------------------- tests

  /*
    text,3,3000
    text,4,5000
    video,1,500000
    audio,3,60000
    --
    text,1,2500
    audio,2,40000
    --
    video, 2, 300000000
   */
  def averageStorageWithState() = {
    val socialStream = readSocialUpdates()

    val averageByPostType = socialStream
      .groupByKey(_.postType)
      .transformWithState(
        // stateful processor - full logic of the aggregation
        new AverageStorageProcessor(),
        // time mode - none, event time and processing time
        TimeMode.ProcessingTime(),
        // output mode - append, update, complete
        OutputMode.Update()
      )

    averageByPostType.writeStream
      .format("console")
      .outputMode(OutputMode.Update())
      .start()
      .awaitTermination()
  }

  /*
    text,3,3000
    text,4,5000
    video,1,500000
    audio,3,60000
    --
    text,1,2500
    audio,2,40000
    --
    video, 2, 300000000
    --
    text,5,5000 // 19000/12
    text,6,7000
    text,1,7000
   */
  def recentActivityWithState() = {
    val socialStream = readSocialUpdates()

    val recentActivityByType = socialStream
      .groupByKey(_.postType)
      .transformWithState(
        // stateful processor - full logic of the aggregation
        new RecentActivityProcessor(5),
        // time mode - none, event time and processing time
        TimeMode.None,
        // output mode - append, update, complete
        OutputMode.Update()
      )

    recentActivityByType.writeStream
      .format("console")
      .outputMode(OutputMode.Update())
      .start()
      .awaitTermination()
  }

  /*
    text,3,500
    text,4,5000
    video,1,500000
    audio,3,30000
    --
    text,1,2500
    audio,2,60000
    --
    video, 2, 3000000
    --
    text,5,15000
    text,6,7000
    text,1,800
   */
  def postBreakdownWithState() = {
    val socialStream = readSocialUpdates()

    val postBreakdownByType = socialStream
      .groupByKey(_.postType)
      .transformWithState(
        // stateful processor - full logic of the aggregation
        new PostBreakdownProcessor,
        // time mode - none, event time and processing time
        TimeMode.None,
        // output mode - append, update, complete
        OutputMode.Update()
      )

    postBreakdownByType.writeStream
      .format("console")
      .outputMode(OutputMode.Update())
      .start()
      .awaitTermination()
  }

  /*
    text,3,500
    text,4,5000
    video,1,500000
    audio,3,30000
    --
    text,1,2500
    audio,2,60000
    --
    video, 2, 3000000
    --
    text,5,15000
    text,6,7000
    text,1,800
   */
  def timerBasedSummaries() = {
    val socialStream = readSocialUpdates()

    val summaries = socialStream
      .groupByKey(_.postType)
      .transformWithState(
        // stateful processor - full logic of the aggregation
        new TimeBasedProcessor,
        // processing time required for this example
        TimeMode.ProcessingTime(),
        // output mode - append, update, complete
        OutputMode.Update()
      )

    summaries.writeStream
      .format("console")
      .outputMode(OutputMode.Update())
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    timerBasedSummaries()
  }
}
