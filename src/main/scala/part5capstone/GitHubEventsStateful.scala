package part5capstone

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming._
import common._

import java.sql.Timestamp
import java.time.{Duration, Instant}

object GitHubEventsStateful {

  val spark = SparkSession.builder()
    .appName("GitHub Events Stateful Processing")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  val gitHubEventSchema = StructType(Array(
    StructField("id", StringType),
    StructField("type", StringType),
    StructField("actor", StructType(Array(
      StructField("login", StringType)
    ))),
    StructField("repo", StructType(Array(
      StructField("name", StringType)
    ))),
    StructField("created_at", TimestampType)
  ))

  def readEventsFromKafka(): Dataset[GitHubEvent] = {
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "github-events")
      .option("startingOffsets", "latest")
      .load()
      .selectExpr("CAST(value AS STRING) as json")
      .select(from_json(col("json"), gitHubEventSchema).as("event"))
      .select(
        col("event.id").as("id"),
        col("event.type").as("eventType"),
        col("event.actor.login").as("actorLogin"),
        col("event.repo.name").as("repoName"),
        col("event.created_at").as("createdAt")
      )
      .as[GitHubEvent]
  }

  // --- Full stateful processor: per-user activity tracking with timers and anomaly detection ---

  class UserActivityProcessor extends StatefulProcessor[String, GitHubEvent, UserActivitySummary] {
    @transient private var totalEvents: ValueState[Long] = _
    @transient private var lastEventTime: ValueState[Long] = _
    @transient private var recentEvents: ListState[String] = _
    @transient private var eventsByType: MapState[String, Long] = _

    private val summaryIntervalMs = 120000L // emit summary every 2 minutes
    private val anomalyThreshold = 20       // flag >20 events per batch as suspicious

    override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
      val ttl = TTLConfig(Duration.ofMinutes(10))
      totalEvents = getHandle.getValueState[Long]("totalEvents", ttl)
      lastEventTime = getHandle.getValueState[Long]("lastEventTime", ttl)
      recentEvents = getHandle.getListState[String]("recentEvents", ttl)
      eventsByType = getHandle.getMapState[String, Long]("eventsByType", ttl)
    }

    override def handleInputRows(
      key: String,
      inputRows: Iterator[GitHubEvent],
      timerValues: TimerValues
    ): Iterator[UserActivitySummary] = {
      var count = if (totalEvents.exists()) totalEvents.get() else 0L
      val wasEmpty = count == 0L
      var batchCount = 0

      inputRows.foreach { event =>
        count += 1
        batchCount += 1

        // update last event time
        lastEventTime.update(event.createdAt.getTime)

        // maintain recent events list (last 10)
        recentEvents.appendValue(s"${event.eventType}:${event.repoName}")

        // update per-type counts
        val typeCount = if (eventsByType.exists() && eventsByType.containsKey(event.eventType))
          eventsByType.getValue(event.eventType) else 0L
        eventsByType.updateValue(event.eventType, typeCount + 1)
      }

      totalEvents.update(count)

      // trim recent events to last 10
      if (recentEvents.exists()) {
        val all = recentEvents.get().toList
        if (all.size > 10) {
          recentEvents.clear()
          all.takeRight(10).foreach(recentEvents.appendValue)
        }
      }

      // register periodic summary timer on first data
      if (wasEmpty) {
        getHandle.registerTimer(timerValues.getCurrentProcessingTimeInMs() + summaryIntervalMs)
      }

      // anomaly detection: too many events in one batch
      if (batchCount > anomalyThreshold) {
        val typeBreakdown: Map[String, Long] = if (eventsByType.exists())
          eventsByType.iterator().map(e => e._1 -> e._2).toMap
        else Map.empty[String, Long]

        Iterator(UserActivitySummary(key, count, typeBreakdown, s"ANOMALY: $batchCount events in one batch"))
      } else {
        Iterator.empty
      }
    }

    override def handleExpiredTimer(
      key: String,
      timerValues: TimerValues,
      expiredTimerInfo: ExpiredTimerInfo
    ): Iterator[UserActivitySummary] = {
      // periodic summary
      val count = if (totalEvents.exists()) totalEvents.get() else 0L
      val typeBreakdown: Map[String, Long] = if (eventsByType.exists())
        eventsByType.iterator().map(e => e._1 -> e._2).toMap
      else Map.empty[String, Long]

      // re-register timer for next summary
      getHandle.registerTimer(timerValues.getCurrentProcessingTimeInMs() + summaryIntervalMs)

      Iterator(UserActivitySummary(key, count, typeBreakdown, "periodic-summary"))
    }
  }

  def trackUserActivity(): Unit = {
    readEventsFromKafka()
      .groupByKey(_.actorLogin)
      .transformWithState(
        new UserActivityProcessor(),
        TimeMode.ProcessingTime(),
        OutputMode.Update()
      )
      .writeStream
      .format("console")
      .outputMode("update")
      .option("truncate", "false")
      .option("checkpointLocation", "checkpoints/github-stateful")
      .start()
      .awaitTermination()
  }

  // write activity summaries to MongoDB
  def trackAndPersist(): Unit = {
    readEventsFromKafka()
      .groupByKey(_.actorLogin)
      .transformWithState(
        new UserActivityProcessor(),
        TimeMode.ProcessingTime(),
        OutputMode.Update()
      )
      .writeStream
      .foreachBatch { (batch: Dataset[UserActivitySummary], _: Long) =>
        // write summaries to console
        batch.show(truncate = false)

        // write anomalies to MongoDB
        val anomalies = batch.filter(col("summaryType").startsWith("ANOMALY"))
        if (!anomalies.isEmpty) {
          anomalies.write
            .format("mongodb")
            .option("connection.uri", "mongodb://localhost:27017")
            .option("database", "rtjvm")
            .option("collection", "github_alerts")
            .mode("append")
            .save()
        }
      }
      .option("checkpointLocation", "checkpoints/github-stateful-persist")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    trackUserActivity()
  }
}
