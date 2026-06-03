package part5github

import common.GithubEvent
import org.apache.spark.sql.streaming.{ExpiredTimerInfo, ListState, MapState, OutputMode, StatefulProcessor, TTLConfig, TimeMode, TimerValues, ValueState}
import org.apache.spark.sql.functions._

import java.sql.Timestamp
import java.time.{Duration, Instant}

object GithubEventsStateful extends GithubEventsProcessor {

  import spark.implicits._

  /*
    v total events, last event time, recent (last N) events, number of events by type, PER USER
    v issue the summaries ^ regardless of data quantity, at a fixed interval (10 seconds)
    - flag anomalies >20 events per batch = this is a bot
    - write the anomalies to Mongo
   */
  case class UserActivitySummary(
                                  actorLogin: String,
                                  totalEvents: Long,
                                  lastEventTime: Timestamp,
                                  recentEvents: List[String], // "eventType:repo"
                                  eventsByType: Map[String, Long],
                                  summaryFlag: String = "NORMAL"
                                )

  class UserActivityProcessor(lastN: Int, summaryIntervalMs: Long, anomalyThreshold: Int = 20)
  extends StatefulProcessor[String, GithubEvent, UserActivitySummary] {


    @transient private var totalEvents: ValueState[Long] = _
    @transient private var lastEventTime: ValueState[Long] = _
    @transient private var recentEvents: ListState[String] = _
    @transient private var eventsByType: MapState[String, Long] = _

    override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
      val ttl = TTLConfig(Duration.ofMinutes(10))
      totalEvents = getHandle.getValueState[Long]("totalEvents", ttl)
      lastEventTime = getHandle.getValueState[Long]("lastEventTime", ttl)
      recentEvents = getHandle.getListState[String]("recentEvents", ttl)
      eventsByType = getHandle.getMapState[String,Long]("eventsByType", ttl)
    }

    override def handleInputRows(
                                  key: String,
                                  inputRows: Iterator[GithubEvent],
                                  timerValues: TimerValues
                                ): Iterator[UserActivitySummary] = {
      // total events, last event time, recent (last N) events, number of events by type
      var count = if (totalEvents.exists()) totalEvents.get() else 0L
      var lastTime = if (lastEventTime.exists()) lastEventTime.get() else 0L
      var lastEvents = if (recentEvents.exists()) recentEvents.get().toList else List()
      var batchCount = 0
      val wasEmpty = count == 0L

      inputRows.foreach { event =>
        batchCount += 1
        count += 1
        lastTime = event.createdAt.getTime
        lastEvents = lastEvents :+ s"${event.eventType}:${event.repoName}"

        val typeCount =
          if (eventsByType.exists() && eventsByType.containsKey(event.eventType))
            eventsByType.getValue(event.eventType)
          else
            0L

        eventsByType.updateValue(event.eventType, typeCount + 1)
      }

      totalEvents.update(count)
      lastEventTime.update(lastTime)
      recentEvents.put(lastEvents.takeRight(lastN).toArray)

      if (wasEmpty) {
        // fire a timer to push a summary after
        getHandle.registerTimer(timerValues.getCurrentProcessingTimeInMs() + summaryIntervalMs)
      }

      if (batchCount > anomalyThreshold) {
        Iterator(UserActivitySummary(key, count, Timestamp.from(Instant.ofEpochMilli(lastTime)), lastEvents, eventsByType.iterator().toMap, "ANOMALY"))
      } else {
        Iterator.empty // not sending any data here when we receive input data
      }
    }

    override def handleExpiredTimer(
                                     key: String,
                                     timerValues: TimerValues,
                                     expiredTimerInfo: ExpiredTimerInfo
                                   ) = {
      val count = if (totalEvents.exists()) totalEvents.get() else 0L
      val lastTime = if (lastEventTime.exists()) lastEventTime.get() else 0L
      val recent = if (recentEvents.exists()) recentEvents.get().toList else List()
      val events = if (eventsByType.exists()) eventsByType.iterator().toMap else Map.empty[String, Long]

      // trigger a new timer for the next report
      getHandle.registerTimer(timerValues.getCurrentProcessingTimeInMs() + summaryIntervalMs)

      // push the data to output stream
      Iterator(UserActivitySummary(key, count, Timestamp.from(Instant.ofEpochMilli(lastTime)), recent, events))
    }
  }

  def trackUserActivity() =
    readFromKafka()
      .groupByKey(_.actorLogin)
      .transformWithState(
        new UserActivityProcessor(10, 10000),
        TimeMode.ProcessingTime(),
        OutputMode.Update()
      ) // Dataset[UserActivitySummary]
      .writeStream
      .format("console")
      .outputMode("update")
      .option("truncate", "false")
      .start()
      .awaitTermination()

  def trackAnomaliesToMongo() =
    readFromKafka()
      .groupByKey(_.actorLogin)
      .transformWithState(
        new UserActivityProcessor(10, 10000),
        TimeMode.ProcessingTime(),
        OutputMode.Update()
      ) // Dataset[UserActivitySummary]
      .filter(col("summaryFlag") === "ANOMALY") // track anomalies
      .writeStream
      .format("mongodb")
      .option("checkpointLocation", "checkpoints/anomaly-detection")
      .option("connection.uri", "mongodb://localhost:27017")
      .option("database", "rtjvm")
      .option("collection", "githubAnomalies")
      .outputMode("append")
      .start()
      .awaitTermination()

  def main(args: Array[String]): Unit = {
    trackAnomaliesToMongo()
  }
}
