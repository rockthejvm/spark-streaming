package part4advanced

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

import scala.concurrent.duration._

object RealTimeMode {

  val spark = SparkSession.builder()
    .appName("Real-Time Mode")
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

  def readFromKafka(): DataFrame = spark.readStream
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

  // --- Example 1: Micro-batch baseline ---

  def microBatchPipeline(): Unit = {
    readFromKafka()
      .select(
        col("eventType"),
        col("repoName"),
        current_timestamp().as("processedAt")
      )
      .writeStream
      .format("console")
      .outputMode("append")
      .option("truncate", "false")
      .trigger(Trigger.ProcessingTime(1.second))
      .start()
      .awaitTermination()
  }

  // --- Example 2: Real-Time Mode (same pipeline, just change the trigger) ---

  def realTimePipeline(): Unit = {
    readFromKafka()
      .select(
        col("eventType"),
        col("repoName"),
        current_timestamp().as("processedAt")
      )
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "github-events-processed")
      .option("checkpointLocation", "checkpoints/rtm-demo")
      .trigger(Trigger.RealTime())
      .start()
      .awaitTermination()
  }

  // --- Example 3: Real-Time Mode with checkpoint interval ---

  def realTimeWithCheckpointInterval(): Unit = {
    readFromKafka()
      .selectExpr(
        "CAST(eventType AS STRING) AS key",
        "CAST(repoName AS STRING) AS value"
      )
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "github-events-processed")
      .option("checkpointLocation", "checkpoints/rtm-interval-demo")
      .trigger(Trigger.RealTime("5 minutes"))
      .start()
      .awaitTermination()
  }

  // --- Example 4: Real-Time Mode with ForeachSink for latency measurement ---

  def realTimeWithLatencyMeasurement(): Unit = {
    readFromKafka()
      .select(
        col("id"),
        col("eventType"),
        col("createdAt"),
        current_timestamp().as("processedAt")
      )
      .writeStream
      .outputMode("update")
      .foreach(new org.apache.spark.sql.ForeachWriter[org.apache.spark.sql.Row] {
        override def open(partitionId: Long, epochId: Long): Boolean = true

        override def process(row: org.apache.spark.sql.Row): Unit = {
          val createdAt = row.getTimestamp(2)
          val processedAt = row.getTimestamp(3)
          if (createdAt != null && processedAt != null) {
            val latencyMs = processedAt.getTime - createdAt.getTime
            println(s"[RTM] Event ${row.getString(0)} | type=${row.getString(1)} | latency=${latencyMs}ms")
          }
        }

        override def close(errorOrNull: Throwable): Unit = {}
      })
      .trigger(Trigger.RealTime())
      .option("checkpointLocation", "checkpoints/rtm-latency-demo")
      .start()
      .awaitTermination()
  }

  // --- Example 5: Stateless filter in Real-Time Mode ---

  def realTimeFilteredPipeline(): Unit = {
    readFromKafka()
      .filter(col("eventType") === "PushEvent")
      .selectExpr(
        "CAST(actorLogin AS STRING) AS key",
        "CAST(repoName AS STRING) AS value"
      )
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "github-push-events")
      .option("checkpointLocation", "checkpoints/rtm-filter-demo")
      .trigger(Trigger.RealTime())
      .start()
      .awaitTermination()
  }

  /*
    Exercises

    1) Run microBatchPipeline() and realTimeWithLatencyMeasurement() side by side (on different topics).
       Compare the latency printed for each event. RTM should show single-digit to low double-digit ms,
       while micro-batch should show 100ms+ due to batch scheduling overhead.

    2) Build an RTM pipeline that reads from github-events, filters to WatchEvent (stars) only,
       and writes to a "github-stars-rt" Kafka topic. Verify with kafka-console-consumer.

    Current limitations of Real-Time Mode (Spark 4.1):
    - Kafka source only (no socket, file, or rate source)
    - Kafka and Foreach sinks only (no console sink)
    - Stateless, single-stage queries only (no aggregations, joins, or transformWithState)
    - Update output mode only
    - Exactly-once semantics (improvement over old Trigger.Continuous which was at-least-once)
   */

  def main(args: Array[String]): Unit = {
    realTimeWithLatencyMeasurement()
  }
}
