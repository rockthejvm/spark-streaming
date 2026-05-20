package part4advanced

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

import scala.concurrent.duration._

/*
  Real-Time Mode (Spark 4.1) — event-at-a-time processing with sub-millisecond latency.

  In micro-batch mode, Spark collects events into batches and processes them periodically (e.g. every 1 second).
  This adds at least one batch interval of latency. Real-Time Mode (Trigger.RealTime()) processes each event
  individually as soon as it arrives, achieving single-digit millisecond latency.

  To produce test data, run kafka-console-producer from Docker:
    docker exec -it rockthejvm-sparkstreaming-kafka kafka-console-producer --bootstrap-server localhost:9092 --topic rtm-input
  Then type messages (one per line). Each message becomes a Kafka record with an automatic timestamp.

  Current limitations (Spark 4.1):
  - Kafka source only (no socket, file, or rate source)
  - Kafka and Foreach sinks only (no console sink)
  - Stateless, single-stage queries only (no aggregations, joins, or transformWithState)
  - Update output mode only
  - Exactly-once semantics (improvement over old Trigger.Continuous which was at-least-once)
*/
object RealTimeMode {

  val spark = SparkSession.builder()
    .appName("Real-Time Mode")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  // Every Kafka record has: key, value, topic, partition, offset, timestamp, timestampType.
  // We use "timestamp" (when Kafka received the record) to measure end-to-end latency.
  def readFromKafka(): DataFrame = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "rtm-input")
    .option("startingOffsets", "latest")
    .load()
    .select(
      col("value").cast("string").as("message"),
      col("timestamp").as("kafkaTimestamp") // when Kafka received the record
    )

  // --- Example 1: Micro-batch latency baseline ---
  // Each event waits up to 1 second (the batch interval) before being processed.
  // The ForeachWriter prints the delay between Kafka ingestion and Spark processing.

  def microBatchLatency(): Unit = {
    readFromKafka()
      .select(
        col("message"),
        col("kafkaTimestamp"),
        current_timestamp().as("processedAt")
      )
      .writeStream
      .outputMode("update")
      .foreach(new org.apache.spark.sql.ForeachWriter[org.apache.spark.sql.Row] {
        override def open(partitionId: Long, epochId: Long): Boolean = true
        override def process(row: org.apache.spark.sql.Row): Unit = {
          val kafkaTs = row.getTimestamp(1)
          val processedAt = row.getTimestamp(2)
          val latencyMs = processedAt.getTime - kafkaTs.getTime
          println(s"[MICRO-BATCH] message='${row.getString(0)}' | latency=${latencyMs}ms")
        }
        override def close(errorOrNull: Throwable): Unit = {}
      })
      .trigger(Trigger.ProcessingTime(1.second))
      .option("checkpointLocation", "checkpoints/rtm-microbatch")
      .start()
      .awaitTermination()
  }

  // --- Example 2: Real-Time Mode latency ---
  // Same pipeline, only the trigger changes. Events are processed immediately.
  // You should see single-digit ms latency vs hundreds of ms in micro-batch.

  def realTimeLatency(): Unit = {
    readFromKafka()
      .select(
        col("message"),
        col("kafkaTimestamp"),
        current_timestamp().as("processedAt")
      )
      .writeStream
      .outputMode("update")
      .foreach(new org.apache.spark.sql.ForeachWriter[org.apache.spark.sql.Row] {
        override def open(partitionId: Long, epochId: Long): Boolean = true
        override def process(row: org.apache.spark.sql.Row): Unit = {
          val kafkaTs = row.getTimestamp(1)
          val processedAt = row.getTimestamp(2)
          val latencyMs = processedAt.getTime - kafkaTs.getTime
          println(s"[REAL-TIME]   message='${row.getString(0)}' | latency=${latencyMs}ms")
        }
        override def close(errorOrNull: Throwable): Unit = {}
      })
      .trigger(Trigger.RealTime())
      .option("checkpointLocation", "checkpoints/rtm-realtime")
      .start()
      .awaitTermination()
  }

  // --- Example 3: Real-Time Mode with stateless filter (Kafka-to-Kafka) ---
  // Filters messages and forwards them to another Kafka topic.
  // Demonstrates that stateless transforms (filter, map, select) work in RTM.

  def realTimeFilter(): Unit = {
    readFromKafka()
      .filter(length(col("message")) > 5)
      .selectExpr(
        "CAST(message AS STRING) AS key",
        "CAST(message AS STRING) AS value"
      )
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "rtm-output")
      .option("checkpointLocation", "checkpoints/rtm-filter")
      .trigger(Trigger.RealTime())
      .start()
      .awaitTermination()
  }

  /*
    Exercise:
    Run microBatchLatency() and realTimeLatency() one at a time. In each case, type a few messages
    into kafka-console-producer and compare the printed latency values.
    Micro-batch should show ~1000ms+ (the batch interval), Real-Time should show single-digit ms.
   */

  def main(args: Array[String]): Unit = {
    realTimeLatency()
  }
}
