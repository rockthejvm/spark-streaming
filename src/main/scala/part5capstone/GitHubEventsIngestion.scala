package part5capstone

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger
import common.GitHubEvent

object GitHubEventsIngestion {

  val spark = SparkSession.builder()
    .appName("GitHub Events Ingestion")
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

  def readFromKafka(): Dataset[GitHubEvent] = {
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

  // basic ingestion: read from Kafka, print to console
  def logEvents(): Unit = {
    readFromKafka()
      .writeStream
      .format("console")
      .outputMode("append")
      .option("truncate", "false")
      .start()
      .awaitTermination()
  }

  // filtered ingestion: only PushEvents and PullRequestEvents
  def logCodeEvents(): Unit = {
    readFromKafka()
      .filter(col("eventType").isin("PushEvent", "PullRequestEvent"))
      .writeStream
      .format("console")
      .outputMode("append")
      .option("truncate", "false")
      .start()
      .awaitTermination()
  }

  // write to PostgreSQL using foreachBatch
  def writeToPostgres(): Unit = {
    readFromKafka()
      .writeStream
      .foreachBatch { (batch: Dataset[GitHubEvent], _: Long) =>
        batch.write
          .format("jdbc")
          .option("driver", "org.postgresql.Driver")
          .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
          .option("user", "docker")
          .option("password", "docker")
          .option("dbtable", "public.github_events")
          .mode("append")
          .save()
      }
      .option("checkpointLocation", "checkpoints/github-ingestion")
      .start()
      .awaitTermination()
  }

  // backfill mode: process all available Kafka data then stop
  def backfillFromKafka(): Unit = {
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "github-events")
      .option("startingOffsets", "earliest")
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
      .writeStream
      .format("console")
      .outputMode("append")
      .option("truncate", "false")
      .trigger(Trigger.AvailableNow())
      .option("checkpointLocation", "checkpoints/github-backfill")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    logEvents()
  }
}
