package part5github

import common.GithubEvent
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.functions.{col, from_json}

class GithubEventsProcessor {

  val spark = SparkSession.builder()
    .appName("Github Events Processor")
    .master("local[2]")
    .config(
      "spark.sql.streaming.stateStore.providerClass",
      "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider" // necessary for multiple state variables per processor
    )
    .getOrCreate()

  import spark.implicits._
  spark.sparkContext.setLogLevel("WARN")

  def readFromKafka(backfill: Boolean = true): Dataset[GithubEvent] =
    spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "github-events")
      .option("startingOffsets", if (backfill) "earliest" else "latest")
      .load()
      .selectExpr("CAST(value AS STRING) as json")
      .select(from_json(col("json"), GithubEvent.schema).as("event"))
      .select(
        col("event.id").as("id"),
        col("event.type").as("eventType"),
        col("event.actor.login").as("actorLogin"),
        col("event.repo.name").as("repoName"),
        col("event.created_at").as("createdAt")
      ).as[GithubEvent]
}
