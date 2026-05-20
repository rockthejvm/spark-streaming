package part5capstone

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger

import scala.concurrent.duration._

object GitHubEventsAnalytics {

  val spark = SparkSession.builder()
    .appName("GitHub Events Analytics")
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

  def readEventsFromKafka(): DataFrame = {
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
  }

  // tumbling window: events per repo per 5-minute window
  def eventsPerRepoPerWindow(): Unit = {
    val eventsDF = readEventsFromKafka()

    val eventsPerRepo = eventsDF
      .withWatermark("createdAt", "30 seconds")
      .groupBy(
        window(col("createdAt"), "5 minutes").as("timeWindow"),
        col("repoName")
      )
      .agg(
        count("*").as("eventCount"),
        countDistinct("actorLogin").as("uniqueActors")
      )
      .select(
        col("timeWindow").getField("start").as("windowStart"),
        col("timeWindow").getField("end").as("windowEnd"),
        col("repoName"),
        col("eventCount"),
        col("uniqueActors")
      )

    eventsPerRepo.writeStream
      .format("console")
      .outputMode("update")
      .option("truncate", "false")
      .start()
      .awaitTermination()
  }

  // sliding window: trending repos (most stars in last 1 hour, updated every 5 minutes)
  def trendingRepos(): Unit = {
    val eventsDF = readEventsFromKafka()

    val starEvents = eventsDF
      .filter(col("eventType") === "WatchEvent")
      .withWatermark("createdAt", "30 seconds")
      .groupBy(
        window(col("createdAt"), "1 hour", "5 minutes").as("timeWindow"),
        col("repoName")
      )
      .agg(count("*").as("starCount"))
      .select(
        col("timeWindow").getField("start").as("windowStart"),
        col("timeWindow").getField("end").as("windowEnd"),
        col("repoName"),
        col("starCount")
      )

    starEvents.writeStream
      .format("console")
      .outputMode("complete")
      .option("truncate", "false")
      .start()
      .awaitTermination()
  }

  // stream-stream join: correlate push events with subsequent star events within 10 minutes
  def pushToStarCorrelation(): Unit = {
    val eventsDF = readEventsFromKafka()

    val pushEvents = eventsDF
      .filter(col("eventType") === "PushEvent")
      .withWatermark("createdAt", "30 seconds")
      .select(
        col("repoName").as("pushRepo"),
        col("actorLogin").as("pusher"),
        col("createdAt").as("pushTime")
      )

    val starEvents = eventsDF
      .filter(col("eventType") === "WatchEvent")
      .withWatermark("createdAt", "30 seconds")
      .select(
        col("repoName").as("starRepo"),
        col("actorLogin").as("starrer"),
        col("createdAt").as("starTime")
      )

    val pushThenStar = pushEvents.join(
      starEvents,
      expr("""
        pushRepo = starRepo AND
        starTime > pushTime AND
        starTime < pushTime + INTERVAL 10 MINUTES
      """),
      "inner"
    )

    pushThenStar.writeStream
      .format("console")
      .outputMode("append")
      .option("truncate", "false")
      .start()
      .awaitTermination()
  }

  // event type distribution over time
  def eventTypeDistribution(): Unit = {
    val eventsDF = readEventsFromKafka()

    val distribution = eventsDF
      .withWatermark("createdAt", "30 seconds")
      .groupBy(
        window(col("createdAt"), "2 minutes").as("timeWindow"),
        col("eventType")
      )
      .count()
      .select(
        col("timeWindow").getField("start").as("windowStart"),
        col("eventType"),
        col("count")
      )

    distribution.writeStream
      .format("console")
      .outputMode("update")
      .option("truncate", "false")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    eventsPerRepoPerWindow()
  }
}
