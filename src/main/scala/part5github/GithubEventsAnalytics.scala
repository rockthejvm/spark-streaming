package part5github

import org.apache.spark.sql.functions._

object GithubEventsAnalytics extends GithubEventsProcessor {

  // window aggregations
  // count of all events in 30-second windows, per repo
  def eventsPerRepoPerWindow() = {
    val eventsDF = readFromKafka()

    val eventsPerRepo = eventsDF
      .withWatermark("createdAt", "10 seconds")
      .groupBy(
        col("repoName"),
        window(col("createdAt"), "30 seconds").as("timeWindow")
      )
      .agg(
        count("*").as("eventCount")
      )
      .selectExpr(
        "timeWindow.start as windowStart",
        "timeWindow.end as windowEnd",
        "repoName",
        "eventCount"
      )

    eventsPerRepo.writeStream
      .format("console")
      .outputMode("update")
      .option("truncate", "false")
      .start()
      .awaitTermination()
  }

  // sliding window of trending repos (counting StarEvents), size = 30s, sliding every 10s
  def trendingRepos() = {
    val eventsDF = readFromKafka()

    val starEvents = eventsDF
      .withWatermark("createdAt", "10 seconds")
      .filter(col("eventType") === "StarEvent")
      .groupBy(
        col("repoName"),
        window(col("createdAt"), "30 seconds", "10 seconds").as("timeWindow")
      )
      .agg(
        count("*").as("starCount")
      )
      .selectExpr(
        "timeWindow.start as windowStart",
        "timeWindow.end as windowEnd",
        "repoName",
        "starCount"
      )

    starEvents.writeStream
      .format("console")
      .outputMode("update")
      .option("truncate", "false")
      .start()
      .awaitTermination()
  }

  // stream-stream join
  // correlate push events with subsequent star events (on the same repo) within 10 seconds
  def pushToStarCorrelation() = {
    val eventsDF = readFromKafka()

    val pushEvents = eventsDF
      .withWatermark("createdAt", "10 seconds")
      .filter(col("eventType") === "PushEvent")
      .select(
        col("repoName").as("pushRepo"),
        col("actorLogin").as("pushActor"),
        col("createdAt").as("pushTime")
      )

    val starEvents = eventsDF
      .withWatermark("createdAt", "10 seconds")
      .filter(col("eventType") === "StarEvent")
      .select(
        col("repoName").as("starRepo"),
        col("actorLogin").as("starActor"),
        col("createdAt").as("starTime")
      )

    val pushThenStar = pushEvents.join(
      starEvents,
      expr(
        """
          |pushRepo = starRepo AND
          |pushTime < starTime AND
          |starTime < pushTime + INTERVAL 10 SECONDS
          |""".stripMargin),
      "inner"
    )

    pushThenStar.writeStream
      .format("console")
      .outputMode("append")
      .option("truncate", "false")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    pushToStarCorrelation()
  }
}
