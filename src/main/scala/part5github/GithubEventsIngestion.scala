package part5github

import common.GithubEvent
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object GithubEventsIngestion extends GithubEventsProcessor {

  // basic ingestion: read from Kafka, print to console
  def logEvents() =
    readFromKafka()
      .writeStream
      .format("console")
      .outputMode("append")
      .option("truncate", "false")
      .start()
      .awaitTermination()

  // listen just for PushEvents, ForkEvents
  def logCodeEvents() =
    readFromKafka()
      .filter(col("eventType").isin("PushEvent", "ForkEvent"))
      .writeStream
      .format("console")
      .outputMode("append")
      .option("truncate", "false")
      .start()
      .awaitTermination()

  // backfill and dump to postgres
  def writeToPostgres() =
    readFromKafka(backfill = true)
      .writeStream
      .foreachBatch { (batch: Dataset[GithubEvent], _: Long) =>
        val driver = "org.postgresql.Driver"
        val url = "jdbc:postgresql://localhost:5432/rtjvm"
        val user = "docker"
        val password = "docker"

        batch.write
          .format("jdbc")
          .option("driver", driver)
          .option("url", url)
          .option("user", user)
          .option("password", password)
          .option("dbtable", "public.github_events")
          .mode("append")
          .save
      }
      .trigger(Trigger.AvailableNow()) // will read whatever is currently in Kafka then stop
      .start()
      .awaitTermination()

  def main(args: Array[String]): Unit = {
    writeToPostgres()
  }
}
