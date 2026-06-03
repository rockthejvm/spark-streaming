package common

import org.apache.spark.sql.types.{StringType, StructField, StructType, TimestampType}

import java.sql.Timestamp

case class GithubEvent(
                      id: String,
                      eventType: String,
                      actorLogin: String,
                      repoName: String,
                      createdAt: Timestamp
                      )

object GithubEvent {
  val schema = StructType(Array(
    StructField("id", StringType),
    StructField("type", StringType),
    StructField("actor", StructType(Array(
      StructField("login", StringType),
    ))),
    StructField("repo", StructType(Array(
      StructField("name", StringType),
    ))),
    StructField("created_at", TimestampType),
  ))
}