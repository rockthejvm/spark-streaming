package part5twitter

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.Status

object TwitterProject {

  val spark = SparkSession.builder()
    .appName("The Twitter Project")
    .master("local[*]")
    .getOrCreate()

  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  def readTwitter(): Unit = {
    val twitterStream: DStream[Status] = ssc.receiverStream(new TwitterReceiver)
    val tweets: DStream[String] = twitterStream.map { status =>
      val username = status.getUser.getName
      val followers = status.getUser.getFollowersCount
      val text = status.getText

      s"User $username ($followers followers) says: $text"
    }

    tweets.print()
    ssc.start()
    ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    readTwitter()
    ssc.start()
    ssc.awaitTermination()
  }
}
