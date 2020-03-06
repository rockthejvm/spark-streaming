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

  /**
    * Exercises
    * 1. Compute average tweet length in the past 5 seconds, every 5 seconds. Use a window function.
    * 2. Compute the most popular hashtags (hashtags most used) during a 1 minute window, update every 10 seconds.
    */

  def getAverageTweetLength(): DStream[Double] = {
    val tweets = ssc.receiverStream(new TwitterReceiver)
    tweets
      .map(status => status.getText)
      .map(text => text.length)
      .map(len => (len, 1))
      .reduceByWindow((tuple1, tuple2) => (tuple1._1 + tuple2._1, tuple1._2 + tuple2._2), Seconds(5), Seconds(5))
      .map { megaTuple  =>
        val tweetLengthSum = megaTuple._1
        val tweetCount = megaTuple._2

        tweetLengthSum * 1.0 / tweetCount
      }
  }

  def computeMostPopularHashtags(): DStream[(String, Int)] = {
    val tweets = ssc.receiverStream(new TwitterReceiver)

    ssc.checkpoint("checkpoints")

    tweets
      .flatMap(_.getText.split(" "))
      .filter(_.startsWith("#"))
      .map(hashtag => (hashtag, 1))
      .reduceByKeyAndWindow((x, y) => x + y, (x, y) => x - y, Seconds(60), Seconds(10))
      .transform(rdd => rdd.sortBy(tuple => - tuple._2))
  }

  def main(args: Array[String]): Unit = {
    computeMostPopularHashtags().print()
    ssc.start()
    ssc.awaitTermination()
  }
}
