package part3lowlevel

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

object DStreamsWindowTransformations {

  val spark = SparkSession.builder()
    .appName("DStream Window Transformations")
    .master("local[2]")
    .getOrCreate()

  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  def readLines(): DStream[String] = ssc.socketTextStream("localhost", 12345)

  /*
    window = keep all the values emitted between now and X time back
    window interval updated with every batch
    window interval must be a multiple of the batch interval
   */
  def linesByWindow() = readLines().window(Seconds(10))

  /*
    first arg = window duration
    second arg = sliding duration
    both args need to be a multiple of the original batch duration
   */
  def linesBySlidingWindow() = readLines().window(Seconds(10), Seconds(5))

  // count the number of elements over a window
  def countLinesByWindow() = readLines().countByWindow(Minutes(60), Seconds(30))

  // aggregate data in a different way over a window
  def sumAllTextByWindow() = readLines().map(_.length).window(Seconds(10), Seconds(5)).reduce(_ + _)

  // identical
  def sumAllTextByWindowAlt() = readLines().map(_.length).reduceByWindow(_ + _, Seconds(10), Seconds(5))

  // tumbling windows
  def linesByTumblingWindow() = readLines().window(Seconds(10), Seconds(10))  // batch of batches

  def computeWordOccurrencesByWindow() = {
    // for reduce by key and window you need a checkpoint directory set
    ssc.checkpoint("checkpoints")

    readLines()
      .flatMap(_.split(" "))
      .map(word => (word, 1))
      .reduceByKeyAndWindow(
        (a, b) => a + b, // reduction function
        (a, b) => a - b, // "inverse" function
        Seconds(60), // window duration
        Seconds(30)  // sliding duration
      )
  }

  /**
    * Exercise.
    * Word longer than 10 chars => $2
    * Every other word => $0
    *
    * Input text into the terminal => money made over the past 30 seconds, updated every 10 seconds.
    * - use window
    * - use countByWindow
    * - use reduceByWindow
    * - use reduceByKeyAndWindow
    */

  val moneyPerExpensiveWord = 2

  def showMeTheMoney() = readLines()
    .flatMap(line => line.split(" "))
    .filter(_.length >= 10)
    .map(_ => moneyPerExpensiveWord)
    .reduce(_ + _)
    .window(Seconds(30), Seconds(10))
    .reduce(_ + _)

  def showMeTheMoney2() = readLines()
    .flatMap(_.split(" "))
    .filter(_.length >= 10)
    .countByWindow(Seconds(30), Seconds(10))
    .map(_ * moneyPerExpensiveWord)

  def showMeTheMoney3() = readLines()
    .flatMap(line => line.split(" "))
    .filter(_.length >= 10)
    .map(_ => moneyPerExpensiveWord)
    .reduceByWindow(_ + _, Seconds(30), Seconds(10))

  def showMeTheMoney4() = {
    ssc.checkpoint("checkpoints")

    readLines()
      .flatMap(line => line.split(" "))
      .filter(_.length >= 10)
      .map { word =>
        if (word.length >= 10) ("expensive", 2)
        else ("cheap", 1)
      }
      .reduceByKeyAndWindow(_ + _, _ - _, Seconds(30), Seconds(10))
  }


  def main(args: Array[String]): Unit = {
    computeWordOccurrencesByWindow().print()
    ssc.start()
    ssc.awaitTermination()
  }
}
