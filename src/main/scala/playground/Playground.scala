package playground

import org.apache.spark.sql.{Row, SparkSession}
import common._

/**
  * This is a small application that loads some data from a socket source and prints it.
  * How to run this:
  * - run 'nc -lk 12345' in the terminal, or the SocketSource application in this package
  * - Run Playground
  * - start typing some lines in the terminal or in the stdin of the SocketSource app
  * - watch the data printed in Spark
  *
  * Feel free to modify this code as you see fit, fiddle with the code and play with your own exercises, ideas and datasets.
  *
  * Daniel @ Rock the JVM
  */
object Playground {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark Streaming Playground")
      .master("local[2]")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    // test Structured Streaming: read from a socket and print to console
    // to test: run `nc -lk 12345` in a terminal, then run this app and type some lines
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    // if the stream starts and you see batches in the console,
    // the Spark Structured Streaming library works correctly and you can safely jump into the course!
    lines.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }
}
