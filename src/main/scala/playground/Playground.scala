package playground

import org.apache.spark.sql.{Row, SparkSession}
import common._

/**
  * This is a small application that loads some manually inserted rows into a Spark DataFrame
  * and runs a simple streaming query from a socket source.
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

    val cars = Seq(
      Row("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA"),
      Row("buick skylark 320",15.0,8L,350.0,165L,3693L,11.5,"1970-01-01","USA"),
      Row("plymouth satellite",18.0,8L,318.0,150L,3436L,11.0,"1970-01-01","USA"),
      Row("amc rebel sst",16.0,8L,304.0,150L,3433L,12.0,"1970-01-01","USA"),
      Row("ford torino",17.0,8L,302.0,140L,3449L,10.5,"1970-01-01","USA"),
      Row("ford galaxie 500",15.0,8L,429.0,198L,4341L,10.0,"1970-01-01","USA"),
      Row("chevrolet impala",14.0,8L,454.0,220L,4354L,9.0,"1970-01-01","USA"),
      Row("plymouth fury iii",14.0,8L,440.0,215L,4312L,8.5,"1970-01-01","USA"),
      Row("pontiac catalina",14.0,8L,455.0,225L,4425L,10.0,"1970-01-01","USA"),
      Row("amc ambassador dpl",15.0,8L,390.0,190L,3850L,8.5,"1970-01-01","USA")
    )

    val carsRows = sc.parallelize(cars)
    val carsDF = spark.createDataFrame(carsRows, carsSchema)

    // if the schema and the contents of the DataFrame are printed correctly,
    // the Spark SQL library works correctly
    carsDF.printSchema()
    carsDF.show()

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
