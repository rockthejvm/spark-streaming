package part3integrations

import org.apache.spark.sql.{Dataset, SparkSession}
import common._

object IntegratingMongoDB {

  val spark = SparkSession.builder()
    .appName("Integrating MongoDB")
    .master("local[2]")
    .config("spark.mongodb.write.connection.uri", "mongodb://localhost:27017/rtjvm.cars")
    .getOrCreate()

  import spark.implicits._

  def writeStreamToMongoDB(): Unit = {
    val carsDF = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")

    val carsDS = carsDF.as[Car]

    // Option 1: using foreachBatch (most common pattern)
    carsDS.writeStream
      .foreachBatch { (batch: Dataset[Car], _: Long) =>
        batch.write
          .format("mongodb")
          .option("connection.uri", "mongodb://localhost:27017")
          .option("database", "rtjvm")
          .option("collection", "cars")
          .mode("append")
          .save()
      }
      .start()
      .awaitTermination()
  }

  /**
    * Exercise: write a streaming dataset to MongoDB using the native Spark connector format.
    * Hint: the MongoDB Spark Connector supports streaming writes with checkpoints.
    */
  def writeStreamToMongoDBNative(): Unit = {
    val carsDF = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")

    carsDF.writeStream
      .format("mongodb")
      .option("checkpointLocation", "checkpoints/mongodb")
      .option("connection.uri", "mongodb://localhost:27017")
      .option("database", "rtjvm")
      .option("collection", "cars_stream")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    writeStreamToMongoDB()
  }
}
