package part3integrations

import org.apache.spark.sql.{Dataset, SparkSession}
import common._

object IntegratingMongoDB {

  val spark = SparkSession.builder()
    .appName("Integrating MongoDB")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  // The MongoDB Spark Connector supports streaming writes natively — no foreachBatch needed.
  def writeStreamToMongoDB(): Unit = {
    val carsDF = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")

    carsDF.writeStream
      .format("mongodb")
      .option("checkpointLocation", "checkpoints/mongodb")
      .option("connection.uri", "mongodb://localhost:27017")
      .option("database", "rtjvm")
      .option("collection", "cars")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  // Alternative: using foreachBatch for full control over each micro-batch.
  // Useful when you need custom logic per batch (e.g. upserts, conditional writes, multi-collection routing).
  def writeStreamWithForeachBatch(): Unit = {
    val carsDF = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")

    val carsDS = carsDF.as[Car]

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

  def main(args: Array[String]): Unit = {
    writeStreamToMongoDB()
  }
}
