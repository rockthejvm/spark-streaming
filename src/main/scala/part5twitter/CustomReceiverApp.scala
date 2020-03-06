package part5twitter

import java.net.Socket

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.concurrent.{Future, Promise}
import scala.io.Source

class CustomSocketReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {
  import scala.concurrent.ExecutionContext.Implicits.global

  val socketPromise: Promise[Socket] = Promise[Socket]()
  val socketFuture = socketPromise.future

  // called asynchronously
  override def onStart(): Unit = {
    val socket = new Socket(host, port)

    // run on another thread
    Future {
      Source.fromInputStream(socket.getInputStream)
        .getLines()
        .foreach(line => store(line)) // store makes this string available to Spark
    }

    socketPromise.success(socket)
  }

  // called asynchronously
  override def onStop(): Unit = socketFuture.foreach(socket => socket.close())
}

object CustomReceiverApp {

  val spark = SparkSession.builder()
    .appName("Custom receiver app")
    .master("local[*]")
    .getOrCreate()

  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))

  def main(args: Array[String]): Unit = {
    val dataStream: DStream[String] = ssc.receiverStream(new CustomSocketReceiver("localhost", 12345))
    dataStream.print()
    ssc.start()
    ssc.awaitTermination()
  }

}
