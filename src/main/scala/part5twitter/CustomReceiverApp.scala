package part5twitter

import java.net.Socket
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.io.Source

class CustomSocketReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {

  /* This variable is used to store the Socket instance, when the receiver starts, it establishes the socket
  connection and reads data from the socket.
     */
  var mayBeSocket: Option[Socket] = None

  // called asynchronously
  override def onStart(): Unit = {
    mayBeSocket = Some(new Socket(host, port))
    val socket = mayBeSocket.get

    // Read data from the socket and store it
    val source = Source.fromInputStream(socket.getInputStream)
    source.getLines().foreach(line => store(line))

    source.close()
    socket.close()
  }

  // called asynchronously
  override def onStop(): Unit = mayBeSocket.foreach(socket => socket.close())
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
