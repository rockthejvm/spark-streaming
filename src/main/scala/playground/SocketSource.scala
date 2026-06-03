package playground

import java.io.{OutputStreamWriter, PrintWriter}
import java.net.{InetSocketAddress, ServerSocket}
import scala.io.Source

/**
  * This is a simple application that opens a socket source on a port, waits for connection and then
  * sends all data to the socket exactly as printed in the stdin.
  *
  * You can use it instead of a local netcat.
  */
object SocketSource {
  def main(args: Array[String]): Unit = {
    val serverSocket = new ServerSocket()
    val port = 12345
    try {
      serverSocket.bind(new InetSocketAddress(port))
      println(s"Socket bound to $port. Waiting for connection...")
      val socket = serverSocket.accept()
      println("Connected. Write your text data here, one row per line.")
      val writer = new PrintWriter(new OutputStreamWriter(socket.getOutputStream))
      Source.stdin.getLines().foreach { line =>
        writer.println(line)
        writer.flush()
      }
    } finally {
      serverSocket.close()
    }
  }
}
