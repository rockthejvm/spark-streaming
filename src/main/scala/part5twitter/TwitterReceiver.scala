package part5twitter

import java.io.{OutputStream, PrintStream}

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import twitter4j._

import scala.concurrent.Promise

class TwitterReceiver extends Receiver[Status](StorageLevel.MEMORY_ONLY) {
  import scala.concurrent.ExecutionContext.Implicits.global

  val twitterStreamPromise = Promise[TwitterStream]
  val twitterStreamFuture = twitterStreamPromise.future

  private def simpleStatusListener = new StatusListener {
    override def onStatus(status: Status): Unit = store(status)
    override def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice): Unit = ()
    override def onTrackLimitationNotice(numberOfLimitedStatuses: Int): Unit = ()
    override def onScrubGeo(userId: Long, upToStatusId: Long): Unit = ()
    override def onStallWarning(warning: StallWarning): Unit = ()
    override def onException(ex: Exception): Unit = ex.printStackTrace()
  }

  private def redirectSystemError() = System.setErr(new PrintStream(new OutputStream {
    override def write(b: Array[Byte]): Unit = ()
    override def write(b: Array[Byte], off: Int, len: Int): Unit = ()
    override def write(b: Int): Unit = ()
  }))

  // run asynchronously
  override def onStart(): Unit = {
    redirectSystemError()

    val twitterStream: TwitterStream = new TwitterStreamFactory("src/main/resources")
      .getInstance()
      .addListener(simpleStatusListener)
      .sample("en") // call the Twitter sample endpoint for English tweets

    twitterStreamPromise.success(twitterStream)
  }

  // run asynchronously
  override def onStop(): Unit = twitterStreamFuture.foreach { twitterStream =>
    twitterStream.cleanUp()
    twitterStream.shutdown()
  }

}
