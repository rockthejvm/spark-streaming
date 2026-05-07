package part5twitter

import java.io.{OutputStream, PrintStream}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import twitter4j._

class TwitterReceiver extends Receiver[Status](StorageLevel.MEMORY_ONLY) {

  /* This variable is used to keep track of the TwitterStream object, which is created when the receiver is started. It starts as None and is updated
  to Some(twitterStream) when the Twitter stream is successfully created in the onStart method. This approach allows the code to check whether the Twitter
  stream has been initialized & is active before performing cleanup or shutdown operations in the onStop method, ensuring that these operations only occur
  if a Twitter stream has been created.
   */
  var mayBeStream: Option[TwitterStream] = None

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

    mayBeStream = Some(twitterStream)
  }

  // run asynchronously
  override def onStop(): Unit = mayBeStream.foreach { twitterStream =>
    twitterStream.cleanUp()
    twitterStream.shutdown()
  }

}
