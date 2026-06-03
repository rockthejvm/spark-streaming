package part5github

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import java.time.Instant
import java.util.{Properties, UUID}
import scala.util.Random

object GithubEventsSimulator {

  val eventTypes = List(
    "PushEvent",
    "WatchEvent",
    "DeleteEvent",
    "CreateEvent",
    "ForkEvent",
    "StarEvent"
  )

  val actors = List(
    "alice",
    "bob",
    "charlie",
    "dave",
    "eve",
    "frank",
    "grace"
  )

  val repos = List(
    "apache/spark",
    "scala/scala",
    "apache/kafka",
    "typelevel/cats",
    "twitter/finagle",
    "playframework/playframework",
    "akka/akka",
  )

  val random = new Random()

  def generateEvent(withJitter: Boolean = true): String = {
    val id = UUID.randomUUID().toString
    val eventType = eventTypes(random.nextInt(eventTypes.length))
    val actor = actors(random.nextInt(actors.length))
    val repo = repos(random.nextInt(repos.length))

    val jitterMs = {
      if (withJitter && random.nextDouble() < 0.15)
        -random.nextInt(30000)
      else
        0
    }

    val timestamp = Instant.now.plusMillis(jitterMs).toString
    s"""
       |{
       |  "id":"$id",
       |  "type":"$eventType",
       |  "actor":{"login":"$actor"},
       |  "repo":{"name":"$repo"},
       |  "created_at":"$timestamp"
       |}
       |""".stripMargin
  }

  def createKafkaProducer(): KafkaProducer[String, String] = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[StringSerializer].getName)
    new KafkaProducer[String, String](props)
  }

  def produceToKafka(topic: String = "github-events", eventsPerSecond: Int = 5, durationInSeconds: Int = 300) = {
    val producer = createKafkaProducer()
    val totalEvents = eventsPerSecond * durationInSeconds
    val delayMs = 1000 / eventsPerSecond

    println(s"Starting events simulator: $eventsPerSecond events/s for $durationInSeconds s")

    try {
      (1 to totalEvents).foreach { i =>
        val event = generateEvent()
        val record = new ProducerRecord[String, String](topic, event)
        producer.send(record)

        if (i % 50 == 0) println(s"Produced $i events.")

        Thread.sleep(delayMs)
      }
    } finally {
      producer.flush()
      producer.close()
      println(s"Done. generated $totalEvents events.")
    }
  }

  def produceBotBurst(topic: String = "github-events", count: Int = 100) = {
    val producer = createKafkaProducer()

    println(s"Starting bot simulator")

    try {
      (1 to count).foreach { i =>
        val event = generateEvent()
        val record = new ProducerRecord[String, String](topic, event)
        producer.send(record)

        Thread.sleep(20)
      }
    } finally {
      producer.flush()
      producer.close()
      println(s"Done. generated $count events.")
    }
  }

  def main(args: Array[String]): Unit = {
    produceBotBurst()
  }
}
