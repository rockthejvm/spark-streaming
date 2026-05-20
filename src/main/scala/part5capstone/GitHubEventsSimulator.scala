package part5capstone

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import java.sql.Timestamp
import java.time.Instant
import java.util.{Properties, UUID}
import scala.util.Random

object GitHubEventsSimulator {

  val eventTypes = List("PushEvent", "WatchEvent", "ForkEvent", "IssuesEvent", "PullRequestEvent", "CreateEvent")
  val actors = List("alice-dev", "bob-coder", "carol-ml", "dave-ops", "eve-data", "frank-web", "grace-api", "heidi-cli")
  val repos = List(
    "apache/spark", "scala/scala", "typelevel/cats", "zio/zio", "akka/akka",
    "softwaremill/sttp", "http4s/http4s", "circe/circe", "fs2/fs2", "lampepfl/dotty",
    "playframework/playframework", "slick/slick", "twitter/finagle", "spotify/scio"
  )

  val random = new Random()

  def generateEvent(withJitter: Boolean = true): String = {
    val id = UUID.randomUUID().toString
    val eventType = eventTypes(random.nextInt(eventTypes.length))
    val actor = actors(random.nextInt(actors.length))
    val repo = repos(random.nextInt(repos.length))

    // introduce occasional out-of-order timestamps (for watermark demos)
    val jitterMs = if (withJitter && random.nextDouble() < 0.15) -random.nextInt(30000) else 0
    val timestamp = Instant.now().plusMillis(jitterMs).toString

    s"""{"id":"$id","type":"$eventType","actor":{"login":"$actor"},"repo":{"name":"$repo"},"created_at":"$timestamp"}"""
  }

  def createKafkaProducer(): KafkaProducer[String, String] = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[StringSerializer].getName)
    new KafkaProducer[String, String](props)
  }

  def produceToKafka(
    topic: String = "github-events",
    eventsPerSecond: Int = 5,
    durationSeconds: Int = 300
  ): Unit = {
    val producer = createKafkaProducer()
    val totalEvents = eventsPerSecond * durationSeconds
    val delayMs = 1000 / eventsPerSecond

    println(s"Starting GitHub Events Simulator: $eventsPerSecond events/sec for ${durationSeconds}s")
    println(s"Producing to Kafka topic: $topic")

    try {
      (1 to totalEvents).foreach { i =>
        val event = generateEvent()
        val record = new ProducerRecord[String, String](topic, event)
        producer.send(record)

        if (i % 50 == 0) println(s"Produced $i events...")
        Thread.sleep(delayMs)
      }
    } finally {
      producer.flush()
      producer.close()
      println(s"Done. Produced $totalEvents events.")
    }
  }

  // burst mode: simulate a bot pushing many events rapidly (for anomaly detection in lesson 5.4)
  def produceBotBurst(
    topic: String = "github-events",
    actor: String = "suspicious-bot",
    count: Int = 50
  ): Unit = {
    val producer = createKafkaProducer()
    println(s"Simulating bot burst: $count events from $actor")

    try {
      (1 to count).foreach { _ =>
        val id = UUID.randomUUID().toString
        val timestamp = Instant.now().toString
        val event = s"""{"id":"$id","type":"PushEvent","actor":{"login":"$actor"},"repo":{"name":"apache/spark"},"created_at":"$timestamp"}"""
        producer.send(new ProducerRecord[String, String](topic, event))
        Thread.sleep(50)
      }
    } finally {
      producer.flush()
      producer.close()
      println("Bot burst complete.")
    }
  }

  def main(args: Array[String]): Unit = {
    produceToKafka()
  }
}
