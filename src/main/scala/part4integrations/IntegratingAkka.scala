package part4integrations

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props}
import akka.stream.{ActorMaterializer, OverflowStrategy}
import akka.stream.scaladsl.{Sink, Source}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{Dataset, SparkSession}
import common._

object IntegratingAkka {

  val spark = SparkSession.builder()
    .appName("Integrating Akka")
    .master("local[2]")
    .getOrCreate()

  // foreachBatch
  // receiving system is on another JVM

  import spark.implicits._

  def writeCarsToAkka(): Unit = {
    val carsDS = spark.readStream
      .schema(carsSchema)
      .json("src/main/resources/data/cars")
      .as[Car]

    carsDS
      .writeStream
      .foreachBatch { (batch: Dataset[Car], batchId: Long) =>
        batch.foreachPartition { cars: Iterator[Car] =>
          // this code is run by a single executor

          val system = ActorSystem(s"SourceSystem$batchId", ConfigFactory.load("akkaconfig/remoteActors"))
          val entryPoint = system.actorSelection("akka://ReceiverSystem@localhost:2552/user/entrypoint")

          // send all the data
          cars.foreach(car => entryPoint ! car)
        }
      }
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    writeCarsToAkka()
  }
}

object ReceiverSystem {
  implicit val actorSystem = ActorSystem("ReceiverSystem", ConfigFactory.load("akkaconfig/remoteActors").getConfig("remoteSystem"))
  implicit val actorMaterializer = ActorMaterializer()

  class Destination extends Actor with ActorLogging {
    override def receive = {
      case m => log.info(m.toString)
    }
  }

  object EntryPoint {
    def props(destination: ActorRef) = Props(new EntryPoint(destination))
  }

  class EntryPoint(destination: ActorRef) extends Actor with ActorLogging {
    override def receive = {
      case m =>
        log.info(s"Received $m")
        destination ! m
    }
  }

  def main(args: Array[String]): Unit = {
    val source = Source.actorRef[Car](bufferSize = 10, overflowStrategy = OverflowStrategy.dropHead)
    val sink = Sink.foreach[Car](println)
    val runnableGraph = source.to(sink)
    val destination: ActorRef = runnableGraph.run()

    val entryPoint = actorSystem.actorOf(EntryPoint.props(destination), "entrypoint")
  }
}
