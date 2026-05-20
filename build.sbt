name := "spark-streaming"

version := "2.0"

scalaVersion := "2.13.17"

val sparkVersion = "4.1.1"
val postgresVersion = "42.7.4"
val mongoSparkVersion = "10.4.0"
val kafkaVersion = "3.7.1"
val log4jVersion = "2.24.3"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,

  // Structured Streaming Kafka connector
  "org.apache.spark" % "spark-sql-kafka-0-10_2.13" % sparkVersion,

  // PostgreSQL
  "org.postgresql" % "postgresql" % postgresVersion,

  // MongoDB
  "org.mongodb.spark" %% "mongo-spark-connector" % mongoSparkVersion,

  // Kafka client (for the simulator producer)
  "org.apache.kafka" % "kafka-clients" % kafkaVersion,

  // Logging
  "org.apache.logging.log4j" % "log4j-api" % log4jVersion,
  "org.apache.logging.log4j" % "log4j-core" % log4jVersion,
  "org.apache.logging.log4j" % "log4j-slf4j2-impl" % log4jVersion
)

Compile / run / javaOptions ++= Seq(
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED"
)
