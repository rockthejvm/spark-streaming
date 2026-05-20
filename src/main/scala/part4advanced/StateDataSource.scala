package part4advanced

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming._

import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit

/*
  State Data Source — Spark 4's way to inspect streaming state outside a running query.

  Why it exists:
  When a stateful streaming query runs, its state (ValueState, ListState, MapState) is stored in checkpoints
  on disk, but it's opaque — you can't just open the files and read them. The State Data Source lets you
  read that checkpoint data as a regular DataFrame, so you can debug, audit, or analyze state after stopping a query.

  Two formats:
  - "state-metadata": shows which stateful operators exist, how many partitions, and the range of batch IDs.
    Use this first to understand what's in the checkpoint.
  - "statestore": reads the actual key-value state contents for a specific state variable (e.g. "totalCount").
    You can optionally specify a batchId to see the state as it was at a particular point in time.

  Typical scenario:
  A stateful query has been running for hours and the results look wrong. You stop the query, use the State
  Data Source to inspect what's accumulated per key, find the bad state, and decide whether to fix the data
  and resume or restart from scratch. Also useful for unit testing stateful logic — run a few batches,
  stop, and assert on the state contents.
*/
object StateDataSource {

  val spark = SparkSession.builder()
    .appName("State Data Source")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  // NOTE: SocialPostRecord, AveragePostStorage, and AverageStorageProcessor are duplicated from
  // TransformWithState.scala — refactor these into a shared location (e.g. common/) so both files can reuse them.

  case class SocialPostRecord(postType: String, count: Int, storageUsed: Int)
  case class AveragePostStorage(postType: String, averageStorage: Double)

  // --- Step 1: Run a stateful query that writes checkpoints ---

  class AverageStorageProcessor extends StatefulProcessor[String, SocialPostRecord, AveragePostStorage] {
    @transient private var totalCount: ValueState[Long] = _
    @transient private var totalStorage: ValueState[Long] = _

    override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
      totalCount = getHandle.getValueState[Long]("totalCount", TTLConfig.NONE)
      totalStorage = getHandle.getValueState[Long]("totalStorage", TTLConfig.NONE)
    }

    override def handleInputRows(
      key: String,
      inputRows: Iterator[SocialPostRecord],
      timerValues: TimerValues
    ): Iterator[AveragePostStorage] = {
      var count = if (totalCount.exists()) totalCount.get() else 0L
      var storage = if (totalStorage.exists()) totalStorage.get() else 0L

      inputRows.foreach { record =>
        count += record.count
        storage += record.storageUsed
      }

      totalCount.update(count)
      totalStorage.update(storage)

      Iterator(AveragePostStorage(key, storage.toDouble / count))
    }
  }

  val checkpointPath = "checkpoints/state-data-source-demo"

  def runStatefulQuery(): Unit = {
    val socialStream = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
      .as[String]
      .map { line =>
        val tokens = line.split(",")
        SocialPostRecord(tokens(0), tokens(1).trim.toInt, tokens(2).trim.toInt)
      }

    socialStream
      .groupByKey(_.postType)
      .transformWithState(
        new AverageStorageProcessor(),
        TimeMode.None(),
        OutputMode.Update()
      )
      .writeStream
      .format("console")
      .outputMode("update")
      .option("checkpointLocation", checkpointPath)
      .start()
      .awaitTermination()
  }

  // --- Step 2: Read state metadata (which operators, how many partitions, batch IDs) ---

  def readStateMetadata(): Unit = {
    val metadataDF = spark.read
      .format("state-metadata")
      .load(checkpointPath)

    println("=== State Metadata ===")
    metadataDF.show(truncate = false)
  }

  // --- Step 3: Read actual state store contents ---

  def readStateStoreContents(): Unit = {
    val stateDF = spark.read
      .format("statestore")
      .option("stateVarName", "totalCount")
      .load(checkpointPath)

    println("=== State Store: totalCount ===")
    stateDF.show(truncate = false)

    val storageStateDF = spark.read
      .format("statestore")
      .option("stateVarName", "totalStorage")
      .load(checkpointPath)

    println("=== State Store: totalStorage ===")
    storageStateDF.show(truncate = false)
  }

  // --- Step 4: Read state at a specific batch ID ---

  def readStateAtBatch(batchId: Long): Unit = {
    val stateDF = spark.read
      .format("statestore")
      .option("batchId", batchId)
      .option("stateVarName", "totalCount")
      .load(checkpointPath)

    println(s"=== State at batch $batchId ===")
    stateDF.show(truncate = false)
  }

  /*
    Exercise:
    1) Run the stateful query (runStatefulQuery) with a few batches of data via nc -lk 12345, then stop it.
    2) Use readStateMetadata() to see the operator info and batch range.
    3) Use readStateStoreContents() to inspect the accumulated state for each key.
    4) Use readStateAtBatch(0) to see what the state looked like after the first batch.
    5) Restart runStatefulQuery() and verify it resumes from the checkpoint with the correct state.
   */

  def main(args: Array[String]): Unit = {
    // Step 1: first run the stateful query, send some data, then stop it
    // runStatefulQuery()

    // Step 2: after stopping, inspect the state
    readStateMetadata()
    readStateStoreContents()
  }
}
