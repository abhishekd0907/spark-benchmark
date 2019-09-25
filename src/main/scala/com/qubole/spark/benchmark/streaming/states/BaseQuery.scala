package com.qubole.spark.benchmark.streaming.states

import com.qubole.spark.benchmark.utils.WriteProgressToFileListener
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{avg, round, max, sum}

abstract class BaseQuery(conf: StateStoreBenchmarkConf, appName: String, queryName: String) {

  def applyOperations(spark: SparkSession, df: DataFrame): DataFrame

  def runBenchmark(): Unit = {
    val queryStatusFile = conf.queryStatusFile()
    val rateRowPerSecond = conf.rateRowPerSecond()
    val runTimeInSec = conf.runTimeInSec()
    val triggerIntervalMillis = conf.triggerIntervalMillis()

    val spark = SparkSession
      .builder()
      .appName(appName)
      .getOrCreate()

    spark.streams.addListener( new WriteProgressToFileListener(queryStatusFile))
    if (conf.useRocksDB()) {
      spark.sql(s"""set spark.sql.streaming.stateStore.providerClass =
      org.apache.spark.sql.execution.streaming.state.RocksDbStateStoreProvider""")
    }
    spark.sql(s"set spark.sql.shuffle.partitions = ${conf.numShufflePartition()}")
    spark.sql(s"set spark.sql.streaming.metricsEnabled = true")
    spark.sql(s"set spark.sql.streaming.minBatchesToRetain= 5")

    val df = spark.readStream
      .format("rate")
      .option("rowsPerSecond", rateRowPerSecond)
      .load()

    df.printSchema()

      val query = applyOperations(spark, df)
        .writeStream.foreach(
          new ForeachWriter[Row] {
            def open(partitionId: Long, version: Long): Boolean = {
              // nothing
              false
            }
            def process(record: Row) = {
              // do nothing
            }
            def close(errorOrNull: Throwable): Unit = {
              // Close the connection
            }
        }
    )
      .trigger(Trigger.ProcessingTime(triggerIntervalMillis))
      .outputMode(conf.getSparkOutputMode)
      .start()

    query.awaitTermination(runTimeInSec * 1000)

  }
}
