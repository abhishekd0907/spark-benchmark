package com.qubole.spark.benchmark

import org.apache.spark.sql.streaming.OutputMode
import org.rogach.scallop.ScallopConf

abstract class BenchmarkConf(args: Array[String]) extends ScallopConf(args) {
  import BenchmarkConf._

  val queryStatusFile = opt[String](name = "query-status-file", required = true, noshort = true)
  val rateRowPerSecond = opt[String](name = "rate-row-per-second", required = true, noshort = true)
  val runTimeInSec = opt[Long](name = "run-time-in-sec", required = true, noshort = true)
  val useRocksDB = opt[Boolean](name = "use-rocks-db", default = Option(false), required = true, noshort = true)
  val numShufflePartition = opt[Int](name = "shuffle-partition", default = Option(8), required = true, noshort = true)
  val triggerIntervalMillis = opt[Long]("trigger-interval", default=Option(30000L), required = true, noshort = true)

  val outputMode = opt[String](name = "output-mode", required = true, noshort = true,
    validate = (s: String) => validOutputModes.map(_.toLowerCase()).contains(s.toLowerCase()))

  def getSparkOutputMode: OutputMode = {
    outputMode.toOption match {
      case Some("append") => OutputMode.Append()
      case Some("update") => OutputMode.Update()
      case Some("complete") => OutputMode.Complete()
      case Some(x) => throw new IllegalArgumentException(s"Not supported output mode: $x")
      case None => throw new IllegalArgumentException("Output mode must be presented!")
    }
  }
}

object BenchmarkConf {
  val validOutputModes = Seq("append", "update", "complete")
}
