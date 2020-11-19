package com.qubole.spark.benchmark.utils

import java.io.{BufferedWriter, File, FileWriter}

import scala.util.Random

object SparseFileUtils {

  def writeToFile(data: String, rootPath: String): Option[String] = {
    try {
      val randomFileName = generateRandomFileName()
      val fileName = new File(rootPath +"/" + randomFileName);
      val bw = new BufferedWriter(new FileWriter(fileName))
      bw.write(data)
      bw.close()
      Some(randomFileName)
    } catch {
      case e: Exception =>
        println("Error Occurred while writing file. Cause:\n" + e.getMessage)
        None
    }
  }

  private def generateRandomFileName(): String = {
    Random.alphanumeric.take(6).mkString("")
  }

  def throwUnsupportedOperationException(exception: String): Unit = {
    throw new Exception(exception + "is not supported")
  }

}
