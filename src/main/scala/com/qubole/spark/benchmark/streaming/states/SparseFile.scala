package com.qubole.spark.benchmark.streaming.states

class SparseFile(rootPath: String) {

  import com.qubole.spark.benchmark.utils.SparseFileUtils._

  val headerFileManager = new HeaderFileManager(rootPath)
  var fileSize: Long = _

  // Write the bytes in <data> at <offset> bytes from the start
  // of the file.
  // ask if overwrite needs to be done
  def writeBytes(offset: Long, data: String): Unit = {
    val offsets = computeOffsets(offset, data)
    val overlappingOffsets = headerFileManager.getOverLappingOffsets(offsets)
    if (overlappingOffsets.partialOverlaps.nonEmpty) {
      throwUnsupportedOperationException("Partial Overwrite")
    }
    performWrite(offsets, data, overlappingOffsets.completeOverlaps)
  }

  private def performWrite(offsets: Offsets, data: String, completeOverlaps: List[Offsets]): Unit =
    synchronized {
    val fileName = writeToFile(data, rootPath)
    if (fileName.isDefined) {
      headerFileManager.addMapEntry(offsets, FileStatus(fileName.get))
      headerFileManager.removeCompleteOverlaps(completeOverlaps)
      headerFileManager.commitHeaderFile()
      updateSize(data)
    }
  }

  private def updateSize(str: String): Unit = {
    fileSize += str.getBytes.length
  }

  private def computeOffsets(offset: Long, data: String): Offsets = {
    Offsets(offset, offset + data.size)
  }

  // Read bytes starting from the offset upto the given length.
  def readBytes(offset: Long, size: Int) : String = {
    val offsets = Offsets(offsets, offset + size)
    headerFileManager.readFiles(offsets)
  }

  // Set the size of the file. This can be called at any point
  // in time. Unwritten bytes must be assumed to be zero. If
  // more bytes have already been written, it should be
  // considered gone.
  // size in bytes
  def setSize(size: Long): Unit = {

  }

  def getSize(): Long = {
    fileSize
  }

}

object SparseFile {

  def main(args: Array[String]): Unit = {

    val sparseFile = createSparseFile()

  }

  def createSparseFile(): SparseFile = {

    println("Enter Root Path")
    val rootPath = scala.io.StdIn.readLine()
    new SparseFile(rootPath)
  }

}