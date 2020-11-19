package com.qubole.spark.benchmark.streaming.states

import scala.collection.mutable.{HashMap, List}

import java.io.{File, FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}



class HeaderFileManager(rootPath: String) {

  var headerMap: HashMap[Offsets, FileStatus] = _

  val headerFilePath = rootPath + "/" + "_header"

  def openHeaderFile(): Unit = {

    val file = new File(headerFilePath)
    val fis = new FileInputStream()(file)
    val ois = new ObjectInputStream(fis)
    headerMap = ois.readObject().asInstanceOf[HashMap[Offsets, FileStatus]]
  }




  def commitHeaderFile(): Unit = {

    Serialization.write
  }

  // Get partial and Completely overlapping offsets
  def getOverLappingOffsets(offsets: Offsets): OverlappingOffsets = {
    val partialOverlaps = headerMap.keys.filter { otherOffsets =>
      offsets.isPartialOverlap(otherOffsets)
    }.toList
    val completeOverlaps = headerMap.keys.filter { otherOffsets =>
      offsets.isCompleteOverlap(otherOffsets)
    }.toList
    OverlappingOffsets(partialOverlaps, completeOverlaps)
  }

  def read(other: Offsets): String = {
    val reader = new StringBuilder()

    val effectiveOffsets = headerMap.keys.filterNot(offset => other.isNonOverlap(offset))
    val sortedOffsets = effectiveOffsets.toList.sortWith(_.startOffset < _.startOffset)

    val offsetList = mutable.List[Offsets]()



  }


  def addMapEntry(offsets: Offsets, fileStatus: FileStatus): Unit = {
    headerMap += (offsets -> fileStatus)
  }

  def removeCompleteOverlaps(offsets: List[Offsets]): Unit = {
    headerMap.retain((other, _) => !offsets.contains(other))
  }

}
