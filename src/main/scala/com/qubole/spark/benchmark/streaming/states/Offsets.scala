package com.qubole.spark.benchmark.streaming.states

case class Offsets(startOffset: Long,
                   endOffset: Long,
                   isZeroOffset: Boolean = false) {

  def isPartialOverlap(other: Offsets): Boolean = {
    !isCompleteOverlap(other) && !isNonOverlap(other)
  }

  def isCompleteOverlap(other: Offsets): Boolean = {
    startOffset <= other.startOffset && endOffset >= other.endOffset
  }

  def isNonOverlap(other: Offsets): Boolean = {
    endOffset < other.startOffset || startOffset > other.endOffset
  }


}

case class FileStatus(path: String)

case class OverlappingOffsets(partialOverlaps: List[Offsets],
                              completeOverlaps: List[Offsets])