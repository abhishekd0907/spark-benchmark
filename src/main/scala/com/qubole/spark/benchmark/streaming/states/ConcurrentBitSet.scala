package com.qubole.spark.benchmark.streaming.states

import java.util.concurrent.atomic.AtomicLongArray


object ConcurrentBitSet {
  /**
    * STATE
    */
  val BASE = 64
  val MAX_UNSIGNED_LONG = -1L

  def mask(id: Int) = 1L << id
}

class ConcurrentBitSet(val bitsCount: Long) {

  val bucketsCount: Int = bitsCount.toInt / ConcurrentBitSet.BASE

  val buckets = new AtomicLongArray(bucketsCount)

  var i = 0
  while (i < buckets.length()) {
    buckets.set(i, 0)
    i += 1
  }

  /**
    * API to set Bits for a key
    */
  def set(idx: Long): Unit = {
    val bucketIdx = idx.toInt / ConcurrentBitSet.BASE
    atomicSet(bucketIdx, idx.toInt - (ConcurrentBitSet.BASE * bucketIdx))
  }

  /**
    * API to set Bits for a key
    */
  def get(idx: Long): Boolean = {
    val bucketIdx = idx.toInt / ConcurrentBitSet.BASE
    atomicGet(bucketIdx, idx.toInt - (ConcurrentBitSet.BASE * bucketIdx))
  }

  def clear(): Unit = {
    throw new RuntimeException("not implemented")
  }

  def capacity: Long = this.buckets.length * 64

  /**
    * IMLEMENTATION
    */
  private def atomicGet(bucketIdx: Int, toGet: Int): Boolean = {
    val l = buckets.get(bucketIdx)
    val idxMask = ConcurrentBitSet.mask(toGet)
    (l & idxMask) == idxMask
  }

  private def atomicSet(bucketIdx: Int, toSet: Int): Unit = {
    while ( {
      true
    }) {
      val l = buckets.get(bucketIdx)
      if (buckets.compareAndSet(bucketIdx, l, l | ConcurrentBitSet.mask(toSet))) return
    }
  }

  def longToBinaryStr(num: Long): String = {
    val stringBuilder = new StringBuilder
    var i = 0
    while ( {
      i < ConcurrentBitSet.BASE
    }) {
      val idxMask = ConcurrentBitSet.mask(i)
      stringBuilder.append(if ((num & idxMask) == idxMask) "1"
      else "0")

      {
        i += 1; i - 1
      }
    }
    stringBuilder.toString
  }
}