/* Copyright (c) 2023 bitlap.org */
package org.bitlap.spark.reader

import java.io.*

import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapred.InputSplit
import org.apache.spark.SerializableWritable
import org.apache.spark.sql.connector.read.InputPartition

/** @since 2022/10/16
 *  @author
 *    梦境迷离
 */
final class BitlapInputPartition(bitlapInputSplit: BitlapInputSplit) extends InputPartition {

  lazy val bitlapWritableInputSplit: SerializableWritable[BitlapInputSplit] =
    new SerializableWritable[BitlapInputSplit](bitlapInputSplit)
}

final class BitlapInputSplit extends InputSplit with Serializable with Writable {
  override def getLength: Long = 1

  override def getLocations: Array[String] = Array()

  override def write(dataOutput: DataOutput): Unit = ()

  override def readFields(dataInput: DataInput): Unit = ()
}
