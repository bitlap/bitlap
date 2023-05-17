/* Copyright (c) 2023 bitlap.org */
package org.bitlap.spark.writer

import org.bitlap.spark.BitlapOptions

import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.connector.write.streaming.StreamingWrite
import org.apache.spark.sql.sources.{ Filter, InsertableRelation }

/** @author
 *    梦境迷离
 *  @version 1.0,10/15/22
 */
final class BitlapWriteBuilder(val writeInfo: LogicalWriteInfo, val options: BitlapOptions)
    extends WriteBuilder
    with SupportsOverwrite
    with SupportsDynamicOverwrite {

  override def build(): Write = new V1Write {
    override def toBatch: BatchWrite = super.toBatch

    override def toStreaming: StreamingWrite = super.toStreaming

    override def toInsertableRelation: InsertableRelation = {
      new BitlapInsertableRelation(writeInfo, options)
    }
  }

  override def overwrite(filters: Array[Filter]): WriteBuilder = this

  override def overwriteDynamicPartitions(): WriteBuilder = this
}
