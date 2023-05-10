/* Copyright (c) 2023 bitlap.org */
package org.bitlap.spark.writer

import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.connector.write.BatchWrite
import org.apache.spark.sql.connector.write.streaming.StreamingWrite
import org.apache.spark.sql.sources.{Filter, InsertableRelation}

/** @author
 *    梦境迷离
 *  @version 1.0,10/15/22
 */
final class BitlapWriteBuilder(val writeInfo: LogicalWriteInfo, val options: Map[String, String]) extends WriteBuilder with SupportsOverwrite with SupportsDynamicOverwrite {

  override def build(): Write = new V1Write {
    override def toBatch: BatchWrite = {
      // new BitlapBatchWrite(writeInfo, options)
      super.toBatch
    }

    override def toStreaming: StreamingWrite = super.toStreaming

    override def toInsertableRelation: InsertableRelation = {
      new BitlapInsertableRelation
    }
  }

  override def overwrite(filters: Array[Filter]): WriteBuilder = this

  override def overwriteDynamicPartitions(): WriteBuilder = this
}
