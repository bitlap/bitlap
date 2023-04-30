/* Copyright (c) 2023 bitlap.org */
package org.bitlap.spark.writer

import org.apache.spark.sql.connector.write.*
import org.apache.spark.sql.connector.write.BatchWrite

/** @author
 *    梦境迷离
 *  @version 1.0,10/15/22
 */
final class BitlapWriteBuilder(val writeInfo: LogicalWriteInfo, val options: Map[String, String]) extends WriteBuilder {
  override def buildForBatch(): BatchWrite = new BitlapBatchWrite(writeInfo, options)
}
