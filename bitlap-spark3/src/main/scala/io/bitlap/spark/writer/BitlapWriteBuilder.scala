/* Copyright (c) 2022 bitlap.org */
package io.bitlap.spark.writer

import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.connector.write.BatchWrite

/** @author
 *    梦境迷离
 *  @version 1.0,10/15/22
 */
final class BitlapWriteBuilder(val writeInfo: LogicalWriteInfo, val options: Map[String, String]) extends WriteBuilder {
  override def buildForBatch(): BatchWrite = new BitlapBatchWrite(writeInfo, options)
}
