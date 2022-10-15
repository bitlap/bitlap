/* Copyright (c) 2022 bitlap.org */
package io.bitlap.spark.writer

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{ DataWriter, WriterCommitMessage }
import org.apache.spark.sql.types.StructType

/** @author
 *    梦境迷离
 *  @version 1.0,10/15/22
 */
final class BitlapDataWriter(private val schema: StructType) extends DataWriter[InternalRow] {

  override def write(t: InternalRow): Unit = ???

  override def commit(): WriterCommitMessage = ???

  override def abort(): Unit = ???

  override def close(): Unit = ???
}
