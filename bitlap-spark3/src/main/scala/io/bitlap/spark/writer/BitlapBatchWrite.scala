/* Copyright (c) 2022 bitlap.org */
package io.bitlap.spark.writer

import org.apache.spark.sql.connector.write._

/** @author
 *    梦境迷离
 *  @version 1.0,2022/10/16
 */
final class BitlapBatchWrite(
  writeInfo: LogicalWriteInfo,
  options: Map[String, String]
) extends BatchWrite {

  private lazy val bitlapOptions: BitlapDataSourceWriteOptions = new BitlapDataSourceWriteOptions(
    tableName = options.getOrElse("table", null),
    url = options.getOrElse("url", null),
    scan = null,
    tenantId = null,
    schema = null,
    overriddenProps = null,
    false
  )

  override def createBatchWriterFactory(physicalWriteInfo: PhysicalWriteInfo): DataWriterFactory =
    new BitlapDataWriterFactory(writeInfo.schema(), bitlapOptions)

  override def commit(writerCommitMessages: Array[WriterCommitMessage]): Unit = {}

  override def abort(writerCommitMessages: Array[WriterCommitMessage]): Unit = {}
}
