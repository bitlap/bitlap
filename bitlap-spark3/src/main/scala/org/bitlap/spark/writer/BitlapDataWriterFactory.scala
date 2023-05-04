/* Copyright (c) 2023 bitlap.org */
package org.bitlap.spark.writer

import org.apache.spark.sql.connector.write.DataWriterFactory
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.DataWriter

/** @author
 *    梦境迷离
 *  @version 1.0,2022/10/16
 */
final class BitlapDataWriterFactory(schema: StructType, options: BitlapDataSourceWriteOptions)
    extends DataWriterFactory {

  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] =
    new BitlapDataWriter(schema, options)
}
