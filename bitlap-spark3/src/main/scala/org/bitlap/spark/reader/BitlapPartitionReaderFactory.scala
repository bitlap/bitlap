/* Copyright (c) 2023 bitlap.org */
package org.bitlap.spark.reader

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.*
import org.apache.spark.sql.types.StructType

/** @since 2022/10/16
 *  @author
 *    梦境迷离
 */
final class BitlapPartitionReaderFactory(schema: StructType, options: BitlapDataSourceReadOptions)
    extends PartitionReaderFactory {

  override def createReader(inputPartition: InputPartition): PartitionReader[InternalRow] =
    new BitlapPartitionReader(
      options = options,
      schema = schema,
      bitlapPartition = inputPartition.asInstanceOf[BitlapInputPartition]
    )
}
