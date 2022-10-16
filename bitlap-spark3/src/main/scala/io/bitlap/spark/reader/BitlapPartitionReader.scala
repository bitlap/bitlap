/* Copyright (c) 2022 bitlap.org */
package io.bitlap.spark.reader

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types.StructType

/** @since 2022/10/16
 *  @author
 *    梦境迷离
 */
final class BitlapPartitionReader(
  options: BitlapDataSourceReadOptions,
  schema: StructType,
  bitlapPartition: BitlapPartition
) extends PartitionReader[InternalRow] {

  // TODO  initialize

  private var currentRow: InternalRow         = InternalRow.empty
  private var iterator: Iterator[InternalRow] = Iterator.empty

  override def next(): Boolean =
    if (!iterator.hasNext) {
      false
    } else {
      currentRow = iterator.next()
      true
    }

  override def get(): InternalRow = currentRow

  override def close(): Unit = ()
}
