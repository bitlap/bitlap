/* Copyright (c) 2022 bitlap.org */
package io.bitlap.spark.reader

import org.apache.spark.sql.connector.read.{ Batch, InputPartition, PartitionReaderFactory, Scan }
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/** @author
 *    梦境迷离
 *  @version 1.0,10/15/22
 */
final class BitlapScan(
  private val schema: StructType,
  private val options: CaseInsensitiveStringMap,
  private val whereClause: String
) extends Scan
    with Batch {

  private var bitlapDataSourceReadOptions: BitlapDataSourceReadOptions = _

  private val url   = options.getOrDefault("url", null)
  private val table = options.getOrDefault("table", null)

  override def readSchema(): StructType = schema

  override def description: String = this.getClass.toString

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    // BitlapDataSourceReadOptions
    // selectStatement  and statement
    // overriddenProps
    bitlapDataSourceReadOptions = new BitlapDataSourceReadOptions(
      url,
      scan = null,
      tenantId = null,
      overriddenProps = null,
      selectStatement = ""
    )
    Array.empty
  }

  override def createReaderFactory(): PartitionReaderFactory =
    new BitlapPartitionReaderFactory(schema, bitlapDataSourceReadOptions)
}
