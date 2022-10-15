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

  override def readSchema(): StructType = schema

  override def description: String = this.getClass.toString

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = ???

  override def createReaderFactory(): PartitionReaderFactory = ???
}
