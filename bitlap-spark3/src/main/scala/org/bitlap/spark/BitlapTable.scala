/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.spark

import java.util.{ Map => JMap, Set => JSet }

import scala.jdk.CollectionConverters._

import org.bitlap.spark.reader.BitlapScanBuilder
import org.bitlap.spark.writer.BitlapWriteBuilder

import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/** @author
 *    梦境迷离
 *  @version 1.0,10/15/22
 */
final class BitlapTable(val schema: StructType, transforms: Array[Transform], val options: BitlapOptions)
    extends SupportsRead
    with SupportsWrite {

  override def newScanBuilder(caseInsensitiveStringMap: CaseInsensitiveStringMap): ScanBuilder =
    new BitlapScanBuilder(schema, caseInsensitiveStringMap)

  override def newWriteBuilder(logicalWriteInfo: LogicalWriteInfo): WriteBuilder =
    new BitlapWriteBuilder(logicalWriteInfo, options)

  override def name(): String = options.tableOrQuery

  override def capabilities(): JSet[TableCapability] = BitlapTable.SUPPORT_CAPABILITIES.asJava

  def getOptions: JMap[String, String] = options.parameters.asJava
}

object BitlapTable {

  val SUPPORT_CAPABILITIES: Set[TableCapability] = Set(
    TableCapability.BATCH_READ,
    TableCapability.BATCH_WRITE,
//    TableCapability.OVERWRITE_BY_FILTER,
    TableCapability.OVERWRITE_DYNAMIC,
    TableCapability.V1_BATCH_WRITE,
    TableCapability.ACCEPT_ANY_SCHEMA,
    TableCapability.TRUNCATE
  )
}
