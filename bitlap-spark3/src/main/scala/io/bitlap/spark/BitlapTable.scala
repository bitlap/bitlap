/* Copyright (c) 2022 bitlap.org */
package io.bitlap.spark

import io.bitlap.spark.reader.BitlapScanBuilder
import io.bitlap.spark.writer.BitlapWriteBuilder
import org.apache.spark.sql.connector.catalog.{ SupportsRead, SupportsWrite, TableCapability }
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{ LogicalWriteInfo, WriteBuilder }
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util.{ Map => JMap, Set => JSet }
import scala.jdk.CollectionConverters.{ MapHasAsJava, SetHasAsJava }

/** @author
 *    梦境迷离
 *  @version 1.0,10/15/22
 */
final class BitlapTable(val schema: StructType, val options: Map[String, String])
    extends SupportsRead
    with SupportsWrite {

  private lazy val tableName: String = options.getOrElse("table", null)

  private final val capability = Set(TableCapability.BATCH_READ, TableCapability.BATCH_WRITE)

  override def newScanBuilder(caseInsensitiveStringMap: CaseInsensitiveStringMap): ScanBuilder =
    new BitlapScanBuilder(_schema = schema, options = caseInsensitiveStringMap)

  override def newWriteBuilder(logicalWriteInfo: LogicalWriteInfo): WriteBuilder =
    new BitlapWriteBuilder(logicalWriteInfo, options)

  override def name(): String = tableName

  override def capabilities(): JSet[TableCapability] = capability.asJava

  def getOptions: JMap[String, String] = options.asJava

}
