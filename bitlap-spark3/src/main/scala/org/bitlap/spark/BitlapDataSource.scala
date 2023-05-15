/* Copyright (c) 2023 bitlap.org */
package org.bitlap.spark

import java.util.{ Map => JMap }

import scala.jdk.CollectionConverters.MapHasAsScala

import org.apache.spark.sql.connector.catalog.{ Table, TableProvider }
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/** @author
 *    梦境迷离
 *  @version 1.0,10/15/22
 */
final class BitlapDataSource extends TableProvider with DataSourceRegister {

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = SparkUtils.validSchema

  override def supportsExternalMetadata(): Boolean = true

  override def getTable(schema: StructType, transforms: Array[Transform], properties: JMap[String, String]): Table = {
    val options = properties.asScala ++ Map("driver" -> "org.bitlap.Driver")
    new BitlapTable(schema, transforms: Array[Transform], new BitlapOptions(options.toMap))
  }

  override def shortName(): String = "bitlap"
}
