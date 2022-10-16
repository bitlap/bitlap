/* Copyright (c) 2022 bitlap.org */
package io.bitlap.spark

import org.apache.spark.sql.connector.catalog.{ Table, TableProvider }
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.{ DataType, LongType, StringType, StructField, StructType }
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util.{ Map => JMap }
import scala.jdk.CollectionConverters.MapHasAsScala

/** @author
 *    梦境迷离
 *  @version 1.0,10/15/22
 */
final class BitlapDataSource extends TableProvider with DataSourceRegister {

  private var schema: StructType = _

  override def inferSchema(caseInsensitiveStringMap: CaseInsensitiveStringMap): StructType = {
    // mock data
    schema = StructType(
      List(
        StructField(
          "key",
          StringType
        ),
        StructField(
          "name",
          StringType
        )
      )
    )
    schema
  }

  override def getTable(schema: StructType, transforms: Array[Transform], properties: JMap[String, String]): Table =
    new BitlapTable(schema, properties.asScala.toMap)

  override def shortName(): String = "bitlap"
}
