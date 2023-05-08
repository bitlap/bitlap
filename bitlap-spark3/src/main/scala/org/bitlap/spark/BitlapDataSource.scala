/* Copyright (c) 2023 bitlap.org */
package org.bitlap.spark

import java.util.Map as JMap

import scala.jdk.CollectionConverters.MapHasAsScala

import org.apache.spark.sql.connector.catalog.TableProvider
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.*
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/** @author
 *    梦境迷离
 *  @version 1.0,10/15/22
 */
final class BitlapDataSource extends TableProvider with DataSourceRegister {
  private var schema: StructType = _

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    if options.get("table") == null then throw new RuntimeException("No Bitlap option table defined")
    if options.get("url") == null then throw new RuntimeException("No Bitlap option url defined")

    // mock data
    schema = StructType(
      List(
        StructField(
          "time",
          LongType
        ),
        StructField(
          "entity",
          IntegerType
        ),
        StructField(
          "dimensions",
          StringType
        ),
        StructField(
          "metric_name",
          StringType
        ),
        StructField(
          "metric_value",
          DoubleType
        )
      )
    )
    schema
  }

  override def getTable(schema: StructType, transforms: Array[Transform], properties: JMap[String, String]) =
    new BitlapTable(schema, properties.asScala.toMap)

  override def shortName(): String = "bitlap"
}
