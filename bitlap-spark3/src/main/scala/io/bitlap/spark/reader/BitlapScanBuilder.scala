/* Copyright (c) 2022 bitlap.org */
package io.bitlap.spark.reader

import org.apache.spark.sql.connector.read.{
  Scan,
  ScanBuilder,
  SupportsPushDownFilters,
  SupportsPushDownRequiredColumns
}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/** @author
 *    梦境迷离
 *  @version 1.0,10/15/22
 */
final class BitlapScanBuilder(private val _schema: StructType, val options: CaseInsensitiveStringMap)
    extends ScanBuilder
    with SupportsPushDownFilters
    with SupportsPushDownRequiredColumns {

  private var schema: StructType = _schema

  protected var whereClause: String = _

  override def build(): Scan = new BitlapScan(schema = schema, options = options, whereClause = whereClause)

  override def pushFilters(filters: Array[Filter]): Array[Filter] = ???

  override def pushedFilters(): Array[Filter] = ???

  override def pruneColumns(requiredSchema: StructType): Unit =
    this.schema = requiredSchema
}
