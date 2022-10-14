/* Copyright (c) 2022 bitlap.org */
package io.bitlap.spark

import org.apache.spark.sql.sources.{ RelationProvider, SchemaRelationProvider }
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructType

/** @author
 *    梦境迷离
 *  @version 1.0,2022/10/14
 */
final class DefaultSource extends RelationProvider with SchemaRelationProvider {

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation =
    createRelation(sqlContext, parameters, null)

  override def createRelation(
    sqlContext: SQLContext,
    parameters: Map[String, String],
    schema: StructType
  ): BaseRelation = {
    val path    = parameters.get("path")
    val charSet = parameters.getOrElse("charSet", "UTF-8")
    path match {
      case Some(p) => new DataSourceRelation(sqlContext, p, charSet, schema)
      case _       => throw new IllegalArgumentException("Path is required for files")
    }
  }
}
