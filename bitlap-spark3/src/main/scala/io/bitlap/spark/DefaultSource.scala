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
    val charSet = parameters.getOrElse("charSet", "UTF-8")
    val url     = parameters.get("url")
    val path    = parameters.get("path")
    (path, url) match {
      case (Some(pt), Some(ul)) => new DataSourceRelation(sqlContext, ul, pt, charSet, schema)
      case _                    => throw new IllegalArgumentException("Both path and url are required!")
    }
  }
}
