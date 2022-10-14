/* Copyright (c) 2022 bitlap.org */
package io.bitlap.spark

import io.bitlap.spark.jdbc.JdbcClientOperator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{ BaseRelation, TableScan }
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{ Row, SQLContext }
import zio._

/** @author
 *    梦境迷离
 *  @version 1.0,2022/10/14
 */
final class DataSourceRelation(
  override val sqlContext: SQLContext,
  url: String,
  path: String,
  charSet: String,
  userSchema: StructType
) extends BaseRelation
    with TableScan
    with Serializable {

  override def schema: StructType =
    // TODO jdbc => StructType?
    // zio.Runtime.default.unsafeRun(JdbcClientOperator.getDatabaseMetaData(url)).getSchemas ?
    ???
  override def buildScan(): RDD[Row] =
    // TODO file => jdbc insert ?
    // TODO file => RDD ?
    // zio.Runtime.default.unsafeRun(JdbcClientOperator.getConnection(url)).getResultSet ?
    ???
}
