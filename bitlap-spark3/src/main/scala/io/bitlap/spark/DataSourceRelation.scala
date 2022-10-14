/* Copyright (c) 2022 bitlap.org */
package io.bitlap.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{ BaseRelation, TableScan }
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{ Row, SQLContext }

/** @author
 *    梦境迷离
 *  @version 1.0,2022/10/14
 */
final class DataSourceRelation(
  override val sqlContext: SQLContext,
  path: String,
  charSet: String,
  userSchema: StructType
) extends BaseRelation
    with TableScan
    with Serializable {

  override def schema: StructType    = ??? // call client
  override def buildScan(): RDD[Row] = ??? // call client
}
