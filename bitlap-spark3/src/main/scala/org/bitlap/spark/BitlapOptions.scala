/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.spark

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions

/** spark datasource options for bitlap
 */
class BitlapOptions(val params: Map[String, String]) extends JDBCOptions(params) {

  val tableIdentifier: TableIdentifier = SparkUtils.getHiveTableIdentifier(tableOrQuery)
  val dimensionMaxId: Int              = params.getOrElse("dimensionMaxId", "2048").toInt
}
