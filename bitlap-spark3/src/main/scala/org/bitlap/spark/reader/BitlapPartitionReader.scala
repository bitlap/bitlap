/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.spark.reader

import java.io.IOException
import java.sql._

import org.bitlap.spark.util.SparkJdbcUtil

import org.apache.spark.executor.InputMetrics
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types.StructType

/** @since 2022/10/16
 *  @author
 *    梦境迷离
 */
final class BitlapPartitionReader(
  options: BitlapDataSourceReadOptions,
  schema: StructType,
  bitlapPartition: BitlapInputPartition)
    extends PartitionReader[InternalRow] {

  private var currentRow: InternalRow         = InternalRow.empty
  private var iterator: Iterator[InternalRow] = Iterator.empty
  private var resultSet: ResultSet            = _

  initialize()

  // TODO  initialize

  def initialize(): Unit = {
    val stmt = DriverManager.getConnection(options.url).createStatement()
    stmt.execute(options.selectStatement)
    resultSet = stmt.getResultSet
    val im = classOf[InputMetrics].getConstructor().newInstance()
    iterator = SparkJdbcUtil.resultSetToSparkInternalRows(resultSet, schema, im)
  }

  override def next(): Boolean =
    if (!iterator.hasNext) {
      false
    } else {
      currentRow = iterator.next()
      true
    }

  override def get(): InternalRow = currentRow

  override def close(): Unit =
    if (resultSet != null) {
      try
        resultSet.close()
      catch {
        case e: SQLException =>
          throw new IOException(e)
      }
    }

}
