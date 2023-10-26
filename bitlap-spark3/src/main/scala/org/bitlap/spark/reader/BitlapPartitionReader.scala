/*
 * Copyright 2020-2023 IceMimosa, jxnu-liguobin and the Bitlap Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bitlap.spark.reader

import java.io.IOException
import java.sql._

import org.bitlap.spark.util.SparkJdbcUtil

import org.apache.spark.executor.InputMetrics
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types.StructType

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
