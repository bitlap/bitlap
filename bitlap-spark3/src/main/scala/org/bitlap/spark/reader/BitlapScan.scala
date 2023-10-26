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

import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

final class BitlapScan(
  private val schema: StructType,
  private val options: CaseInsensitiveStringMap,
  private val whereClause: String)
    extends Scan
    with Batch {

  private var bitlapDataSourceReadOptions: BitlapDataSourceReadOptions = _

  private val url   = options.getOrDefault("url", null)
  private val table = options.getOrDefault("dbtable", null)

  override def readSchema(): StructType = schema

  override def description: String = this.getClass.toString

  override def toBatch: Batch = this

  override def planInputPartitions(): Array[InputPartition] = {
    // BitlapDataSourceReadOptions
    // selectStatement  and statement
    // overriddenProps
    bitlapDataSourceReadOptions = new BitlapDataSourceReadOptions(
      url,
      scan = null,
      tenantId = null,
      overriddenProps = null,
      selectStatement = options.getOrDefault("sql", null)
    )
    Array(
      new BitlapInputPartition(
        new BitlapInputSplit
      )
    )
  }

  override def createReaderFactory(): PartitionReaderFactory =
    new BitlapPartitionReaderFactory(schema, bitlapDataSourceReadOptions)
}
