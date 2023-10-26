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
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

final class BitlapScanBuilder(private val _schema: StructType, val options: CaseInsensitiveStringMap)
    extends ScanBuilder
    with SupportsPushDownFilters
    with SupportsPushDownRequiredColumns {

  private var schema: StructType = _schema

  protected var whereClause: String = _

  private val pushedFilterList = Array[Filter]()

  override def build(): Scan = new BitlapScan(schema = schema, options = options, whereClause = whereClause)

  override def pushFilters(filters: Array[Filter]): Array[Filter] =
    Array.empty

  override def pushedFilters(): Array[Filter] = pushedFilterList

  override def pruneColumns(requiredSchema: StructType): Unit =
    this.schema = requiredSchema
}
