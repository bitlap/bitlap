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
package org.bitlap.core.mdm

import org.bitlap.core.catalog.metadata.Table
import org.bitlap.core.mdm.format.DataType
import org.bitlap.core.mdm.model.RowIterator
import org.bitlap.core.sql.PrunePushedFilter
import org.bitlap.core.sql.PruneTimeFilter

/** Fetch core data from local storage system or remote.
 */
trait Fetcher {

  def fetchMetrics(
    table: Table,
    timeFilter: PruneTimeFilter,
    metrics: List[String],
    metricType: Class[_ <: DataType]
  ): RowIterator

  def fetchMetrics(
    table: Table,
    timeFilter: PruneTimeFilter,
    metrics: List[String],
    metricType: Class[_ <: DataType],
    dimension: String,
    dimensionFilter: PrunePushedFilter
  ): RowIterator
}
