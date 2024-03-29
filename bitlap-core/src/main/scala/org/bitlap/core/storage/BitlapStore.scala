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
package org.bitlap.core.storage

import java.io.Closeable

import org.bitlap.common.BitlapIterator
import org.bitlap.common.exception.BitlapException
import org.bitlap.common.utils.DateEx._
import org.bitlap.core.catalog.metadata.Table
import org.bitlap.core.sql.Keyword
import org.bitlap.core.sql.PrunePushedFilter
import org.bitlap.core.sql.PruneTimeFilter
import org.bitlap.core.storage.load.MetricDimRow
import org.bitlap.core.storage.load.MetricDimRowMeta
import org.bitlap.core.storage.load.MetricRow
import org.bitlap.core.storage.load.MetricRowMeta

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

/** Bitlap store implementation.
 */
class BitlapStore(val table: Table, val hadoopConf: Configuration) extends Closeable {

  private val tablePath = Path(table.path)

  private val fs: FileSystem = {
    val _fs = tablePath.getFileSystem(hadoopConf)
    _fs.setWriteChecksum(false)
    _fs.setVerifyChecksum(false)
    _fs
  }
  private val tableFormatProvider = TableFormat.fromTable(table).getProvider(table, fs)
  // metric
  private val metricDataPath      = Path(tablePath, "m")
  private val readMetricBatchSize = 100

  // metric dimension
  private val metricDimDataPath      = Path(tablePath, "md")
  private val readMetricDimBatchSize = 1000

  def open(): BitlapStore = {
    if (!fs.exists(tablePath)) {
      throw BitlapException(s"Unable to open $table metric store: $tablePath, table path does not exist.")
    }
    if (!fs.exists(metricDataPath)) {
      fs.mkdirs(metricDataPath)
    }
    if (!fs.exists(metricDimDataPath)) {
      fs.mkdirs(metricDimDataPath)
    }
    this
  }

  /** Store metric [rows] with time [tm] as provided data file format.
   */
  def storeMetric(tm: Long, rows: List[MetricRow]): Unit = {
    if (rows.isEmpty) {
      return
    }
    // get output path
    val date   = tm.utc
    val output = Path(metricDataPath, s"${Keyword.TIME}=${date.getMillis}")

    // write rows in one batch
    val sortRows = rows.sortBy { it => s"${it.metricKey}${it.tm}" }
    val writer   = tableFormatProvider.getMetricWriter(output)
    scala.util.Using.resource(writer) { w =>
      w.writeBatch(sortRows)
    }
  }

  /** Store metric dimension [rows] with time [tm] as provided data file format.
   */
  def storeMetricDim(tm: Long, rows: List[MetricDimRow]): Unit = {
    if (rows.isEmpty) {
      return
    }
    // get output path
    val date   = tm.utc
    val output = Path(metricDimDataPath, s"${Keyword.TIME}=${date.getMillis}")

    // write rows in one batch
    val sortRows = rows.sortBy { it => s"${it.metricKey}${it.dimensionKey}${it.dimension}${it.tm}" }
    val provider = TableFormat.fromTable(table).getProvider(table, fs)
    val writer   = provider.getMetricDimWriter(output)
    scala.util.Using.resource(writer) { w =>
      w.writeBatch(sortRows)
    }
  }

  /** query metric rows
   */
  def queryMeta(timeFilter: PruneTimeFilter, metrics: List[String]): BitlapIterator[MetricRowMeta] = {
    val reader = this.tableFormatProvider.getMetricMetaReader(
      metricDataPath,
      timeFilter.mergeCondition(),
      metrics,
      List("mk", "t", "meta")
    )
    BitlapReaderIterator(reader, readMetricBatchSize)
  }

  def queryBBM(timeFilter: PruneTimeFilter, metrics: List[String]): BitlapIterator[MetricRow] = {
    val reader = this.tableFormatProvider.getMetricReader(
      metricDataPath,
      timeFilter.mergeCondition(),
      metrics,
      List("mk", "t", "e")
    )
    BitlapReaderIterator(reader, readMetricBatchSize)
  }

  def queryCBM(timeFilter: PruneTimeFilter, metrics: List[String]): BitlapIterator[MetricRow] = {
    val reader = this.tableFormatProvider.getMetricReader(
      metricDataPath,
      timeFilter.mergeCondition(),
      metrics,
      List("mk", "t", "m")
    )
    BitlapReaderIterator(reader, readMetricBatchSize)
  }

  /** query metric dimension rows
   */
  def queryMeta(
    timeFilter: PruneTimeFilter,
    metrics: List[String],
    dimension: String,
    dimensionFilter: PrunePushedFilter
  ): BitlapIterator[MetricDimRowMeta] = {
    val reader = this.tableFormatProvider.getMetricDimMetaReader(
      metricDimDataPath,
      timeFilter.mergeCondition(),
      metrics,
      List("mk", "dk", "d", "t", "meta"),
      dimension,
      dimensionFilter
    )
    BitlapReaderIterator(reader, readMetricDimBatchSize)
  }

  def queryBBM(
    timeFilter: PruneTimeFilter,
    metrics: List[String],
    dimension: String,
    dimensionFilter: PrunePushedFilter
  ): BitlapIterator[MetricDimRow] = {
    val reader = this.tableFormatProvider.getMetricDimReader(
      metricDimDataPath,
      timeFilter.mergeCondition(),
      metrics,
      List("mk", "dk", "d", "t", "e"),
      dimension,
      dimensionFilter
    )
    BitlapReaderIterator(reader, readMetricDimBatchSize)
  }

  def queryCBM(
    timeFilter: PruneTimeFilter,
    metrics: List[String],
    dimension: String,
    dimensionFilter: PrunePushedFilter
  ): BitlapIterator[MetricDimRow] = {
    val reader = this.tableFormatProvider.getMetricDimReader(
      metricDimDataPath,
      timeFilter.mergeCondition(),
      metrics,
      List("mk", "dk", "d", "t", "m"),
      dimension,
      dimensionFilter
    )
    BitlapReaderIterator(reader, readMetricDimBatchSize)
  }

  override def close(): Unit = {}
}
