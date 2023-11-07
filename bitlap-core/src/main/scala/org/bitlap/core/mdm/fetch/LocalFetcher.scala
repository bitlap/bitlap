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
package org.bitlap.core.mdm.fetch

import org.bitlap.common.BitlapIterator
import org.bitlap.common.extension._
import org.bitlap.core.BitlapContext
import org.bitlap.core.catalog.metadata.Table
import org.bitlap.core.mdm.FetchContext
import org.bitlap.core.mdm.Fetcher
import org.bitlap.core.mdm.MDContainer
import org.bitlap.core.mdm.format.DataType
import org.bitlap.core.mdm.format.DataTypeBBM
import org.bitlap.core.mdm.format.DataTypeCBM
import org.bitlap.core.mdm.format.DataTypeLong
import org.bitlap.core.mdm.format.DataTypeRBM
import org.bitlap.core.mdm.format.DataTypeRowValueMeta
import org.bitlap.core.mdm.format.DataTypes
import org.bitlap.core.mdm.format.DataTypeString
import org.bitlap.core.mdm.model.RowIterator
import org.bitlap.core.mdm.model.RowValueMeta
import org.bitlap.core.sql.Keyword
import org.bitlap.core.sql.PrunePushedFilter
import org.bitlap.core.sql.PruneTimeFilter
import org.bitlap.core.storage.BitlapStore
import org.bitlap.core.storage.load.MetricDimRow
import org.bitlap.core.storage.load.MetricDimRowMeta
import org.bitlap.core.storage.load.MetricRow
import org.bitlap.core.storage.load.MetricRowMeta
import org.bitlap.roaringbitmap.x.BBM
import org.bitlap.roaringbitmap.x.CBM
import org.bitlap.roaringbitmap.x.RBM

/** Fetch results from local jvm.
 */
class LocalFetcher(val context: FetchContext) extends Fetcher {

  override def fetchMetrics(
    table: Table,
    timeFilter: PruneTimeFilter,
    metrics: List[String],
    metricType: Class[_ <: DataType]
  ): RowIterator = {
    // TODO (add store cache)
    val store = new BitlapStore(table, BitlapContext.hadoopConf).open()
    // TODO (eager consume? remote should be eager)
    val container = metricType match {
      case c if c == classOf[DataTypeRowValueMeta] =>
        MDContainer[MetricRowMeta, RowValueMeta](1).also { container =>
          store.queryMeta(timeFilter, metrics).foreach { row =>
            container.put(
              row.tm,
              row,
              { it => RowValueMeta.of(it.entityUniqueCount, it.entityCount, it.metricCount) },
              { (a, b) => a.add(b.entityUniqueCount, b.entityCount, b.metricCount) }
            )
          }
        }
      case c if c == classOf[DataTypeRBM] =>
        MDContainer[MetricRow, RBM](1).also { container =>
          store.queryBBM(timeFilter, metrics).foreach { row =>
            container.put(
              row.tm,
              row,
              { it => it.entity.getRBM },
              { (a, b) => a.or(b.entity.getRBM) }
            )
          }
        }
      case c if c == classOf[DataTypeBBM] =>
        MDContainer[MetricRow, BBM](1).also { container =>
          store.queryBBM(timeFilter, metrics).foreach { row =>
            container.put(
              row.tm,
              row,
              { it => it.entity },
              { (a, b) => a.or(b.entity) }
            )
          }
        }
      case c if c == classOf[DataTypeCBM] =>
        MDContainer[MetricRow, CBM](1).also { container =>
          store.queryCBM(timeFilter, metrics).foreach { row =>
            container.put(
              row.tm,
              row,
              { it => it.metric },
              { (a, b) => a.or(b.metric) }
            )
          }
        }
      case _ => throw IllegalArgumentException(s"Invalid metric data type: ${metricType.getName}")
    }

    // make to flat rows
    val flatRows = container.flatRows(metrics) { () => DataTypes.defaultValue(metricType) }
    RowIterator(
      BitlapIterator.of(flatRows),
      List(DataTypeLong(Keyword.TIME, 0)),
      metrics.zipWithIndex.map { case (m, i) => DataTypes.from(metricType, m, i + 1) }
    )
  }

  override def fetchMetrics(
    table: Table,
    timeFilter: PruneTimeFilter,
    metrics: List[String],
    metricType: Class[_ <: DataType],
    dimension: String,
    dimensionFilter: PrunePushedFilter
  ): RowIterator = {
    // TODO (add store cache)
    val store = new BitlapStore(table, BitlapContext.hadoopConf).open()
    // TODO (eager consume? remote should be eager)
    val container = metricType match {
      case c if c == classOf[DataTypeRowValueMeta] =>
        MDContainer[MetricDimRowMeta, RowValueMeta](2).also { container =>
          store.queryMeta(timeFilter, metrics, dimension, dimensionFilter).foreach { row =>
            container.put(
              List(row.tm, row.dimension),
              row,
              { it => RowValueMeta.of(it.entityUniqueCount, it.entityCount, it.metricCount) },
              { (a, b) => a.add(b.entityUniqueCount, b.entityCount, b.metricCount) }
            )
          }
        }
      case c if c == classOf[DataTypeRBM] =>
        MDContainer[MetricDimRow, RBM](2).also { container =>
          store.queryBBM(timeFilter, metrics, dimension, dimensionFilter).foreach { row =>
            container.put(
              List(row.tm, row.dimension),
              row,
              { it => it.entity.getRBM },
              { (a, b) => a.or(b.entity.getRBM) }
            )
          }
        }
      case c if c == classOf[DataTypeBBM] =>
        MDContainer[MetricDimRow, BBM](2).also { container =>
          store.queryBBM(timeFilter, metrics, dimension, dimensionFilter).foreach { row =>
            container.put(
              List(row.tm, row.dimension),
              row,
              { it => it.entity },
              { (a, b) => a.or(b.entity) }
            )
          }
        }
      case c if c == classOf[DataTypeCBM] =>
        MDContainer[MetricDimRow, CBM](2).also { container =>
          store.queryCBM(timeFilter, metrics, dimension, dimensionFilter).foreach { row =>
            container.put(
              List(row.tm, row.dimension),
              row,
              { it => it.metric },
              { (a, b) => a.or(b.metric) }
            )
          }
        }
      case _ => throw IllegalArgumentException(s"Invalid metric data type: ${metricType.getName}")
    }
    // make to flat rows
    val flatRows = container.flatRows(metrics) { () => DataTypes.defaultValue(metricType) }
    RowIterator(
      BitlapIterator.of(flatRows),
      List(
        DataTypeLong(Keyword.TIME, 0),
        DataTypeString(dimension, 1)
      ),
      metrics.zipWithIndex.map { case (m, i) => DataTypes.from(metricType, m, i + 2) }
    )
  }
}
