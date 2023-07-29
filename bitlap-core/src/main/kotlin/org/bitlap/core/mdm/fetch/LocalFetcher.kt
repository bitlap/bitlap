/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.mdm.fetch

import org.bitlap.common.BitlapIterator
import org.bitlap.common.bitmap.BBM
import org.bitlap.common.bitmap.CBM
import org.bitlap.common.bitmap.RBM
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
import org.bitlap.core.mdm.format.DataTypeString
import org.bitlap.core.mdm.format.DataTypes
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

/**
 * Fetch results from local jvm.
 */
class LocalFetcher(val context: FetchContext) : Fetcher {

    override fun fetchMetrics(
        table: Table,
        timeFilter: PruneTimeFilter,
        metrics: List<String>,
        metricType: Class<out DataType>,
    ): RowIterator {
        // TODO (add store cache)
        val store = BitlapStore(table, BitlapContext.hadoopConf).open()
        // TODO (eager consume? remote should be eager)
        val container = when (metricType) {
            DataTypeRowValueMeta::class.java -> {
                MDContainer<MetricRowMeta, RowValueMeta>(1).also { container ->
                    store.queryMeta(timeFilter, metrics)
                        .asSequence()
                        .forEach { row ->
                            container.put(
                                row.tm, row,
                                { RowValueMeta.of(it.entityUniqueCount, it.entityCount, it.metricCount) },
                                { a, b -> a.add(b.entityUniqueCount, b.entityCount, b.metricCount) }
                            )
                        }
                }
            }
            DataTypeRBM::class.java -> {
                MDContainer<MetricRow, RBM>(1).also { container ->
                    store.queryBBM(timeFilter, metrics).asSequence()
                        .forEach { row ->
                            container.put(
                                row.tm, row,
                                { it.entity.getRBM() },
                                { a, b -> a.or(b.entity.getRBM()) }
                            )
                        }
                }
            }
            DataTypeBBM::class.java -> {
                MDContainer<MetricRow, BBM>(1).also { container ->
                    store.queryBBM(timeFilter, metrics).asSequence()
                        .forEach { row ->
                            container.put(
                                row.tm, row,
                                { it.entity },
                                { a, b -> a.or(b.entity) }
                            )
                        }
                }
            }
            DataTypeCBM::class.java -> {
                MDContainer<MetricRow, CBM>(1).also { container ->
                    store.queryCBM(timeFilter, metrics).asSequence()
                        .forEach { row ->
                            container.put(
                                row.tm, row,
                                { it.metric },
                                { a, b -> a.or(b.metric) }
                            )
                        }
                }
            }
            else -> throw IllegalArgumentException("Invalid metric data type: ${metricType.name}")
        }

        // make to flat rows
        val flatRows = container.flatRows(metrics) { DataTypes.defaultValue(metricType) }
        return RowIterator(
            BitlapIterator.of(flatRows),
            listOf(DataTypeLong(Keyword.TIME, 0)),
            metrics.mapIndexed { i, m -> DataTypes.from(metricType, m, i + 1) }
        )
    }

    override fun fetchMetrics(
        table: Table,
        timeFilter: PruneTimeFilter,
        metrics: List<String>,
        metricType: Class<out DataType>,
        dimension: String,
        dimensionFilter: PrunePushedFilter,
    ): RowIterator {
        // TODO (add store cache)
        val store = BitlapStore(table, BitlapContext.hadoopConf).open()
        // TODO (eager consume? remote should be eager)
        val container = when (metricType) {
            DataTypeRowValueMeta::class.java -> {
                MDContainer<MetricDimRowMeta, RowValueMeta>(2).also { container ->
                    store.queryMeta(timeFilter, metrics, dimension, dimensionFilter)
                        .asSequence()
                        .forEach { row ->
                            container.put(
                                listOf(row.tm, row.dimension), row,
                                { RowValueMeta.of(it.entityUniqueCount, it.entityCount, it.metricCount) },
                                { a, b -> a.add(b.entityUniqueCount, b.entityCount, b.metricCount) }
                            )
                        }
                }
            }
            DataTypeRBM::class.java -> {
                MDContainer<MetricDimRow, RBM>(2).also { container ->
                    store.queryBBM(timeFilter, metrics, dimension, dimensionFilter).asSequence()
                        .forEach { row ->
                            container.put(
                                listOf(row.tm, row.dimension), row,
                                { it.entity.getRBM() },
                                { a, b -> a.or(b.entity.getRBM()) }
                            )
                        }
                }
            }
            DataTypeBBM::class.java -> {
                MDContainer<MetricDimRow, BBM>(2).also { container ->
                    store.queryBBM(timeFilter, metrics, dimension, dimensionFilter).asSequence()
                        .forEach { row ->
                            container.put(
                                listOf(row.tm, row.dimension), row,
                                { it.entity },
                                { a, b -> a.or(b.entity) }
                            )
                        }
                }
            }
            DataTypeCBM::class.java -> {
                MDContainer<MetricDimRow, CBM>(2).also { container ->
                    store.queryCBM(timeFilter, metrics, dimension, dimensionFilter).asSequence()
                        .forEach { row ->
                            container.put(
                                listOf(row.tm, row.dimension), row,
                                { it.metric },
                                { a, b -> a.or(b.metric) }
                            )
                        }
                }
            }
            else -> throw IllegalArgumentException("Invalid metric data type: ${metricType.name}")
        }
        // make to flat rows
        val flatRows = container.flatRows(metrics) { DataTypes.defaultValue(metricType) }
        return RowIterator(
            BitlapIterator.of(flatRows),
            listOf(
                DataTypeLong(Keyword.TIME, 0),
                DataTypeString(dimension, 1),
            ),
            metrics.mapIndexed { i, m -> DataTypes.from(metricType, m, i + 2) }
        )
    }
}
