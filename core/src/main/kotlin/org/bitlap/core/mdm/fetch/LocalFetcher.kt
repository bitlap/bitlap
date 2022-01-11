package org.bitlap.core.mdm.fetch

import org.bitlap.common.BitlapIterator
import org.bitlap.common.bitmap.BBM
import org.bitlap.common.bitmap.CBM
import org.bitlap.common.bitmap.RBM
import org.bitlap.core.BitlapContext
import org.bitlap.core.data.metadata.Table
import org.bitlap.core.mdm.FetchContext
import org.bitlap.core.mdm.Fetcher
import org.bitlap.core.mdm.format.DataType
import org.bitlap.core.mdm.format.DataTypeBBM
import org.bitlap.core.mdm.format.DataTypeCBM
import org.bitlap.core.mdm.format.DataTypeLong
import org.bitlap.core.mdm.format.DataTypeRBM
import org.bitlap.core.mdm.format.DataTypeRowValueMeta
import org.bitlap.core.mdm.format.DataTypes
import org.bitlap.core.mdm.model.Row
import org.bitlap.core.mdm.model.RowIterator
import org.bitlap.core.mdm.model.RowValueMeta
import org.bitlap.core.sql.Keyword
import org.bitlap.core.sql.TimeFilterFun

/**
 * Fetch results from local jvm.
 */
class LocalFetcher(val context: FetchContext) : Fetcher {

    override fun fetchMetrics(
        table: Table,
        timeFilter: TimeFilterFun,
        metrics: List<String>,
        metricType: Class<out DataType>,
    ): RowIterator {
        val storeProvider = table.getTableFormat().getProvider(table, BitlapContext.hadoopConf)
        val metricStore = storeProvider.getMetricStore()
        // TODO: eager consume? remote should be eager
        val rows = when (metricType) {
            DataTypeRowValueMeta::class.java -> {
                LinkedHashMap<Long, MutableMap<String, RowValueMeta>>().also { rows ->
                    metricStore.queryMeta(timeFilter, metrics)
                        .asSequence()
                        .forEach {
                            val row = rows.computeIfAbsent(it.tm) { mutableMapOf() }
                            if (row.containsKey(it.metricKey)) {
                                row[it.metricKey]!!.add(it.entityUniqueCount, it.entityCount, it.metricCount)
                            } else {
                                row[it.metricKey] =
                                    RowValueMeta.of(it.entityUniqueCount, it.entityCount, it.metricCount)
                            }
                        }
                }
            }
            DataTypeRBM::class.java -> {
                LinkedHashMap<Long, MutableMap<String, RBM>>().also { rows ->
                    metricStore.queryBBM(timeFilter, metrics)
                        .asSequence()
                        .forEach {
                            val row = rows.computeIfAbsent(it.tm) { mutableMapOf() }
                            if (row.containsKey(it.metricKey)) {
                                row[it.metricKey]!!.or(it.entity.getRBM())
                            } else {
                                row[it.metricKey] = it.entity.getRBM()
                            }
                        }
                }
            }
            DataTypeBBM::class.java -> {
                LinkedHashMap<Long, MutableMap<String, BBM>>().also { rows ->
                    metricStore.queryBBM(timeFilter, metrics)
                        .asSequence()
                        .forEach {
                            val row = rows.computeIfAbsent(it.tm) { mutableMapOf() }
                            if (row.containsKey(it.metricKey)) {
                                row[it.metricKey]!!.or(it.entity)
                            } else {
                                row[it.metricKey] = it.entity
                            }
                        }
                }
            }
            DataTypeCBM::class.java -> {
                LinkedHashMap<Long, MutableMap<String, CBM>>().also { rows ->
                    metricStore.queryCBM(timeFilter, metrics)
                        .asSequence()
                        .forEach {
                            val row = rows.computeIfAbsent(it.tm) { mutableMapOf() }
                            if (row.containsKey(it.metricKey)) {
                                row[it.metricKey]!!.or(it.metric)
                            } else {
                                row[it.metricKey] = it.metric
                            }
                        }
                }
            }
            else -> throw IllegalArgumentException("Invalid metric data type: ${metricType.name}")
        }

        // make to flat rows
        val flatRows = rows.map { rs ->
            arrayOfNulls<Any>(metrics.size + 1).let {
                it[0] = rs.key
                metrics.mapIndexed { i, p ->
                    it[i + 1] = rs.value[p] ?: DataTypes.defaultValue(metricType)
                }
                Row(it)
            }
        }
        return RowIterator(
            BitlapIterator.of(flatRows),
            listOf(DataTypeLong(Keyword.TIME, 0)),
            metrics.mapIndexed { i, m -> DataTypes.from(metricType, m, i + 1) }
        )
    }
}
