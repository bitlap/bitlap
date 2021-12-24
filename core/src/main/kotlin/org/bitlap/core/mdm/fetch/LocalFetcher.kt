package org.bitlap.core.mdm.fetch

import org.bitlap.common.BitlapIterator
import org.bitlap.common.bitmap.CBM
import org.bitlap.core.BitlapContext
import org.bitlap.core.data.metadata.Table
import org.bitlap.core.mdm.FetchContext
import org.bitlap.core.mdm.Fetcher
import org.bitlap.core.mdm.format.DataTypeCBM
import org.bitlap.core.mdm.format.DataTypeLong
import org.bitlap.core.mdm.format.DataTypeRowValueMeta
import org.bitlap.core.mdm.model.Row
import org.bitlap.core.mdm.model.RowIterator
import org.bitlap.core.mdm.model.RowValueMeta
import org.bitlap.core.sql.Keyword
import org.bitlap.core.sql.TimeFilterFun
import org.bitlap.core.storage.store.MetricStore

/**
 * Fetch results from local jvm.
 */
class LocalFetcher(val context: FetchContext) : Fetcher {

    override fun fetchMetricsMeta(table: Table, timeFilter: TimeFilterFun, metrics: List<String>): RowIterator {
        val metricStore = MetricStore(table, BitlapContext.hadoopConf, context.queryContext.runtimeConf!!)
        val rows = LinkedHashMap<Long, MutableMap<String, RowValueMeta>>()
        metricStore.queryMeta(timeFilter, metrics)
            .asSequence().toList() // TODO: eager consume? remote should be eager
            .map {
                val row = rows.computeIfAbsent(it.tm) { mutableMapOf() }
                if (row.containsKey(it.metricKey)) {
                    row[it.metricKey]!!.add(it.entityUniqueCount, it.entityCount, it.metricCount)
                } else {
                    row[it.metricKey] = RowValueMeta.of(it.entityUniqueCount, it.entityCount, it.metricCount)
                }
            }
        val flatRows = rows.map { rs ->
            arrayOfNulls<Any>(metrics.size + 1).let {
                it[0] = rs.key
                metrics.mapIndexed { i, p ->
                    it[i + 1] = rs.value[p] ?: RowValueMeta.empty()
                }
                Row(it)
            }
        }
        return RowIterator(
            BitlapIterator.of(flatRows),
            listOf(DataTypeLong(Keyword.TIME, 0)),
            metrics.mapIndexed { i, m -> DataTypeRowValueMeta(m, i + 1) }
        )
    }

    override fun fetchMetrics(table: Table, timeFilter: TimeFilterFun, metrics: List<String>): RowIterator {
        val metricStore = MetricStore(table, BitlapContext.hadoopConf, context.queryContext.runtimeConf!!)
        val rows = LinkedHashMap<Long, MutableMap<String, CBM>>()
        metricStore.query(timeFilter, metrics)
            .asSequence().toList() // eager consume
            .map {
                val row = rows.computeIfAbsent(it.tm) { mutableMapOf() }
                if (row.containsKey(it.metricKey)) {
                    row[it.metricKey]!!.or(it.metric)
                } else {
                    row[it.metricKey] = it.metric
                }
            }
        val flatRows = rows.map { rs ->
            arrayOfNulls<Any>(metrics.size + 1).let {
                it[0] = rs.key
                metrics.mapIndexed { i, p ->
                    it[i + 1] = rs.value[p] ?: CBM()
                }
                Row(it)
            }
        }
        return RowIterator(
            BitlapIterator.of(flatRows),
            listOf(DataTypeLong(Keyword.TIME, 0)),
            metrics.mapIndexed { i, m -> DataTypeCBM(m, i + 1) }
        )
    }
}
