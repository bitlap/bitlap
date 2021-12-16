package org.bitlap.core.mdm.fetch

import org.bitlap.common.BitlapIterator
import org.bitlap.core.BitlapContext
import org.bitlap.core.data.metadata.Table
import org.bitlap.core.mdm.FetchContext
import org.bitlap.core.mdm.Fetcher
import org.bitlap.core.mdm.model.FetchResult
import org.bitlap.core.mdm.model.Row
import org.bitlap.core.mdm.model.RowValueMeta
import org.bitlap.core.sql.Keyword
import org.bitlap.core.sql.TimeFilterFun
import org.bitlap.core.storage.store.MetricStore

/**
 * Fetch results from local jvm.
 */
class LocalFetcher(val context: FetchContext) : Fetcher {

    override fun fetchMetricsMeta(
        table: Table,
        timeFilter: TimeFilterFun,
        metrics: List<String>
    ): FetchResult {
        val metricStore = MetricStore(table, BitlapContext.hadoopConf, context.runtimeConf)
        val rows = LinkedHashMap<Long, MutableMap<String, RowValueMeta>>()
        metricStore.queryMeta(timeFilter, metrics)
            .asSequence().toList() // eager consume
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
        return FetchResult(BitlapIterator.of(flatRows), listOf(Keyword.TIME) + metrics)
    }
}
