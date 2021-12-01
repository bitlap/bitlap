package org.bitlap.core.mdm.fetch

import org.bitlap.common.BitlapIterator
import org.bitlap.core.BitlapContext
import org.bitlap.core.data.metadata.Table
import org.bitlap.core.mdm.FetchContext
import org.bitlap.core.mdm.Fetcher
import org.bitlap.core.sql.TimeFilterFun
import org.bitlap.core.storage.load.MetricRowMeta
import org.bitlap.core.storage.store.MetricStore

/**
 * Fetch results from local jvm.
 */
class LocalFetcher(val context: FetchContext) : Fetcher {

    override fun fetchMetricsMeta(
        table: Table,
        timeFilter: TimeFilterFun,
        metrics: List<String>
    ): BitlapIterator<MetricRowMeta> {
        val metricStore = MetricStore(table, BitlapContext.hadoopConf, context.runtimeConf)
        val rows = metricStore.queryMeta(timeFilter, metrics).asSequence().toList() // eager consume
        return BitlapIterator.of(rows)
    }
}
