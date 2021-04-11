package org.bitlap.core.reader

import org.bitlap.common.utils.PreConditions
import org.bitlap.core.BitlapReader
import org.bitlap.core.DataSourceManager
import org.bitlap.core.model.query.Query
import org.bitlap.core.model.query.RawRow

/**
 * Desc: Default bitlap reader
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/3/17
 */
class DefaultBitlapReader : BitlapReader {

    override fun read(query: Query): List<RawRow> {
        val dsStore = DataSourceManager.getDataSourceStore(PreConditions.checkNotBlank(query.datasource))
        val metricStore = dsStore.getMetricStore()
        val metas = metricStore.queryMeta(query.time.start, query.metric.map { it.metricKey }, query.entity)

        val rows = mutableListOf<RawRow>()
        // handle metric data
        val metrics = mutableMapOf<String, Double>()
        metas.map {
            metrics.computeIfAbsent(it.metricKey) { _ -> it.metricCount }
        }
        rows.add(RawRow(metrics))

        return rows
    }

    override fun close() {
    }
}
