package org.bitlap.core.reader

import org.bitlap.common.utils.PreConditions
import org.bitlap.core.BitlapContext
import org.bitlap.core.BitlapReader
import org.bitlap.core.model.query.AggType
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
        val dsStore = BitlapContext.dataSourceManager.getDataSourceStore(PreConditions.checkNotBlank(query.datasource))
        val metricStore = dsStore.getMetricStore()

        val shouldMaterialize = query.metrics.any { it.aggType == AggType.None || it.aggType == AggType.Distinct }
        val rows = mutableListOf<RawRow>()
        if (!shouldMaterialize) {
            val metas = metricStore.queryMeta(query.time.start, query.metrics.map { it.metricKey }, query.entity)
                .map { it.metricKey to it }
                .toMap()
            // handle metric meta data
            val metrics = query.metrics.map { metas[it.metricKey]?.metricCount ?: 0.0 }.toTypedArray()
            rows.add(RawRow(metrics))
        } else {
            val mRows = metricStore.query(query.time.start, query.metrics.map { it.metricKey }, query.entity)
                .map { it.metricKey to it }
                .toMap()
            // handle metric meta data
            val metrics = query.metrics.map { mRows[it.metricKey]?.metric ?: 0.0 }.toTypedArray()
            rows.add(RawRow(metrics))
        }
        return rows
    }

    override fun close() {
    }
}
