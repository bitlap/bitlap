package org.bitlap.core.mdm.io

import org.bitlap.common.utils.PreConditions
import org.bitlap.core.BitlapContext
import org.bitlap.core.mdm.model.AggType
import org.bitlap.core.mdm.model.Query
import org.bitlap.core.mdm.model.RawRow

/**
 * Desc: Default bitlap reader
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/3/17
 */
class DefaultBitlapReader : BitlapReader {

    override fun read(query: Query): List<RawRow> {
        val metricStore = BitlapContext.catalog.getMetricStore(PreConditions.checkNotBlank(query.table))

        if (query.hasDimensions()) {
            // TODO: with dimensions
            return emptyList()
        }
        val metaColumns = mutableListOf<String>().apply {
            addAll(query.dimensions)
            addAll(query.metrics.map { it.name })
        }
        val shouldMaterialize = query.metrics.any { it.aggType == AggType.None || it.aggType == AggType.Distinct }
        val rows = mutableListOf<RawRow>()
        val time = query.time.timeRange
        if (shouldMaterialize) {
            val mRows = metricStore.query(time, query.metrics.map { it.name })
                .asSequence().map { it.metricKey to it }.toMap()
            // handle metric meta data
            val metrics = query.metrics.map { mRows[it.name]?.metric ?: 0.0 }.toTypedArray()
            rows.add(RawRow(metrics, metaColumns))
        } else {
            val metas = metricStore.queryMeta(time, query.metrics.map { it.name })
                .asSequence().map { it.metricKey to it }.toMap()
            // handle metric meta data
            val metrics = query.metrics.map { metas[it.name]?.metricCount ?: 0.0 }.toTypedArray()
            rows.add(RawRow(metrics, metaColumns))
        }
        return rows
    }

    override fun close() {
    }
}
