package org.bitlap.core.writer

import org.bitlap.common.bitmap.BBM
import org.bitlap.common.bitmap.CBM
import org.bitlap.common.exception.BitlapException
import org.bitlap.core.BitlapContext
import org.bitlap.core.BitlapWriter
import org.bitlap.core.model.SimpleRow
import org.bitlap.core.model.SimpleRowSingle
import org.bitlap.storage.metadata.MetricRow
import org.bitlap.storage.metadata.MetricRowMeta
import org.bitlap.storage.metadata.MetricRows
import java.util.Collections

/**
 * Desc: Simple data row write to bitlap
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2020/12/15
 */
class SimpleBitlapWriter(datasource: String) : BitlapWriter<SimpleRow> {

    private var closed = false
    private val rows = Collections.synchronizedList(mutableListOf<SimpleRow>())
    private val dataSourceStore = BitlapContext.dataSourceManager.getDataSourceStore(datasource)

    override fun write(t: SimpleRow) {
        rows.add(t)
    }

    override fun write(ts: List<SimpleRow>) {
        rows.addAll(ts)
    }

    @Synchronized
    override fun close() {
        if (closed) {
            throw BitlapException("SimpleBitlapWriter has been closed.")
        }
        closed = true
        val metricStore = this.dataSourceStore.getMetricStore()
        rows.groupBy { it.time }.forEach { (time, rs) ->
            // 1. agg metric
            val singleRows = rs.flatMap { it.toSingleRows() }
                .groupingBy { "${it.entityKey}${it.entity}${it.dimension}${it.metricKey}" }
                .reduce { _, a, b ->
                    SimpleRowSingle(a.time, a.entityKey, a.entity, a.dimension, a.metricKey, a.metric + b.metric)
                }
                .values
            // 2. identify bucket id for dimensions for each entity + metric
            val cleanRows = singleRows.groupBy { "${it.entityKey}${it.entity}${it.metricKey}" }.flatMap { (_, sRows) ->
                val temps = hashMapOf<String, Int>()
                var counter = 0
                sRows.sortedByDescending { it.metric }.map { sRow ->
                    val dim = sRow.dimension.toString()
                    if (temps.containsKey(dim)) {
                        sRow.bucket = temps[dim]!!
                    } else {
                        sRow.bucket = counter
                        temps[dim] = counter
                        counter ++
                    }
                    sRow
                }
            }
            // store metrics
            val metricRows = cleanRows.groupingBy { "${it.entityKey}${it.metricKey}" }
                .fold({ _, r ->
                    MetricRow(time, r.metricKey, r.entityKey, CBM(), BBM(), MetricRowMeta(time, r.metricKey, r.entityKey))
                }) { _, a, b ->
                    a.entity.add(b.bucket, b.entity)
                    a.metric.add(b.bucket, b.entity, b.metric.toLong()) // TODO double support
                    a
                }
                .map { (_, r) ->
                    r.metadata = MetricRowMeta(r.tm, r.metricKey, r.entityKey, r.entity.getCountUnique(), r.entity.getLongCount(), r.metric.getCount())
                    r
                }
            metricStore.store(MetricRows(time, metricRows))

            // store metric with one dimension
            // TODO

            // store metric with high cardinality dimensions
            // TODO
        }
    }
}
