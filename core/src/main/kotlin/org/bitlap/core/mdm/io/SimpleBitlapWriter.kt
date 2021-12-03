package org.bitlap.core.mdm.io

import org.bitlap.common.bitmap.BBM
import org.bitlap.common.bitmap.CBM
import org.bitlap.common.data.Event
import org.bitlap.common.data.EventWithDimId
import org.bitlap.common.exception.BitlapException
import org.bitlap.core.BitlapContext
import org.bitlap.core.Constants.DEFAULT_DATABASE
import org.bitlap.core.storage.load.MetricRow
import org.bitlap.core.storage.load.MetricRowMeta
import java.util.Collections

/**
 * Desc: Simple data row write to bitlap
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2020/12/15
 */
class SimpleBitlapWriter(table: String, database: String = DEFAULT_DATABASE) : BitlapWriter<Event> {

    private var closed = false
    private val rows = Collections.synchronizedList(mutableListOf<Event>())
    private val metricStore = BitlapContext.catalog.getMetricStore(table, database)

    override fun write(t: Event) {
        rows.add(t)
    }

    override fun write(ts: List<Event>) {
        rows.addAll(ts)
    }

    @Synchronized
    override fun close() {
        if (closed) {
            throw BitlapException("SimpleBitlapWriter has been closed.")
        }
        closed = true
        rows.groupBy { it.time }.forEach { (time, rs) ->
            // 1. agg metric
            val singleRows = rs
                .groupingBy { "${it.entity}${it.dimension}${it.metric.key}" }
                .fold({ _, e -> EventWithDimId.from(e, false) }) { _, a, b ->
                    a.also { it.metric += b.metric.value }
                }
                .values
            // 2. identify sort id for dimensions for each entity + metric
            val cleanRows = singleRows.groupBy { "${it.entity}${it.metric.key}" }.flatMap { (_, sRows) ->
                val temps = hashMapOf<String, Int>()
                var counter = 0
                sRows.sortedByDescending { it.metric.value }.map { sRow ->
                    val dim = sRow.dimension.toString()
                    if (temps.containsKey(dim)) {
                        sRow.dimId = temps[dim]!!
                    } else {
                        sRow.dimId = counter
                        temps[dim] = counter
                        counter ++
                    }
                    sRow
                }
            }
            // store metrics
            val metricRows = cleanRows.groupingBy { "${it.entity.key}${it.metric.key}" }
                .fold({ _, r ->
                    MetricRow(time, r.metric.key, CBM(), BBM(), MetricRowMeta(time, r.metric.key))
                }) { _, a, b ->
                    a.entity.add(b.dimId, b.entity.id)
                    a.metric.add(b.dimId, b.entity.id, b.metric.value.toLong()) // TODO double support
                    a
                }
                .map { (_, r) ->
                    r.metadata = MetricRowMeta(r.tm, r.metricKey, r.entity.getCountUnique(), r.entity.getLongCount(), r.metric.getCount())
                    r
                }
            metricStore.store(time to metricRows)

            // store metric with one dimension
            // TODO

            // store metric with high cardinality dimensions
            // TODO
        }
    }
}
