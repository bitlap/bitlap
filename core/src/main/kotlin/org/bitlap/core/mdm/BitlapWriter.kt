package org.bitlap.core.mdm

import cn.hutool.core.convert.Convert
import org.apache.hadoop.conf.Configuration
import org.bitlap.common.bitmap.BBM
import org.bitlap.common.bitmap.CBM
import org.bitlap.common.data.Dimension
import org.bitlap.common.data.Entity
import org.bitlap.common.data.Event
import org.bitlap.common.data.EventWithDimId
import org.bitlap.common.data.Metric
import org.bitlap.common.exception.BitlapException
import org.bitlap.common.logger
import org.bitlap.common.utils.JSONUtils
import org.bitlap.common.utils.PreConditions
import org.bitlap.core.data.metadata.Table
import org.bitlap.core.storage.StoreType
import org.bitlap.core.storage.load.MetricRow
import org.bitlap.core.storage.load.MetricRowMeta
import org.bitlap.core.utils.Excel.readCsv
import org.bitlap.core.utils.Excel.readExcel
import java.io.Closeable
import java.io.InputStream
import java.io.Serializable
import kotlin.system.measureTimeMillis

/**
 * bitmap mdm [Event] writer
 */
class BitlapWriter(val table: Table, hadoopConf: Configuration) : Serializable, Closeable {

    @Volatile
    private var closed = false
    private val log = logger { }
    private val metricStore = StoreType.valueOf(table.props[Table.TABLE_FORMAT_KEY]!!)
        .getProvider(table, hadoopConf)
        .getMetricStore()

    /**
     * write mdm events
     */
    fun write(events: List<Event>) {
        this.checkOpen()
        log.info { "Start writing ${events.size} events." }
        if (events.isEmpty()) {
            return
        }
        val elapsed = measureTimeMillis {
            this.write0(events)
        }
        log.info { "End writing ${events.size} events, elapsed ${elapsed}ms." }
    }

    /**
     * write mdm events from csv
     */
    fun writeCsv(path: String) = this.writeExcel0(path.readCsv())
    fun writeCsv(input: InputStream) = this.writeExcel0(input.readCsv())

    /**
     * write mdm events from excel
     */
    fun writeExcel(path: String) = this.writeExcel0(path.readExcel())
    fun writeExcel(input: InputStream) = this.writeExcel0(input.readExcel())

    private fun writeExcel0(excel: Pair<List<String>, List<List<Any?>>>) {
        val (header, rows) = excel
        PreConditions.checkExpression(
            header == listOf("time", "entity", "dimensions", "metric_name", "metric_value"),
            msg = "Header $header is invalid."
        )
        val events = rows.map { row ->
            val (time, entity, dimensions, metricName, metricValue) = row
            Event.of(
                Convert.toLong(time),
                Entity(Convert.toInt(entity)),
                Dimension(JSONUtils.fromJsonAsMap(Convert.toStr(dimensions))),
                Metric(Convert.toStr(metricName), Convert.toDouble(metricValue))
            )
        }
        this.write(events)
    }

    private fun write0(events: List<Event>) {
        events.groupBy { it.time }.forEach { (time, rs) ->
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
                        counter++
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
                    r.metadata = MetricRowMeta(
                        r.tm, r.metricKey,
                        r.entity.getCountUnique(), r.entity.getLongCount(), r.metric.getCount()
                    )
                    r
                }
            metricStore.store(time, metricRows)

            // store metric with one dimension
            // TODO

            // store metric with high cardinality dimensions
            // TODO
        }
    }

    private fun checkOpen() {
        if (closed) {
            throw BitlapException("BitlapWriter has been closed.")
        }
    }

    override fun close() {
        closed = true
        runCatching {
            this.metricStore.close()
        }.onFailure {
            log.error("Error when closing BitlapWriter, cause: ", it)
        }
    }
}
