/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.mdm

import com.github.doyaaaaaken.kotlincsv.dsl.csvReader
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
import org.bitlap.common.utils.JsonEx.jsonAsMap
import org.bitlap.common.utils.StringEx
import org.bitlap.core.catalog.metadata.Table
import org.bitlap.core.sql.QueryContext
import org.bitlap.core.storage.BitlapStore
import org.bitlap.core.storage.load.MetricDimRow
import org.bitlap.core.storage.load.MetricDimRowMeta
import org.bitlap.core.storage.load.MetricRow
import org.bitlap.core.storage.load.MetricRowMeta
import java.io.Closeable
import java.io.InputStream
import java.io.Serializable
import kotlin.system.measureTimeMillis

/**
 * bitlap mdm [Event] writer
 */
class BitlapEventWriter(val table: Table, hadoopConf: Configuration) : Serializable, Closeable {

    @Volatile
    private var closed = false
    private val log = logger { }
    private val store = BitlapStore(table, hadoopConf).open()

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
            QueryContext.use {
                it.queryId = StringEx.uuid(true)
                this.write0(events)
            }
        }
        log.info { "End writing ${events.size} events, elapsed ${elapsed}ms." }
    }

    /**
     * write mdm events from csv
     */
    fun writeCsv(input: InputStream) {
        val headers = Event.schema.map { it.first }
        val events = csvReader { skipEmptyLine = true }.open(input) {
            readAllWithHeaderAsSequence()
                .map { row -> headers.map { h -> row[h] } }
                .filter { row -> row.all { !it.isNullOrBlank() } }
                .map { row ->
                    val (time, entity, dimensions, metricName, metricValue) = row
                    Event.of(
                        time!!.toLong(),
                        Entity(entity!!.toInt()),
                        Dimension(dimensions.jsonAsMap()),
                        Metric(metricName!!, metricValue!!.toDouble())
                    )
                }
                .toList()
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
            store.storeMetric(time, metricRows)

            // store metric with one dimension
            val metricDimRows = cleanRows
                .flatMap { r ->
                    r.dimension.dimensions.map { (key, value) ->
                        r.copy(dimension = Dimension(key to value))
                    }
                }
                .groupingBy { "${it.entity.key}${it.metric.key}${it.dimension.firstPair()}" }
                .fold({ _, r ->
                    val (dk, d) = r.dimension.firstPair()
                    MetricDimRow(time, r.metric.key, dk, d, CBM(), BBM(), MetricDimRowMeta(time, r.metric.key, dk, d))
                }) { _, a, b ->
                    a.entity.add(b.dimId, b.entity.id)
                    a.metric.add(b.dimId, b.entity.id, b.metric.value.toLong()) // TODO double support
                    a
                }
                .map { (_, r) ->
                    r.metadata = MetricDimRowMeta(
                        r.tm, r.metricKey, r.dimensionKey, r.dimension,
                        r.entity.getCountUnique(), r.entity.getLongCount(), r.metric.getCount()
                    )
                    r
                }
            store.storeMetricDim(time, metricDimRows)

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
            this.store.close()
        }.onFailure {
            log.error("Error when closing BitlapWriter, cause: ", it)
        }
    }
}
