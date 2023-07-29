/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core.storage

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.bitlap.common.BitlapIterator
import org.bitlap.common.exception.BitlapException
import org.bitlap.common.utils.DateEx.utc
import org.bitlap.core.catalog.metadata.Table
import org.bitlap.core.sql.Keyword
import org.bitlap.core.sql.PrunePushedFilter
import org.bitlap.core.sql.PruneTimeFilter
import org.bitlap.core.storage.load.MetricDimRow
import org.bitlap.core.storage.load.MetricDimRowMeta
import org.bitlap.core.storage.load.MetricRow
import org.bitlap.core.storage.load.MetricRowMeta
import java.io.Closeable

/**
 * Bitlap store implementation.
 */
class BitlapStore(val table: Table, val hadoopConf: Configuration) : Closeable {

    private val tablePath = Path(table.path)
    private var fs: FileSystem = tablePath.getFileSystem(hadoopConf).also {
        it.setWriteChecksum(false)
        it.setVerifyChecksum(false)
    }
    private val tableFormatProvider = TableFormat.fromTable(table).getProvider(table, fs)
    // metric
    private val metricDataPath = Path(tablePath, "m")
    private val readMetricBatchSize = 100

    // metric dimension
    private val metricDimDataPath = Path(tablePath, "md")
    private val readMetricDimBatchSize = 1000

    fun open(): BitlapStore {
        if (!fs.exists(tablePath)) {
            throw BitlapException("Unable to open $table metric store: $tablePath, table path does not exist.")
        }
        if (!fs.exists(metricDataPath)) {
            fs.mkdirs(metricDataPath)
        }
        if (!fs.exists(metricDimDataPath)) {
            fs.mkdirs(metricDimDataPath)
        }
        return this
    }

    /**
     * Store metric [rows] with time [tm] as provided data file format.
     */
    fun storeMetric(tm: Long, rows: List<MetricRow>) {
        if (rows.isEmpty()) {
            return
        }
        // get output path
        val date = tm.utc()
        val output = Path(metricDataPath, "${Keyword.TIME}=${date.millis}")

        // write rows in one batch
        val sortRows = rows.sortedBy { "${it.metricKey}${it.tm}" }
        val writer = tableFormatProvider.getMetricWriter(output)
        writer.use {
            it.writeBatch(sortRows)
        }
    }

    /**
     * Store metric dimension [rows] with time [tm] as provided data file format.
     */
    fun storeMetricDim(tm: Long, rows: List<MetricDimRow>) {
        if (rows.isEmpty()) {
            return
        }
        // get output path
        val date = tm.utc()
        val output = Path(metricDimDataPath, "${Keyword.TIME}=${date.millis}")

        // write rows in one batch
        val sortRows = rows.sortedBy { "${it.metricKey}${it.dimensionKey}${it.dimension}${it.tm}" }
        val provider = TableFormat.fromTable(table).getProvider(table, fs)
        val writer = provider.getMetricDimWriter(output)
        writer.use {
            it.writeBatch(sortRows)
        }
    }

    /**
     * query metric rows
     */
    fun queryMeta(timeFilter: PruneTimeFilter, metrics: List<String>): BitlapIterator<MetricRowMeta> {
        val reader = this.tableFormatProvider.getMetricMetaReader(
            metricDataPath,
            timeFilter.mergeCondition(),
            metrics,
            listOf("mk", "t", "meta")
        )
        return BitlapReaderIterator(reader, readMetricBatchSize)
    }

    fun queryBBM(timeFilter: PruneTimeFilter, metrics: List<String>): BitlapIterator<MetricRow> {
        val reader = this.tableFormatProvider.getMetricReader(
            metricDataPath,
            timeFilter.mergeCondition(),
            metrics,
            listOf("mk", "t", "e")
        )
        return BitlapReaderIterator(reader, readMetricBatchSize)
    }

    fun queryCBM(timeFilter: PruneTimeFilter, metrics: List<String>): BitlapIterator<MetricRow> {
        val reader = this.tableFormatProvider.getMetricReader(
            metricDataPath,
            timeFilter.mergeCondition(),
            metrics,
            listOf("mk", "t", "m")
        )
        return BitlapReaderIterator(reader, readMetricBatchSize)
    }

    /**
     * query metric dimension rows
     */
    fun queryMeta(
        timeFilter: PruneTimeFilter,
        metrics: List<String>,
        dimension: String,
        dimensionFilter: PrunePushedFilter,
    ): BitlapIterator<MetricDimRowMeta> {
        val reader = this.tableFormatProvider.getMetricDimMetaReader(
            metricDimDataPath,
            timeFilter.mergeCondition(),
            metrics,
            listOf("mk", "dk", "d", "t", "meta"),
            dimension,
            dimensionFilter
        )
        return BitlapReaderIterator(reader, readMetricDimBatchSize)
    }

    fun queryBBM(
        timeFilter: PruneTimeFilter,
        metrics: List<String>,
        dimension: String,
        dimensionFilter: PrunePushedFilter
    ): BitlapIterator<MetricDimRow> {
        val reader = this.tableFormatProvider.getMetricDimReader(
            metricDimDataPath,
            timeFilter.mergeCondition(),
            metrics,
            listOf("mk", "dk", "d", "t", "e"),
            dimension,
            dimensionFilter
        )
        return BitlapReaderIterator(reader, readMetricDimBatchSize)
    }

    fun queryCBM(
        timeFilter: PruneTimeFilter,
        metrics: List<String>,
        dimension: String,
        dimensionFilter: PrunePushedFilter
    ): BitlapIterator<MetricDimRow> {
        val reader = this.tableFormatProvider.getMetricDimReader(
            metricDimDataPath,
            timeFilter.mergeCondition(),
            metrics,
            listOf("mk", "dk", "d", "t", "m"),
            dimension,
            dimensionFilter
        )
        return BitlapReaderIterator(reader, readMetricDimBatchSize)
    }

    override fun close() {
    }
}
