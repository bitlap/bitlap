/* Copyright (c) 2023 bitlap.org */
package org.bitlap.core.storage.carbon

import org.apache.carbondata.core.metadata.datatype.DataTypes
import org.apache.carbondata.core.metadata.datatype.Field
import org.apache.carbondata.core.scan.filter.FilterUtil
import org.apache.carbondata.sdk.file.CarbonReader
import org.apache.carbondata.sdk.file.CarbonWriter
import org.apache.carbondata.sdk.file.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.bitlap.common.BitlapIterator
import org.bitlap.common.TimeRange
import org.bitlap.common.bitmap.BBM
import org.bitlap.common.bitmap.CBM
import org.bitlap.common.utils.DateEx.utc
import org.bitlap.common.utils.JSONUtils
import org.bitlap.core.data.metadata.Table
import org.bitlap.core.sql.Keyword
import org.bitlap.core.sql.PruneTimeFilter
import org.bitlap.core.sql.TimeFilterFun
import org.bitlap.core.storage.MetricStore
import org.bitlap.core.storage.load.MetricRow
import org.bitlap.core.storage.load.MetricRowIterator
import org.bitlap.core.storage.load.MetricRowMeta

/**
 * Metric implemented by apache carbondata
 */
class CarbonMetricStore(val table: Table, val hadoopConf: Configuration) : MetricStore(table, hadoopConf) {

    private val dataPath = Path(super.storePath, "metric")

    private fun writerB() = CarbonWriter.builder()
        .withHadoopConf(hadoopConf)
        .withCsvInput(
            Schema(
                arrayOf(
                    // TODO: add enum & add shard_id if cbm is too big
                    Field("mk", DataTypes.STRING),
                    Field("t", DataTypes.LONG),
                    // Field("ek", DataTypes.STRING),
                    // Field("m", DataTypes.createArrayType(DataTypes.BINARY)),
                    // Field("e", DataTypes.createArrayType(DataTypes.BINARY)),
                    Field("m", DataTypes.BINARY),
                    Field("e", DataTypes.BINARY),
                    Field("meta", DataTypes.STRING),
                )
            )
        )
        // .sortBy(arrayOf("mk", "t"))
        .withBlockletSize(8)
        .withPageSizeInMb(1)
        .writtenBy(this::class.java.simpleName)

    private fun readerB() = CarbonReader.builder()
        .withHadoopConf(hadoopConf)
        .withRowRecordReader() // disable vector read
        .withBatch(100) // default is 100

    override fun open() {
        super.open()
        if (!fs.exists(dataPath)) {
            fs.mkdirs(dataPath)
        }
    }

    /**
     * Store [rows] with time [tm] as carbon data file format.
     */
    override fun store(tm: Long, rows: List<MetricRow>) {
        if (rows.isEmpty()) {
            return
        }
        val date = tm.utc()
        val output = Path(dataPath, "${date.withTimeAtStartOfDay().millis}/${date.millis}")
        if (fs.exists(output)) {
            fs.delete(output, true)
        }
        val writer = writerB().outputPath(output.toString()).build()
        rows.sortedBy { "${it.metricKey}${it.tm}" }
            .forEach {
                writer.write(
                    arrayOf(
                        it.metricKey,
                        it.tm,
                        it.metric.getBytes(),
                        it.entity.getBytes(),
                        JSONUtils.toJson(it.metadata)
                    )
                )
            }
        writer.close()
    }

    override fun query(time: TimeRange, metrics: List<String>): BitlapIterator<MetricRow> {
        val timeFunc = { tm: Long ->
            time.contains(tm.utc())
        }
        val timeFilter = PruneTimeFilter().add(Keyword.TIME, timeFunc, time.toString())
        return this.queryCBM(timeFilter, metrics)
    }

    override fun queryMeta(time: TimeRange, metrics: List<String>): BitlapIterator<MetricRowMeta> {
        val timeFunc = { tm: Long ->
            time.contains(tm.utc())
        }
        val timeFilter = PruneTimeFilter().add(Keyword.TIME, timeFunc, time.toString())
        return this.queryMeta(timeFilter, metrics)
    }

    override fun queryMeta(timeFilter: PruneTimeFilter, metrics: List<String>): BitlapIterator<MetricRowMeta> {
        val timeFunc = timeFilter.mergeCondition()
        return this.query(timeFunc, metrics, arrayOf("meta")) { row ->
            val (meta) = row
            val metaObj = JSONUtils.fromJson(meta.toString(), MetricRowMeta::class.java)
            if (timeFunc(metaObj.tm)) {
                metaObj
            } else {
                null
            }
        }
    }

    override fun queryBBM(timeFilter: PruneTimeFilter, metrics: List<String>): BitlapIterator<MetricRow> {
        val timeFunc = timeFilter.mergeCondition()
        return this.query(timeFunc, metrics, arrayOf("mk", "t", "e")) { row ->
            val (mk, t, e) = row
            if (timeFunc(t as Long)) {
                MetricRow(t, mk.toString(), CBM(), BBM(e as? ByteArray))
            } else {
                null
            }
        }
    }

    override fun queryCBM(timeFilter: PruneTimeFilter, metrics: List<String>): BitlapIterator<MetricRow> {
        val timeFunc = timeFilter.mergeCondition()
        return this.query(timeFunc, metrics, arrayOf("mk", "t", "m")) { row ->
            val (mk, t, m) = row
            if (timeFunc(t as Long)) {
                MetricRow(t, mk.toString(), CBM(m as? ByteArray), BBM())
            } else {
                null
            }
        }
    }

    private fun <R> query(
        timeFunc: TimeFilterFun,
        metrics: List<String>,
        projections: Array<String>,
        rowHandler: (Array<*>) -> R?
    ): BitlapIterator<R> {
        // 1. get files
        val files = fs.listStatus(dataPath)
            .map { it.path }
            .flatMap { subPath -> fs.listStatus(subPath) { timeFunc(it.name.toLong()) }.map { it.path } }
            .flatMap { filePath -> fs.listStatus(filePath).map { it.path } }

        if (files.isEmpty()) {
            return BitlapIterator.empty()
        }
        // 2. build reader
        val reader = readerB().withFileLists(files)
            .projection(projections)
            .filter(
                FilterUtil.prepareEqualToExpressionSet("mk", "string", metrics)
            )
            .build<Any>()
        return MetricRowIterator(reader, rowHandler)
    }

    override fun close() {
    }
}
