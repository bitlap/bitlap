/* Copyright (c) 2023 bitlap.org */
package org.bitlap.core.storage.io

import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.parquet.filter2.predicate.FilterApi
import org.apache.parquet.filter2.predicate.FilterPredicate
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.util.HiddenFileFilter
import org.bitlap.common.bitmap.BBM
import org.bitlap.common.bitmap.CBM
import org.bitlap.common.utils.JsonEx.json
import org.bitlap.common.utils.JsonEx.jsonAs
import org.bitlap.core.catalog.metadata.Table
import org.bitlap.core.sql.PrunePushedFilter
import org.bitlap.core.sql.TimeFilterFun
import org.bitlap.core.storage.BitlapReader
import org.bitlap.core.storage.BitlapReaders
import org.bitlap.core.storage.BitlapWriter
import org.bitlap.core.storage.TableFormatProvider
import org.bitlap.core.storage.load.MetricDimRow
import org.bitlap.core.storage.load.MetricDimRowMeta
import org.bitlap.core.storage.load.MetricRow
import org.bitlap.core.storage.load.MetricRowMeta

/**
 * Desc: Implementation of parquet table format
 */
class BitlapParquetProvider(val table: Table, private val fs: FileSystem) : TableFormatProvider {

    override fun getMetricWriter(output: Path): BitlapWriter<MetricRow> {
        return BitlapParquetWriter(fs, output, METRIC_SCHEMA, ParquetWriter.DEFAULT_BLOCK_SIZE / 8L) { row ->
            GenericData.Record(METRIC_SCHEMA).apply {
                put(0, row.metricKey)
                put(1, row.tm)
                put(2, row.metric.getBytes())
                put(3, row.entity.getBytes())
                put(4, row.metadata.json())
            }
        }
    }

    override fun getMetricDimWriter(output: Path): BitlapWriter<MetricDimRow> {
        return BitlapParquetWriter(fs, output, METRIC_DIM_SCHEMA, ParquetWriter.DEFAULT_BLOCK_SIZE / 4L) { row ->
            GenericData.Record(METRIC_DIM_SCHEMA).apply {
                put(0, row.metricKey)
                put(1, row.dimensionKey)
                put(2, row.dimension)
                put(3, row.tm)
                put(4, row.metric.getBytes())
                put(5, row.entity.getBytes())
                put(6, row.metadata.json())
            }
        }
    }

    override fun getMetricMetaReader(dataPath: Path, timeFunc: TimeFilterFun, metrics: List<String>, projections: List<String>): BitlapReader<MetricRowMeta> {
        val inputs = this.listFilePath(dataPath, timeFunc)
        val metricFilter = BitlapReaders.makeParquetFilter("mk", metrics)
        val requestedProjection = BitlapReaders.makeAvroSchema(METRIC_SCHEMA, projections)
        return BitlapParquetReader(fs, inputs, METRIC_SCHEMA, requestedProjection, metricFilter.compact()) { row ->
            val metaObj = row.getWithDefault("meta", "{}").jsonAs<MetricRowMeta>()
            if (timeFunc(metaObj.tm)) {
                metaObj
            } else {
                null
            }
        }
    }

    override fun getMetricReader(dataPath: Path, timeFunc: TimeFilterFun, metrics: List<String>, projections: List<String>): BitlapReader<MetricRow> {
        val inputs = this.listFilePath(dataPath, timeFunc)
        val metricFilter = BitlapReaders.makeParquetFilter("mk", metrics)
        val requestedProjection = BitlapReaders.makeAvroSchema(METRIC_SCHEMA, projections)
        return BitlapParquetReader(fs, inputs, METRIC_SCHEMA, requestedProjection, metricFilter.compact()) { row ->
            val tm = row.get("t")
            if (tm != null && timeFunc(tm as Long)) {
                MetricRow(
                    tm,
                    row.getWithDefault("mk", ""),
                    CBM(row.getWithDefault("m", ByteArray(0))),
                    BBM(row.getWithDefault("e", ByteArray(0)))
                )
            } else {
                null
            }
        }
    }

    override fun getMetricDimMetaReader(
        dataPath: Path,
        timeFunc: TimeFilterFun,
        metrics: List<String>,
        projections: List<String>,
        dimension: String,
        dimensionFilter: PrunePushedFilter
    ): BitlapReader<MetricDimRowMeta> {
        val inputs = this.listFilePath(dataPath, timeFunc)
        val filter = this.getMetricDimFilter(metrics, dimension, dimensionFilter)
        val requestedProjection = BitlapReaders.makeAvroSchema(METRIC_DIM_SCHEMA, projections)
        return BitlapParquetReader(fs, inputs, METRIC_DIM_SCHEMA, requestedProjection, filter.compact()) { row ->
            val metaObj = row.getWithDefault("meta", "{}").jsonAs<MetricDimRowMeta>()
            if (timeFunc(metaObj.tm)) {
                metaObj
            } else {
                null
            }
        }
    }

    override fun getMetricDimReader(
        dataPath: Path,
        timeFunc: TimeFilterFun,
        metrics: List<String>,
        projections: List<String>,
        dimension: String,
        dimensionFilter: PrunePushedFilter
    ): BitlapReader<MetricDimRow> {
        val inputs = this.listFilePath(dataPath, timeFunc)
        val filter = this.getMetricDimFilter(metrics, dimension, dimensionFilter)
        val requestedProjection = BitlapReaders.makeAvroSchema(METRIC_DIM_SCHEMA, projections)
        return BitlapParquetReader(fs, inputs, METRIC_DIM_SCHEMA, requestedProjection, filter.compact()) { row ->
            val tm = row.get("t")
            if (tm != null && timeFunc(tm as Long)) {
                MetricDimRow(
                    tm,
                    row.getWithDefault("mk", ""),
                    row.getWithDefault("dk", ""),
                    row.getWithDefault("d", ""),
                    CBM(row.getWithDefault("m", ByteArray(0))),
                    BBM(row.getWithDefault("e", ByteArray(0)))
                )
            } else {
                null
            }
        }
    }

    private fun getMetricDimFilter(metrics: List<String>, dimension: String, dimensionFilter: PrunePushedFilter): FilterPredicate {
        var filter = BitlapReaders.makeParquetFilterAnd(
            listOf(
                "mk" to metrics,
                "dk" to listOf(dimension)
            )
        )
        val dimFilter = BitlapReaders.makeParquetFilterFromPrunePushedFilter(dimensionFilter, "d")
        if (dimFilter != null) {
            filter = FilterApi.and(filter, dimFilter)
        }
        return filter
    }

    // TODO (get partitions)
    private fun listFilePath(dataPath: Path, timeFunc: TimeFilterFun): List<Path> {
        return fs.listStatus(dataPath)
            .filter {
                it.isDirectory && run {
                    val (_, name) = it.path.name.split("=")
                    timeFunc(name.toLong())
                }
            }
            .flatMap { filePath ->
                fs.listStatus(filePath.path, HiddenFileFilter.INSTANCE).map { it.path }
            }
    }

    companion object {

        // TODO (add enum & add shard_id if cbm is too big)
        val METRIC_SCHEMA: Schema = SchemaBuilder.builder()
            .record("metric").namespace(BitlapParquetProvider::class.java.packageName).fields()
            .optionalString("mk")
            .optionalLong("t")
            // .optionalString(ek)
            // .name("m").type().array().items().bytesType().noDefault()
            // .name("e").type().array().items().bytesType().noDefault()
            .optionalBytes("m")
            .optionalBytes("e")
            .optionalString("meta")
            .endRecord()

        val METRIC_DIM_SCHEMA: Schema = SchemaBuilder.builder()
            .record("metric").namespace(BitlapParquetProvider::class.java.packageName).fields()
            .optionalString("mk")
            .optionalString("dk")
            .optionalString("d")
            .optionalLong("t")
            // .optionalString(ek)
            // .name("m").type().array().items().bytesType().noDefault()
            // .name("e").type().array().items().bytesType().noDefault()
            .optionalBytes("m")
            .optionalBytes("e")
            .optionalString("meta")
            .endRecord()
    }
}
