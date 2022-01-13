package org.bitlap.core.storage.carbon

import org.apache.carbondata.core.metadata.datatype.DataTypes
import org.apache.carbondata.core.metadata.datatype.Field
import org.apache.carbondata.core.scan.expression.logical.AndExpression
import org.apache.carbondata.core.scan.filter.FilterUtil
import org.apache.carbondata.sdk.file.CarbonReader
import org.apache.carbondata.sdk.file.CarbonWriter
import org.apache.carbondata.sdk.file.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.bitlap.common.BitlapIterator
import org.bitlap.common.utils.JSONUtils
import org.bitlap.core.data.metadata.Table
import org.bitlap.core.sql.TimeFilterFun
import org.bitlap.core.storage.MetricDimStore
import org.bitlap.core.storage.load.MetricDimRow
import org.bitlap.core.storage.load.MetricDimRowMeta
import org.bitlap.core.storage.load.MetricRowIterator
import org.joda.time.DateTime

/**
 * Metric one dimension implemented by apache carbondata
 */
class CarbonMetricDimStore(val table: Table, val hadoopConf: Configuration) : MetricDimStore(table, hadoopConf) {

    private val dataPath = Path(super.storePath, "metric_dimension")

    private fun writerB() = CarbonWriter.builder()
        .withHadoopConf(hadoopConf)
        .withCsvInput(
            Schema(
                arrayOf(
                    // TODO: add enum & add shard_id if cbm is too big
                    Field("mk", DataTypes.STRING),
                    Field("dk", DataTypes.STRING),
                    Field("t", DataTypes.LONG),
                    Field("d", DataTypes.STRING),
                    // Field("ek", DataTypes.STRING),
                    // Field("m", DataTypes.createArrayType(DataTypes.BINARY)),
                    // Field("e", DataTypes.createArrayType(DataTypes.BINARY)),
                    Field("m", DataTypes.BINARY),
                    Field("e", DataTypes.BINARY),
                    Field("meta", DataTypes.STRING),
                )
            )
        )
        .sortBy(arrayOf("mk", "dk", "t", "d"))
        .withBlockletSize(16)
        .withPageSizeInMb(2)
        .writtenBy(this::class.java.simpleName)

    private fun readerB() = CarbonReader.builder()
        .withHadoopConf(hadoopConf)
        .withRowRecordReader() // disable vector read
        .withBatch(1000) // default is 100

    /**
     * Store [rows] with time [tm] as carbon data file format.
     */
    override fun store(tm: Long, rows: List<MetricDimRow>) {
        if (rows.isEmpty()) {
            return
        }
        val date = DateTime(tm)
        val output = "${date.withTimeAtStartOfDay().millis}/${date.millis}"
        val writer = writerB().outputPath(Path(dataPath, output).toString()).build()
        rows.forEach {
            writer.write(
                arrayOf(
                    it.metricKey,
                    it.dimensionKey,
                    it.tm,
                    it.dimension,
                    it.metric.getBytes(),
                    it.entity.getBytes(),
                    JSONUtils.toJson(it.metadata)
                )
            )
        }
        writer.close()
    }

    override fun queryMeta(timeFilter: TimeFilterFun, metrics: List<String>, dimension: Pair<String, List<String>>): BitlapIterator<MetricDimRowMeta> {
        // 1. get files
        val files = fs.listStatus(dataPath)
            .map { it.path }
            .flatMap { subPath -> fs.listStatus(subPath) { timeFilter(it.name.toLong()) }.map { it.path } }
            .flatMap { filePath -> fs.listStatus(filePath).map { it.path } }

        if (files.isEmpty()) {
            return BitlapIterator.empty()
        }
        // 2. build reader
        val reader = readerB().withFileLists(files)
            .projection(arrayOf("meta"))
            .filter(
                AndExpression(
                    FilterUtil.prepareEqualToExpressionSet("mk", "string", metrics),
                    AndExpression(
                        FilterUtil.prepareEqualToExpression("dk", "string", dimension.first),
                        FilterUtil.prepareEqualToExpressionSet("d", "string", dimension.second)
                    )
                )
            )
            .build<Any>()
        return MetricRowIterator(reader) { row ->
            val (meta) = row
            val metaObj = JSONUtils.fromJson(meta.toString(), MetricDimRowMeta::class.java)
            if (timeFilter(metaObj.tm)) {
                metaObj
            } else {
                null
            }
        }
    }

    override fun close() {
    }
}
