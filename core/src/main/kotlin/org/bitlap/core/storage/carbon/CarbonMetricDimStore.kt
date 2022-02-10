package org.bitlap.core.storage.carbon

import arrow.core.tail
import org.apache.carbondata.core.metadata.datatype.DataTypes
import org.apache.carbondata.core.metadata.datatype.Field
import org.apache.carbondata.core.scan.expression.Expression
import org.apache.carbondata.core.scan.expression.logical.AndExpression
import org.apache.carbondata.core.scan.expression.logical.TrueExpression
import org.apache.carbondata.core.scan.filter.FilterUtil
import org.apache.carbondata.sdk.file.CarbonReader
import org.apache.carbondata.sdk.file.CarbonWriter
import org.apache.carbondata.sdk.file.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.bitlap.common.BitlapIterator
import org.bitlap.common.bitmap.BBM
import org.bitlap.common.bitmap.CBM
import org.bitlap.common.utils.JSONUtils
import org.bitlap.core.data.metadata.Table
import org.bitlap.core.sql.PrunePushedFilter
import org.bitlap.core.sql.PrunePushedFilterExpr
import org.bitlap.core.sql.PruneTimeFilter
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
                    Field("d", DataTypes.STRING),
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
        .sortBy(arrayOf("mk", "dk", "d", "t"))
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
                    it.dimension,
                    it.tm,
                    it.metric.getBytes(),
                    it.entity.getBytes(),
                    JSONUtils.toJson(it.metadata)
                )
            )
        }
        writer.close()
    }

    override fun queryMeta(
        timeFilter: PruneTimeFilter,
        metrics: List<String>,
        dimension: String,
        dimensionFilter: PrunePushedFilter,
    ): BitlapIterator<MetricDimRowMeta> {
        val timeFunc = timeFilter.mergeCondition()
        return this.query(timeFunc, metrics, dimension, dimensionFilter, arrayOf("meta")) { row ->
            val (meta) = row
            val metaObj = JSONUtils.fromJson(meta.toString(), MetricDimRowMeta::class.java)
            if (timeFunc(metaObj.tm)) {
                metaObj
            } else {
                null
            }
        }
    }

    override fun queryBBM(
        timeFilter: PruneTimeFilter,
        metrics: List<String>,
        dimension: String,
        dimensionFilter: PrunePushedFilter
    ): BitlapIterator<MetricDimRow> {
        val timeFunc = timeFilter.mergeCondition()
        return this.query(timeFunc, metrics, dimension, dimensionFilter, arrayOf("mk", "dk", "d", "t", "e")) { row ->
            val (mk, dk, d, t, e) = row
            if (timeFunc(t as Long)) {
                MetricDimRow(t, "$mk", "$dk", "$d", CBM(), BBM(e as? ByteArray))
            } else {
                null
            }
        }
    }

    override fun queryCBM(
        timeFilter: PruneTimeFilter,
        metrics: List<String>,
        dimension: String,
        dimensionFilter: PrunePushedFilter
    ): BitlapIterator<MetricDimRow> {
        val timeFunc = timeFilter.mergeCondition()
        return this.query(timeFunc, metrics, dimension, dimensionFilter, arrayOf("mk", "dk", "d", "t", "m")) { row ->
            val (mk, dk, d, t, m) = row
            if (timeFunc(t as Long)) {
                MetricDimRow(t, "$mk", "$dk", "$d", CBM(m as? ByteArray), BBM())
            } else {
                null
            }
        }
    }

    private fun <R> query(
        timeFunc: TimeFilterFun,
        metrics: List<String>,
        dimension: String,
        dimensionFilter: PrunePushedFilter,
        projections: Array<String>,
        rowHandler: (Array<*>) -> R?,
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
                AndExpression(
                    FilterUtil.prepareEqualToExpressionSet("mk", "string", metrics),
                    AndExpression(
                        FilterUtil.prepareEqualToExpression("dk", "string", dimension),
                        this.mergeToExpression(dimensionFilter)
                    )
                )
            )
            .build<Any>()
        return MetricRowIterator(reader, rowHandler)
    }

    private fun mergeToExpression(filter: PrunePushedFilter): Expression {
        val conditions = filter.getConditions()
        if (conditions.isEmpty()) {
            return TrueExpression(null)
        }
        var expr = this.convertToExpression(conditions.first())
        conditions.tail().forEach {
            expr = AndExpression(
                expr,
                this.convertToExpression(it)
            )
        }
        return expr
    }

    private fun convertToExpression(expr: PrunePushedFilterExpr): Expression {
        return when (expr.op) {
            "=", "!=", "<>", "<", "<=", ">", ">=" ->
                FilterUtil.prepareEqualToExpression("d", "string", expr.values.first())
            "in" ->
                FilterUtil.prepareEqualToExpressionSet("d", "string", expr.values)
            else -> throw IllegalArgumentException("Unable to convert ${expr.op} to carbon expression.")
        }
    }

    override fun close() {
    }
}
