package org.bitlap.core.storage.store

import org.apache.carbondata.core.metadata.datatype.DataTypes
import org.apache.carbondata.core.metadata.datatype.Field
import org.apache.carbondata.core.scan.expression.ColumnExpression
import org.apache.carbondata.core.scan.expression.Expression
import org.apache.carbondata.core.scan.expression.LiteralExpression
import org.apache.carbondata.core.scan.expression.conditional.GreaterThanEqualToExpression
import org.apache.carbondata.core.scan.expression.conditional.GreaterThanExpression
import org.apache.carbondata.core.scan.expression.conditional.LessThanEqualToExpression
import org.apache.carbondata.core.scan.expression.conditional.LessThanExpression
import org.apache.carbondata.core.scan.filter.FilterUtil
import org.apache.carbondata.sdk.file.CarbonReader
import org.apache.carbondata.sdk.file.CarbonWriter
import org.apache.carbondata.sdk.file.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.bitlap.common.BitlapConf
import org.bitlap.common.BitlapIterator
import org.bitlap.common.TimeRange
import org.bitlap.common.bitmap.BBM
import org.bitlap.common.bitmap.CBM
import org.bitlap.common.utils.JSONUtils
import org.bitlap.common.utils.Range.BoundType
import org.bitlap.core.data.metadata.Table
import org.bitlap.core.sql.TimeFilterFun
import org.bitlap.core.storage.load.MetricRow
import org.bitlap.core.storage.load.MetricRowIterator
import org.bitlap.core.storage.load.MetricRowMeta
import org.joda.time.DateTime

/**
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/1/26
 */
class MetricStore(
    table: Table,
    hadoopConf: Configuration,
    conf: BitlapConf,
) : AbsBitlapStore<Pair<Long, List<MetricRow>>>(hadoopConf, conf) {

    override val dataDir: Path = Path(rootPath, "data/${table.database}/${table.name}/metric")
    private fun writerB() = CarbonWriter.builder()
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
        .sortBy(arrayOf("mk", "t"))
        .withBlockletSize(8)
        .withPageSizeInMb(1)
        .writtenBy(projectName)

    private fun readerB() = CarbonReader.builder()
        .withRowRecordReader() // disable vector read
        .withBatch(1000) // default is 100

    override fun open() {
        super.open()
    }

    /**
     * Store [t] to persistent filesystem or other stores
     */
    override fun store(t: Pair<Long, List<MetricRow>>): Pair<Long, List<MetricRow>> {
        val (tm, metrics) = t
        if (metrics.isEmpty()) {
            return t
        }
        val date = DateTime(tm)
        val output = "${date.withTimeAtStartOfDay().millis}/${date.millis}"
        val writer = writerB().outputPath(Path(dataDir, output).toString()).build()
        metrics.forEach {
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
        return t
    }

    fun query(time: TimeRange, metrics: List<String>): BitlapIterator<MetricRow> {
        val timeFilter = { tm: Long ->
            time.contains(DateTime(tm))
        }
        return this.queryCBM(timeFilter, metrics)
    }

    fun queryMeta(time: TimeRange, metrics: List<String>): BitlapIterator<MetricRowMeta> {
        val timeFilter = { tm: Long ->
            time.contains(DateTime(tm))
        }
        return this.queryMeta(timeFilter, metrics)
    }

    fun queryMeta(timeFilter: TimeFilterFun, metrics: List<String>): BitlapIterator<MetricRowMeta> {
        return this.query(timeFilter, metrics, arrayOf("meta")) { row ->
            val (meta) = row
            val metaObj = JSONUtils.fromJson(meta.toString(), MetricRowMeta::class.java)
            if (timeFilter(metaObj.tm)) {
                metaObj
            } else {
                null
            }
        }
    }

    fun queryBBM(timeFilter: TimeFilterFun, metrics: List<String>): BitlapIterator<MetricRow> {
        return this.query(timeFilter, metrics, arrayOf("mk", "t", "e")) { row ->
            val (mk, t, e) = row
            if (timeFilter(t as Long)) {
                MetricRow(t, mk.toString(), CBM(), BBM(e as? ByteArray))
            } else {
                null
            }
        }
    }

    fun queryCBM(timeFilter: TimeFilterFun, metrics: List<String>): BitlapIterator<MetricRow> {
        return this.query(timeFilter, metrics, arrayOf("mk", "t", "m")) { row ->
            val (mk, t, m) = row
            if (timeFilter(t as Long)) {
                MetricRow(t, mk.toString(), CBM(m as? ByteArray), BBM())
            } else {
                null
            }
        }
    }

    private fun <R> query(
        timeFilter: TimeFilterFun,
        metrics: List<String>,
        projections: Array<String>,
        rowHandler: (Array<*>) -> R?
    ): BitlapIterator<R> {
        // 1. get files
        val files = fs.listStatus(dataDir)
            .map { it.path }
            .flatMap { subPath -> fs.listStatus(subPath) { timeFilter(it.name.toLong()) }.map { it.path } }
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

    // for pushed down timeFilter
    private fun buildTimeExpression(time: TimeRange): Pair<Expression, Expression> {
        val (startTime, endTime) = time
        val columnExpr = ColumnExpression("t", DataTypes.LONG)
        val startExpr = LiteralExpression(startTime.millis, DataTypes.LONG)
        val endExpr = LiteralExpression(endTime.millis, DataTypes.LONG)
        return when (time.lower.boundType to time.upper.boundType) {
            BoundType.OPEN to BoundType.OPEN -> GreaterThanExpression(columnExpr, startExpr) to LessThanExpression(
                columnExpr,
                endExpr
            )
            BoundType.OPEN to BoundType.CLOSE -> GreaterThanExpression(
                columnExpr,
                startExpr
            ) to LessThanEqualToExpression(columnExpr, endExpr)
            BoundType.CLOSE to BoundType.OPEN -> GreaterThanEqualToExpression(
                columnExpr,
                startExpr
            ) to LessThanExpression(columnExpr, endExpr)
            BoundType.CLOSE to BoundType.CLOSE -> GreaterThanEqualToExpression(
                columnExpr,
                startExpr
            ) to LessThanEqualToExpression(columnExpr, endExpr)
            else -> throw IllegalArgumentException("Illegal arguments time: $time")
        }
    }

    override fun close() {
    }
}
