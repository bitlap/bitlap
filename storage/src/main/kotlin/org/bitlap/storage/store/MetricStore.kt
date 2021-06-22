package org.bitlap.storage.store

import org.apache.carbondata.core.metadata.datatype.DataTypes
import org.apache.carbondata.core.scan.expression.ColumnExpression
import org.apache.carbondata.core.scan.expression.Expression
import org.apache.carbondata.core.scan.expression.LiteralExpression
import org.apache.carbondata.core.scan.expression.conditional.GreaterThanEqualToExpression
import org.apache.carbondata.core.scan.expression.conditional.GreaterThanExpression
import org.apache.carbondata.core.scan.expression.conditional.LessThanEqualToExpression
import org.apache.carbondata.core.scan.expression.conditional.LessThanExpression
import org.apache.carbondata.core.scan.expression.logical.AndExpression
import org.apache.carbondata.core.scan.filter.FilterUtil
import org.apache.carbondata.sdk.file.CarbonReader
import org.apache.carbondata.sdk.file.CarbonWriter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.bitlap.common.BitlapConf
import org.bitlap.common.TimeRange
import org.bitlap.common.bitmap.BBM
import org.bitlap.common.bitmap.CBM
import org.bitlap.common.utils.JSONUtils
import org.bitlap.common.utils.Range.BoundType
import org.bitlap.storage.metadata.MetricRow
import org.bitlap.storage.metadata.MetricRowMeta
import org.bitlap.storage.metadata.MetricRows
import org.joda.time.DateTime

/**
 * Desc:
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/1/26
 */
class MetricStore(dsStore: DataSourceStore, hadoopConf: Configuration, conf: BitlapConf) : AbsBitlapStore<MetricRows>(hadoopConf, conf) {

    override val dataDir: Path = Path(rootPath, "data/${dsStore.name}/metric")
    private val writerB = CarbonWriter.builder()
        .withCsvInput( // TODO: add enum
            """[
                {mk: string}, 
                {t: long},
                {ek: string}, 
                {m: binary},
                {e: binary},
                {meta: string}
            ]
            """.trimIndent()
        )
        .sortBy(arrayOf("mk", "t", "ek"))
        .withBlockletSize(8)
        .withPageSizeInMb(1)
        .writtenBy(projectName)

    private val readerB = CarbonReader.builder()
        .withRowRecordReader() // disable vector read
        .withBatch(1000) // default is 100

    override fun open() {
        super.open()
    }

    /**
     * Store [t] to persistent filesystem or other stores
     */
    override fun store(t: MetricRows): MetricRows {
        if (t.metrics.isEmpty()) {
            return t
        }
        val date = DateTime(t.tm)
        val output = "${date.year}/${date.monthOfYear}/${date.dayOfMonth}/${date.millis}"
        val writer = writerB.outputPath(Path(dataDir, output).toString()).build()
        t.metrics.forEach {
            writer.write(arrayOf(it.metricKey, it.tm, it.entityKey, it.metric.getBytes(), it.entity.getBytes(), JSONUtils.toJson(it.metadata)))
        }
        writer.close()
        return t
    }

    fun query(
        time: TimeRange,
        metrics: List<String>,
        entity: String,
    ): List<MetricRow> {
        return this.query(time, metrics, entity, arrayOf("mk", "t", "ek", "m", "meta")) { row ->
            val (mk, t, ek, m, meta) = row
            val metaObj = JSONUtils.fromJson(meta.toString(), MetricRowMeta::class.java)
            // TODO: with BBM
            MetricRow(t as Long, mk.toString(), ek.toString(), CBM(m as? ByteArray), BBM(), metaObj)
        }
    }

    fun queryMeta(
        time: TimeRange,
        metrics: List<String>,
        entity: String,
    ): List<MetricRowMeta> {
        return this.query(time, metrics, entity, arrayOf("meta")) { row ->
            val (meta) = row
            JSONUtils.fromJson(meta.toString(), MetricRowMeta::class.java)
        }
    }

    private fun <R> query(
        time: TimeRange,
        metrics: List<String>,
        entity: String,
        projections: Array<String>,
        rowHandler: (Array<*>) -> R
    ): List<R> {
        // 1. get files
        val files = time.walkByDayStep { date ->
            val monthDir = "${date.year}/${date.monthOfYear}/${date.dayOfMonth}"
            val path = Path(dataDir, monthDir)
            if (fs.exists(path)) {
                fs.listStatus(path) { p ->
                    val millis = p.name.toLong()
                    time.contains(DateTime(millis))
                }.map { it.path }
            } else {
                emptyList()
            }
        }.flatten().flatMap { p -> fs.listStatus(p).map { it.path } }

        if (files.isEmpty()) {
            return emptyList()
        }
        // 2. build reader
        val timeExpr = buildTimeExpression(time)
        val reader = readerB.withFileLists(files)
            .projection(projections)
            .filter(
                AndExpression(
                    AndExpression(timeExpr.first, timeExpr.second),
                    AndExpression(
                        FilterUtil.prepareEqualToExpressionSet("mk", "string", metrics),
                        FilterUtil.prepareEqualToExpression("ek", "string", entity)
                    )
                ),
            )
            .build<Any>()
        val result = mutableListOf<R>()
        while (reader.hasNext()) {
            val rows = reader.readNextBatchRow()
            rows.forEach { row ->
                row as Array<*>
                result.add(rowHandler.invoke(row))
            }
        }
        reader.close()
        return result
    }

    private fun buildTimeExpression(time: TimeRange): Pair<Expression, Expression> {
        val (startTime, endTime) = time
        val columnExpr = ColumnExpression("t", DataTypes.LONG)
        val startExpr = LiteralExpression(startTime.millis, DataTypes.LONG)
        val endExpr = LiteralExpression(endTime.millis, DataTypes.LONG)
        return when (time.lower.boundType to time.upper.boundType) {
            BoundType.OPEN to BoundType.OPEN -> GreaterThanExpression(columnExpr, startExpr) to LessThanExpression(columnExpr, endExpr)
            BoundType.OPEN to BoundType.CLOSE -> GreaterThanExpression(columnExpr, startExpr) to LessThanEqualToExpression(columnExpr, endExpr)
            BoundType.CLOSE to BoundType.OPEN -> GreaterThanEqualToExpression(columnExpr, startExpr) to LessThanExpression(columnExpr, endExpr)
            BoundType.CLOSE to BoundType.CLOSE -> GreaterThanEqualToExpression(columnExpr, startExpr) to LessThanEqualToExpression(columnExpr, endExpr)
            else -> throw IllegalArgumentException("Illegal arguments time: $time")
        }
    }

    override fun close() {
    }
}
