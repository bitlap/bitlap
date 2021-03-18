package org.bitlap.storage.store

import cn.hutool.json.JSONUtil
import org.apache.carbondata.core.scan.expression.logical.AndExpression
import org.apache.carbondata.core.scan.filter.FilterUtil
import org.apache.carbondata.sdk.file.CarbonReader
import org.apache.carbondata.sdk.file.CarbonWriter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.bitlap.common.BitlapProperties
import org.bitlap.storage.metadata.metric.MetricRowMeta
import org.bitlap.storage.metadata.metric.MetricRows
import org.joda.time.DateTime
import java.util.*

/**
 * Desc:
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/1/26
 */
class MetricStore(dsStore: DataSourceStore, conf: Configuration) : AbsBitlapStore<MetricRows>(conf) {

    override val dataDir: Path = Path(BitlapProperties.getRootDir(), "data/${dsStore.name}/metric")
    private val writerB = CarbonWriter.builder()
        .withCsvInput( // TODO: add enum
            """[
                {mk: string}, 
                {ek: string}, 
                {t: long},
                {m: binary},
                {e: binary},
                {meta: string}
            ]
            """.trimIndent()
        )
        .sortBy(arrayOf("mk", "ek", "t"))
        .withBlockletSize(8)
        .withPageSizeInMb(1)
        .writtenBy("bitlap")

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
            writer.write(arrayOf(it.metricKey, it.entityKey, it.tm, ByteArray(0), it.entity.getBytes(), JSONUtil.toJsonStr(it.metadata)))
        }
        writer.close()
        return t
    }

    fun queryMeta(time: Long, metric: String, entity: String): List<MetricRowMeta> {
        val date = DateTime(time)
        val dir = "${date.year}/${date.monthOfYear}/${date.dayOfMonth}/${date.millis}"
        val reader = readerB.withFolder(Path(dataDir, dir).toString())
            .projection(arrayOf("meta"))
            .filter(
                AndExpression(
                    FilterUtil.prepareEqualToExpression("t", "long", time),
                    AndExpression(
                        FilterUtil.prepareEqualToExpression("mk", "string", metric),
                        FilterUtil.prepareEqualToExpression("ek", "string", entity)
                    )
                ),
            )
            .build<Any>()
        val metas = mutableListOf<MetricRowMeta>()
        while (reader.hasNext()) {
            val rows = reader.readNextBatchRow()
            rows.forEach { row ->
                row as Array<*>
                val jsonObj = JSONUtil.parseObj(row.first().toString())
                metas.add(
                    MetricRowMeta(
                        jsonObj.getLong("entityUniqueCount"),
                        jsonObj.getLong("entityCount"),
                        jsonObj.getDouble("metricCount"),
                    )
                )
            }
        }
        reader.close()
        return metas
    }

    override fun close() {
    }
}
