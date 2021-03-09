package org.bitlap.storage.store

import cn.hutool.json.JSONUtil
import org.apache.carbondata.sdk.file.CarbonWriter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.bitlap.common.BitlapProperties
import org.bitlap.storage.metadata.metric.MetricRows
import org.joda.time.DateTime

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
        .withCsvInput(
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

    override fun close() {
    }
}
