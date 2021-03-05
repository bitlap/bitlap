package org.bitlap.storage.store

import cn.hutool.json.JSONUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.bitlap.common.BitlapProperties
import org.bitlap.common.utils.PreConditions
import org.bitlap.core.metadata.DataSource

/**
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2020/12/23
 */
class DataSourceStore(val name: String, val conf: Configuration) : AbsBitlapStore<DataSource>(conf) {

    override val dataDir: Path = Path(BitlapProperties.getRootDir(), "data/$name")
    private lateinit var metricStore: MetricStore

    override fun open() {
        super.open()
        if (!fs.exists(dataDir)) {
            fs.mkdirs(dataDir)
        }
        this.metricStore = MetricStore(this, conf)
        this.metricStore.open()
    }

    override fun store(t: DataSource): DataSource {
        val name = PreConditions.checkNotBlank(t.name).trim()
        // TODO: check name valid
        val schema = JSONUtil.toJsonStr(mapOf("name" to name, "createTime" to t.createTime, "updateTime" to t.updateTime)).toByteArray()
        fs.create(Path(dataDir, ".schema"), true).use {
            it.writeInt(schema.size)
            it.write(schema)
        }
        return t
    }

    fun exists(): Boolean {
        return fs.exists(dataDir)
    }

    fun get(): DataSource {
        val schema = fs.open(Path(dataDir, ".schema")).use {
            val len = it.readInt()
            val buf = ByteArray(len)
            it.readFully(buf, 0, len)
            String(buf)
        }
        val json = JSONUtil.parse(schema)
        return DataSource(
                json.getByPath("name", String::class.java),
                json.getByPath("createTime", Long::class.java)
        ).also {
            it.updateTime = json.getByPath("updateTime", Long::class.java)
        }
    }

    fun getMetricStore(): MetricStore = this.metricStore

    override fun close() {

    }
}