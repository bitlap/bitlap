package org.bitlap.core.storage.store

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.bitlap.common.BitlapConf
import org.bitlap.common.utils.PreConditions
import org.bitlap.core.storage.metadata.DataSource
import org.bitlap.network.proto.storage.DataSourcePB

/**
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2020/12/23
 */
class DataSourceStore(val name: String, val schema: String, val hadoopConf: Configuration, val conf: BitlapConf) : AbsBitlapStore<DataSource>(hadoopConf, conf) {

    override val dataDir: Path = Path(rootPath, "data/$schema/$name")
    private lateinit var metricStore: MetricStore

    override fun open() {
        super.open()
        if (!fs.exists(dataDir)) {
            fs.mkdirs(dataDir)
        }
        this.metricStore = MetricStore(this, hadoopConf, conf)
        this.metricStore.open()
    }

    override fun store(t: DataSource): DataSource {
        val name = PreConditions.checkNotBlank(t.name).trim()
        // TODO: check name valid
        val schema = DataSourcePB.newBuilder().setSchema(t.schema).setName(name).setCreateTime(t.createTime).setUpdateTime(t.updateTime).build().toByteArray()
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
            DataSourcePB.parseFrom(buf)
        }
        return DataSource(
            schema.schema,
            schema.name,
            schema.createTime,
            schema.updateTime
        )
    }

    fun getMetricStore(): MetricStore = this.metricStore

    override fun close() {
    }
}
