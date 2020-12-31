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
class DataSourceStore : AbsBitlapStore<DataSource> {

    private var dataRootDir: Path

    constructor(conf: Configuration) : super(conf) {
        // init datasource data directory
        dataRootDir = Path(BitlapProperties.getRootDir(), "data")
        if (!fs.exists(dataRootDir)) {
            fs.mkdirs(dataRootDir)
        }
    }

    override fun store(t: DataSource): DataSource {
        val name = PreConditions.checkNotBlank(t.name).trim()
        // TODO: check name valid
        val dsDir = Path(dataRootDir, name)
        if (!fs.exists(dsDir)) {
            fs.mkdirs(dsDir)
            val schema = JSONUtil.toJsonStr(mapOf("name" to name, "createTime" to "${System.currentTimeMillis()}")).toByteArray()
            fs.create(Path(dsDir, ".schema"), true).use {
                it.writeInt(schema.size)
                it.write(schema)
            }
        }
        return t
    }

    override fun exists(t: DataSource): Boolean {
        val name = PreConditions.checkNotBlank(t.name).trim()
        val dsDir = Path(dataRootDir, name)
        return fs.exists(dsDir)
    }

    override fun get(t: DataSource): DataSource {
        val name = PreConditions.checkNotBlank(t.name).trim()
        val dsDir = Path(dataRootDir, name)
        val schema = fs.open(Path(dsDir, ".schema")).use {
            val len = it.readInt()
            val buf = ByteArray(len)
            it.readFully(buf, 0, len)
            String(buf)
        }
        val json = JSONUtil.parse(schema)
        return DataSource(json.getByPath("name", String::class.java)).also {
            it.createTime = json.getByPath("createTime", String::class.java).toLong()
        }
    }
}