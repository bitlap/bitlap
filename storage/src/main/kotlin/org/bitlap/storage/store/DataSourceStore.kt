package org.bitlap.storage.store

import org.apache.hadoop.conf.Configuration
import org.bitlap.core.metadata.DataSource

/**
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2020/12/23
 */
class DataSourceStore : AbsBitlapStore<DataSource> {

    constructor(conf: Configuration) : super(conf) {
        // init root directory
        if (!fs.exists(rootPath)) {
            fs.create(rootPath)
        }
    }

    override fun store(t: DataSource): DataSource {
        TODO("Not yet implemented")
    }

    override fun exists(t: DataSource): Boolean {
        TODO("Not yet implemented")
    }
}