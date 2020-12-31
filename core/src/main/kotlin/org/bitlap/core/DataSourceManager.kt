package org.bitlap.core

import org.apache.hadoop.conf.Configuration
import org.bitlap.common.error.BitlapException
import org.bitlap.common.utils.PreConditions
import org.bitlap.core.metadata.DataSource
import org.bitlap.storage.store.DataSourceStore

/**
 * Desc: datasource manager
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2020/12/20
 */
class DataSourceManager {

    private val store = DataSourceStore(Configuration())

    /**
     * create [DataSource] with [name].
     *
     * if [ifNotExists] is false, exception will be thrown when [DataSource] is exists
     * otherwise ignored.
     */
    fun createDataSource(name: String, ifNotExists: Boolean = false) {
        PreConditions.checkNotBlank(name, "DataSource name cannot be null or blank.")
        val ds = DataSource(name)
        val exists = store.exists(ds)
        if (exists && ifNotExists) {
            return
        } else if (exists) {
            throw BitlapException("DataSource [$name] already exists.")
        }
        store.store(ds)
    }

    /**
     * check [DataSource] with [name] exists.
     */
    fun exists(name: String): Boolean {
        return store.exists(DataSource(name))
    }

    /**
     * get [DataSource] with [name]
     */
    fun getDataSource(name: String): DataSource {
        if (!exists(name)) {
            throw BitlapException("DataSource [$name] is not exists.")
        }
        return store.get(DataSource(name))
    }

}