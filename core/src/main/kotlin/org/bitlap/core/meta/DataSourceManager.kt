package org.bitlap.core.meta

import org.apache.hadoop.conf.Configuration
import org.bitlap.common.BitlapConf
import org.bitlap.common.exception.BitlapException
import org.bitlap.common.utils.PreConditions
import org.bitlap.core.storage.metadata.DataSource
import org.bitlap.core.storage.store.DataSourceStore

/**
 * Desc: datasource manager
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2020/12/20
 */
open class DataSourceManager(val conf: BitlapConf) {

    private val configuration = Configuration()

    /**
     * create [DataSource] with [name].
     *
     * if [ifNotExists] is false, exception will be thrown when [DataSource] is exists
     * otherwise ignored.
     */
    fun createDataSource(name: String, ifNotExists: Boolean = false) {
        PreConditions.checkNotBlank(name, "DataSource name cannot be null or blank.")
        val store = DataSourceStore(name, configuration, conf)
        val ds = DataSource(name)
        val exists = store.exists()
        if (exists && ifNotExists) {
            return
        } else if (exists) {
            throw BitlapException("DataSource [$name] already exists.")
        }
        store.open()
        store.store(ds)
    }

    /**
     * get [DataSource] with [name]
     */
    fun getDataSource(name: String): DataSource {
        val store = DataSourceStore(name, configuration, conf)
        if (!store.exists()) {
            throw BitlapException("DataSource [$name] is not exists.")
        }
        return store.get()
    }

    fun getDataSourceStore(name: String): DataSourceStore {
        val store = DataSourceStore(name, configuration, conf)
        if (!store.exists()) {
            throw BitlapException("DataSource [$name] is not exists.")
        }
        store.open()
        return store
    }
}
