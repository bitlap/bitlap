package org.bitlap.core

import org.bitlap.common.utils.PreConditions
import org.bitlap.core.metadata.DataSource

/**
 * Desc: datasource manager
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2020/12/20
 */
object DataSourceManager {

    /**
     * create [DataSource] with [name]
     */
    fun createDataSource(name: String) {
        PreConditions.checkNotBlank(name, "DataSource name cannot be null or blank.")
        val ds = DataSource(name)
    }

}