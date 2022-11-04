/* Copyright (c) 2022 bitlap.org */
package org.bitlap.core

import org.apache.hadoop.conf.Configuration
import org.bitlap.common.BitlapConf
import org.bitlap.common.EventBus
import org.bitlap.core.data.impl.BitlapCatalogImpl
import org.bitlap.core.sql.BitlapSqlPlanner
import kotlin.concurrent.getOrSet

/**
 * Desc: Context with core components.
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/5/30
 */
object BitlapContext {

    val bitlapConf = BitlapConf()
    val hadoopConf = Configuration()

    val catalog by lazy {
        BitlapCatalogImpl(bitlapConf, hadoopConf).apply {
            start()
        }
    }

    val sqlPlanner by lazy {
        BitlapSqlPlanner(catalog)
    }

    val eventBus by lazy {
        EventBus().apply { start() }
    }

    val currentContext = ThreadLocal<String>()

    fun getCurrentDatabase(): String {
        return currentContext.getOrSet { Constants.DEFAULT_DATABASE }
    }
}
