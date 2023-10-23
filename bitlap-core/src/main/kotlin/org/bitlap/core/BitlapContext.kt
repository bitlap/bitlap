/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.core

import org.apache.hadoop.conf.Configuration
import org.bitlap.common.BitlapConf
import org.bitlap.common.EventBus
import org.bitlap.core.catalog.impl.BitlapCatalogImpl
import org.bitlap.core.sql.BitlapSqlPlanner

/**
 * Desc: Context with core components.
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/5/30
 */
object BitlapContext {

    val bitlapConf = BitlapConf()
    val hadoopConf = Configuration() // TODO (merge bitlap.hadoop.xxx)

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
}
