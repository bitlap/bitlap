package org.bitlap.core

import org.apache.hadoop.conf.Configuration
import org.bitlap.common.BitlapConf
import org.bitlap.common.EventBus
import org.bitlap.core.data.impl.BitlapCatalogImpl
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

    val catalog by lazy {
        BitlapCatalogImpl(bitlapConf, Configuration()).apply {
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
