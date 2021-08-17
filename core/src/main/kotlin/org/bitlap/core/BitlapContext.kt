package org.bitlap.core

import org.apache.hadoop.conf.Configuration
import org.bitlap.common.BitlapConf
import org.bitlap.core.data.BitlapCatalog

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
        BitlapCatalog.apply {
            conf = bitlapConf
            hadoopConf = Configuration()
            start()
        }
    }
}
