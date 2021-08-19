package org.bitlap.core

import org.bitlap.common.BitlapConf
import org.bitlap.core.meta.DataSourceManager

/**
 * Desc: Context with core components.
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/5/30
 */
object BitlapContext {

    val bitlapConf = BitlapConf()

    val dataSourceManager = DataSourceManager(bitlapConf)
}
