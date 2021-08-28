package org.bitlap.core.sql.parser

import org.bitlap.core.data.BitlapCatalog

/**
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2021/8/28
 */
interface SqlCommand {

    /**
     * run command
     */
    fun run(catalog: BitlapCatalog)

}
