package org.bitlap.common.utils

import org.bitlap.common.error.BitlapException

/**
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2020/12/20
 */
object PreConditions {

    /**
     * check [str] cannot be null or blank
     */
    @JvmStatic
    fun checkNotBlank(str: String?, key: String = "string", msg: String = "$key cannot be null or blank."): String {
        if (str.isNullOrBlank()) {
            throw BitlapException(msg)
        }
        return str
    }

    /**
     * check [expr] cannot be false
     */
    @JvmStatic
    fun checkExpression(expr: Boolean, key: String = "expr", msg: String = "$key cannot be false") {
        if (!expr) {
            throw BitlapException(msg)
        }
    }
}
