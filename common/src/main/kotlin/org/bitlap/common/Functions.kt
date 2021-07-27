package org.bitlap.common

import mu.KLogger
import mu.KotlinLogging

/**
 * Desc: Enhance functions
 *
 * Mail: chk19940609@gmail.com
 * Created by IceMimosa
 * Date: 2020/11/21
 */

/**
 * Do [func] when [flag] is true,
 * if flag is false, return [t] only
 */
fun <T> doIf(flag: Boolean, t: T, func: (T) -> T): T {
    if (flag) {
        return func.invoke(t)
    }
    return t
}

/**
 * Create kotlin logger wrapper
 */
fun logger(func: () -> Unit): KLogger {
    return KotlinLogging.logger(func)
}
