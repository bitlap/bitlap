/* Copyright (c) 2022 bitlap.org */
package org.bitlap.common

import mu.KLogger
import mu.KotlinLogging

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

fun logger(clazz: Class<*>): KLogger {
    return logger(clazz.name)
}

fun logger(name: String): KLogger {
    return KotlinLogging.logger(name)
}
