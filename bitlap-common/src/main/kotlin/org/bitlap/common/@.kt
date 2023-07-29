/**
 * Copyright (C) 2023 bitlap.org .
 */
package org.bitlap.common

import mu.KLogger
import mu.KotlinLogging
import java.time.Duration

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
 * see [kotlin.system.measureTimeMillis] and [kotlin.time.measureTimedValue]
 */
fun <T> elapsed(block: () -> T): Pair<T, Duration> {
    val start = System.currentTimeMillis()
    return block() to Duration.ofMillis(System.currentTimeMillis() - start)
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

/**
 * like sql nvl
 */
fun <T> nvl(t: T?, default: T?): T? {
    return t ?: default
}

/**
 * like sql coalesce
 */
fun <T> coalesce(vararg t: T?): T? {
    return t.find { it != null }
}
